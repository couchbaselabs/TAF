"""
Created on October 26, 2026

@author: Nimiya Joseph

AWS availability-zone selection on cluster create

Covers the v4 create/GET/list "zones" field, the deployment-options zone
catalog and the /internal/support deploy endpoints.
"""

import json

from couchbase_utils.capella_utils.dedicated import CapellaUtils
from pytests.Capella.RestAPIv4.Projects.get_projects import GetProject


class ClusterZoneSelection(GetProject):

    def setUp(self, nomenclature="Clusters_Zones"):
        GetProject.setUp(self, nomenclature)
        self.server_version = self.normalize_server_version(
            self._get_server_version())
        cases = self.input.param("cases", None)
        self.selected_cases = set(str(cases).split(";")) if cases else None
        # Clusters deployed by these tests (or accepted by mistake), deleted
        # in tearDown before the wrapper project can be removed.
        self.zone_clusters = list()

        # Zone catalog per region, as returned by deployment-options.
        self.discovery_cases = [
            {"id": "Z1", "region": "us-east-1",
             "expected_zones": ["use1-az5", "use1-az6"]},
            {"id": "Z2", "region": "eu-west-2", "expected_zone": "euw2-az4"},
            {"id": "Z3", "region": "ap-northeast-3",
             "expected_zone": "apne3-az1"},
            {"id": "Z4", "region": "ap-southeast-7",
             "expected_zone": "apse7-az1"},
            {"id": "Z5", "region": "mx-central-1",
             "expected_zone": "mxc1-az1"},
            # us-west-1 has only 2 zones, so it can't do multi-AZ; some pods
            # don't offer it at all.
            {"id": "Z6", "region": "us-west-1", "max_zones": 2,
             "may_be_unsupported": True}
        ]

        # v4 creates that deploy a cluster. A case without a "zones" key
        # omits the field from the request.
        self.v4_valid_cases = [
            {"id": "H1", "description": "multi-AZ, 3 valid unique zones",
             "region": "us-east-1", "availability": "multi",
             "zones": ["use1-az1", "use1-az2", "use1-az4"]},
            {"id": "H2a", "description": "multi-AZ, zones omitted",
             "region": "us-east-1", "availability": "multi"},
            {"id": "H2b", "description": "multi-AZ, zones empty array",
             "region": "us-east-1", "availability": "multi", "zones": []},
            {"id": "H2c", "description": "multi-AZ, zones null",
             "region": "us-east-1", "availability": "multi", "zones": None},
            {"id": "H3", "description": "single-AZ, 1 zone",
             "region": "us-east-1", "availability": "single",
             "zones": ["use1-az1"]},
            {"id": "H4", "description": "single-AZ, zones omitted",
             "region": "us-east-1", "availability": "single"},
            {"id": "H5a", "description": "eu-west-2 multi-AZ incl. euw2-az4",
             "region": "eu-west-2", "availability": "multi",
             "zones": ["euw2-az1", "euw2-az2", "euw2-az4"]},
            {"id": "H5b", "description": "ap-northeast-3 multi-AZ",
             "region": "ap-northeast-3", "availability": "multi",
             "zones": ["apne3-az1", "apne3-az2", "apne3-az3"]},
            {"id": "H5c", "description": "ap-southeast-7 multi-AZ",
             "region": "ap-southeast-7", "availability": "multi",
             "zones": ["apse7-az1", "apse7-az2", "apse7-az3"]},
            {"id": "H5d", "description": "mx-central-1 multi-AZ",
             "region": "mx-central-1", "availability": "multi",
             "zones": ["mxc1-az1", "mxc1-az2", "mxc1-az3"]},
            {"id": "H6", "description": "us-east-1 multi-AZ using az5/az6",
             "region": "us-east-1", "availability": "multi",
             "zones": ["use1-az2", "use1-az5", "use1-az6"]},
            {"id": "N11", "description": "GCP + zones, zones ignored",
             "provider": "gcp", "region": "us-east1",
             "availability": "multi",
             "zones": ["use1-az1", "use1-az2", "use1-az4"]}
        ]

        # v4 creates that must be rejected. expected_message is a substring
        # of the error body.
        self.v4_invalid_cases = [
            {"id": "N1", "description": "multi-AZ, 1 zone",
             "region": "us-east-1", "availability": "multi",
             "zones": ["use1-az1"],
             "expected_message": "number of availability zones is invalid"},
            {"id": "N2", "description": "multi-AZ, 2 zones",
             "region": "us-east-1", "availability": "multi",
             "zones": ["use1-az1", "use1-az2"],
             "expected_message": "number of availability zones is invalid"},
            {"id": "N3", "description": "multi-AZ, 4 zones",
             "region": "us-east-1", "availability": "multi",
             "zones": ["use1-az1", "use1-az2", "use1-az4", "use1-az5"],
             "expected_message": "number of availability zones is invalid"},
            {"id": "N4", "description": "multi-AZ, duplicate zone",
             "region": "us-east-1", "availability": "multi",
             "zones": ["use1-az1", "use1-az1", "use1-az2"],
             "expected_message": "more than once"},
            {"id": "N5", "description": "multi-AZ, same zone x3",
             "region": "us-east-1", "availability": "multi",
             "zones": ["use1-az1", "use1-az1", "use1-az1"],
             "expected_message": "more than once"},
            {"id": "N6", "description": "multi-AZ, zone from another region",
             "region": "us-east-1", "availability": "multi",
             "zones": ["use1-az1", "use1-az2", "use2-az1"],
             "expected_message": "use2-az1"},
            {"id": "N7a", "description": "multi-AZ, non-existent zone ID",
             "region": "us-east-1", "availability": "multi",
             "zones": ["use1-az1", "use1-az2", "use1-az9"],
             "expected_message": "use1-az9"},
            {"id": "N7b", "description": "multi-AZ, uppercase zone ID",
             "region": "us-east-1", "availability": "multi",
             "zones": ["USE1-AZ1", "use1-az2", "use1-az4"],
             "expected_message": "USE1-AZ1"},
            {"id": "N8", "description": "single-AZ, 2 zones",
             "region": "us-east-1", "availability": "single",
             "zones": ["use1-az1", "use1-az2"],
             "expected_message": "number of availability zones is invalid"},
            # Disabled until AV-149325 is fixed: returns 500 instead of 422.
            # {"id": "N9", "description": "us-west-1 multi-AZ, 3 zones",
            #  "region": "us-west-1", "availability": "multi",
            #  "zones": ["usw1-az1", "usw1-az3", "usw1-az2"],
            #  "bug": "https://couchbasecloud.atlassian.net/browse/AV-149325"},
            {"id": "N10", "description": "Basic plan, multi-AZ + zones",
             "region": "us-east-1", "availability": "multi",
             "plan": "basic",
             "zones": ["use1-az1", "use1-az2", "use1-az4"]},
            # Disabled until AV-149327 is fixed: accepted with 202 and the
            # cluster then fails to deploy.
            # {"id": "N12", "description": "use1-az3 with c5.2xlarge-class "
            #                              "compute (8 vCPU/16 GB)",
            #  "region": "us-east-1", "availability": "multi", "cpu": 8,
            #  "zones": ["use1-az1", "use1-az2", "use1-az3"],
            #  # The status depends on which compute v4 maps 8 vCPU/16 GB to.
            #  "expected_status_codes": [400, 422],
            #  "bug": "https://couchbasecloud.atlassian.net/browse/AV-149327",
            #  "expected_message": "use1-az3"},
            {"id": "N13", "description": "single node + multi-AZ zones",
             "region": "us-east-1", "availability": "multi", "nodes": 1,
             "zones": ["use1-az1", "use1-az2", "use1-az4"]},
            {"id": "N14", "description": "single-AZ, zone from another "
                                         "region",
             "region": "us-east-1", "availability": "single",
             "zones": ["euw1-az1"], "expected_message": "euw1-az1"},
            {"id": "N15", "description": "single-AZ, new eu-west-2 zone in "
                                         "us-east-1",
             "region": "us-east-1", "availability": "single",
             "zones": ["euw2-az4"], "expected_message": "euw2-az4"}
        ]

        # /internal/support/.../clusters/deploy/v2; the v1 /deploy is
        # deprecated.
        self.support_valid_cases = [
            {"id": "S1", "availability": "multi",
             "zones": ["use1-az1", "use1-az2", "use1-az4"]}
        ]
        self.support_invalid_cases = [
            {"id": "S2", "description": "zone not in catalog",
             "availability": "multi",
             "zones": ["use1-az1", "use1-az2", "use1-az9"],
             "expected_status_code": 422, "expected_message": "use1-az9"},
            {"id": "S2b", "description": "duplicate zones",
             "availability": "multi",
             "zones": ["use1-az1", "use1-az1", "use1-az2"],
             "expected_status_code": 422,
             "expected_message": "more than once"},
            {"id": "S2c", "description": "multi-AZ, 1 zone",
             "availability": "multi", "zones": ["use1-az1"],
             "expected_status_code": 422,
             "expected_message": "number of availability zones is invalid"}
        ]

    def tearDown(self):
        self.update_auth_with_api_token(self.curr_owner_key)
        self.delete_zone_clusters()
        super(ClusterZoneSelection, self).tearDown()

    def delete_zone_clusters(self):
        # A cluster still deploying can't be deleted, so let each settle.
        deleting = list()
        for cluster_id in self.zone_clusters:
            self.wait_for_deployment(cluster_id)
            self.log.info("Destroying Cluster: {}".format(cluster_id))
            res = self.capellaAPI.cluster_ops_apis.delete_cluster(
                self.organisation_id, self.project_id, cluster_id)
            if res.status_code == 429:
                self.handle_rate_limit(int(res.headers["Retry-After"]))
                res = self.capellaAPI.cluster_ops_apis.delete_cluster(
                    self.organisation_id, self.project_id, cluster_id)
            if res.status_code in [202, 404]:
                deleting.append(cluster_id)
            else:
                self.log.error("Error while deleting cluster {}: {}"
                               .format(cluster_id, res.content))
        for cluster_id in deleting:
            if not self.wait_for_deletion(cluster_id):
                self.log.error("!!!...Cluster {} could not be destroyed...!!!"
                               .format(cluster_id))
        self.zone_clusters = list()

    def selected(self, cases):
        if self.selected_cases is None:
            return cases
        return [case for case in cases if case["id"] in self.selected_cases]

    def create_v4_cluster(self, case):
        provider = case.get("provider", "aws")
        if provider == "gcp":
            disk = {"type": "pd-ssd", "storage": 64}
        else:
            disk = {"type": "gp3", "storage": 50, "iops": 3000}
        kwargs = dict()
        if "zones" in case:
            kwargs["zones"] = case["zones"]
        return self.select_CIDR(
            self.organisation_id, self.project_id,
            self.prefix + "zones_" + case["id"],
            {
                "type": provider,
                "region": case["region"],
                "cidr": CapellaUtils.get_next_cidr() + "/20"
            },
            [
                {
                    "node": {
                        "compute": {
                            "cpu": case.get("cpu", 4),
                            "ram": 16
                        },
                        "disk": disk
                    },
                    "numOfNodes": case.get("nodes", 3),
                    "services": ["data", "index", "query"]
                }
            ],
            {"type": case["availability"]},
            {"plan": case.get("plan", "developer pro"), "timezone": "PT"},
            {"version": self.server_version}, **kwargs)

    def support_deploy(self, case):
        body = {
            "name": self.prefix + "zones_" + case["id"],
            "description": "PR 58599 support API test",
            "projectId": self.project_id,
            "deploymentType": "custom",
            "serviceGroupsTemplate": "custom",
            "provider": "aws",
            "region": "us-east-1",
            "cidr": CapellaUtils.get_next_cidr() + "/20",
            "plan": "Developer Pro",
            "serverVersion": self.server_version,
            "availabilityZones": {
                "key": case["availability"],
                "zones": case["zones"]
            },
            "serviceGroups": [
                {
                    "key": "dataWithAny1",
                    "nodes": 3,
                    "compute": "m7g.xlarge",
                    "storage": {
                        "key": "gp3",
                        "sizeInGb": 50,
                        "iops": 3000,
                        "autoScalingEnabled": True
                    },
                    "services": ["kv", "index", "n1ql"]
                }
            ]
        }
        headers = {
            "Authorization": "Bearer {}".format(self.internal_support_token()),
            "Content-Type": "application/json"
        }
        url = self.internal_support_url(
            "/organizations/{}/clusters/deploy/v2".format(
                self.organisation_id))
        # Retry on a CIDR clash only; any other answer is the result.
        for _ in range(5):
            res = self.capellaAPI._urllib_request(
                url, "POST", params=json.dumps(body), headers=headers)
            if res is None or res.status_code == 202 or not any(
                    msg in res.text for msg in self.DUPLICATE_CIDR_MESSAGES):
                return res
            body["cidr"] = CapellaUtils.get_next_cidr() + "/20"
            self.log.info("Trying CIDR: {}".format(body["cidr"]))
        return res

    def track_created_cluster(self, res):
        try:
            cluster_id = res.json()["id"]
        except (Exception,):
            self.log.error("No cluster ID in 202 response: {}"
                           .format(res.content))
            return None
        self.zone_clusters.append(cluster_id)
        return cluster_id

    def validate_rejection(self, case, res, expected_codes, failures):
        # A case with an open bug names it, so a failure points at the ticket.
        known_bug = " (known bug {})".format(case["bug"]) \
            if "bug" in case else ""
        if res is None:
            failures.append("{}: no response{}".format(case["id"], known_bug))
            return
        if res.status_code == 202:
            self.track_created_cluster(res)
        if res.status_code not in expected_codes:
            self.log.warning("Result : {}".format(res.content))
            failures.append("{} {}: expected status {}, got {}{}".format(
                case["id"], case["description"], expected_codes,
                res.status_code, known_bug))
        elif "expected_message" in case and \
                case["expected_message"] not in res.text:
            self.log.warning("Result : {}".format(res.content))
            failures.append("{} {}: error does not mention '{}'{}".format(
                case["id"], case["description"], case["expected_message"],
                known_bug))

    def zones_mismatch(self, provider, availability, zones,
                       requested=None):
        """Why the zones a deployed cluster reports are wrong, None if right."""
        if provider != "aws":
            if zones is not None:
                return "{} cluster reports zones {}".format(provider, zones)
            return None
        expected_count = 3 if availability == "multi" else 1
        if not isinstance(zones, list) or len(zones) != expected_count:
            return "expected {} zones for {}-AZ, got {}".format(
                expected_count, availability, zones)
        if len(set(zones)) != len(zones):
            return "zones are not unique: {}".format(zones)
        if requested and set(zones) != set(requested):
            return "requested zones {}, cluster reports {}".format(
                requested, zones)
        return None

    def validate_deployed_zones(self, cluster_id, provider, availability,
                                requested, label, failures):
        """Returns the zones GET reported, None if any check failed."""
        if not self.wait_for_deployment(cluster_id):
            failures.append("{}: cluster {} did not deploy".format(
                label, cluster_id))
            return None
        res = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
            self.organisation_id, self.project_id, cluster_id)
        if res.status_code == 429:
            self.handle_rate_limit(int(res.headers["Retry-After"]))
            res = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
                self.organisation_id, self.project_id, cluster_id)
        if res.status_code != 200:
            failures.append("{}: GET cluster returned {}".format(
                label, res.status_code))
            return None
        zones = res.json().get("zones")
        self.log.info("{}: cluster {} zones {}".format(
            label, cluster_id, zones))
        mismatch = self.zones_mismatch(provider, availability, zones,
                                       requested)
        if mismatch:
            failures.append("{}: {}".format(label, mismatch))
            return None
        return zones

    def list_zones_failures(self, expected_zones=None):
        """Checks the zones field across the project's cluster list.
        expected_zones maps cluster ID to the zones its GET returned."""
        res = self.capellaAPI.cluster_ops_apis.list_clusters(
            self.organisation_id, self.project_id, 1, 100)
        if res.status_code == 429:
            self.handle_rate_limit(int(res.headers["Retry-After"]))
            res = self.capellaAPI.cluster_ops_apis.list_clusters(
                self.organisation_id, self.project_id, 1, 100)
        if res.status_code != 200:
            return ["List clusters returned {}".format(res.status_code)]
        failures = list()
        for cluster in res.json().get("data", []):
            zones = cluster.get("zones")
            provider = cluster["cloudProvider"]["type"]
            self.log.info("{} {} {} {}".format(
                cluster["id"], provider, cluster["availability"]["type"],
                zones))
            if zones is not None and (not isinstance(zones, list) or
                                      not zones):
                failures.append("List: cluster {} has empty zones {}".format(
                    cluster["id"], zones))
            if provider != "aws" and zones is not None:
                failures.append("List: {} cluster {} has zones {}".format(
                    provider, cluster["id"], zones))
            if expected_zones and cluster["id"] in expected_zones and \
                    set(zones or []) != \
                    set(expected_zones[cluster["id"]] or []):
                failures.append("List: cluster {} zones {} differ from GET "
                                "{}".format(cluster["id"], zones,
                                            expected_zones[cluster["id"]]))
        return failures

    def report(self, failures, total):
        # A case can fail more than one check, so count cases, not failures.
        if failures:
            for fail in failures:
                self.log.warning(fail)
            failed = len(set(fail.split(" ")[0].rstrip(":")
                             for fail in failures))
            self.fail("{} tests FAILED out of {} TOTAL tests ({} failures)"
                      .format(failed, total, len(failures)))

    def test_zone_discovery(self):
        cases = self.selected(self.discovery_cases)
        failures = list()
        for case in cases:
            self.log.info("Executing test: {} {} zones".format(
                case["id"], case["region"]))
            res = self.capellaAPI.get_deployment_options(
                self.organisation_id, "aws", "custom", case["region"])
            if case.get("may_be_unsupported") and res.status_code == 422 \
                    and "ErrRegionNotSupported" in res.text:
                self.log.info("{} is not offered on this pod".format(
                    case["region"]))
                continue
            if res.status_code != 200:
                self.log.warning("Result : {}".format(res.content))
                failures.append("{}: status {}".format(
                    case["id"], res.status_code))
                continue
            az = res.json().get("availabilityZones", {})
            zones = az.get("zones")
            self.log.info("{} zones: {}".format(case["region"], zones))
            if not isinstance(zones, list) or not zones:
                failures.append("{}: availabilityZones.zones missing or "
                                "empty".format(case["id"]))
                continue
            options = dict((o.get("key"), o) for o in az.get("options", []))
            # Zone selection stays API-only, the UI must not offer it.
            multi = options.get("multi", {}).get("zonesSelection", {})
            if "multi" in options and \
                    multi.get("eligibility", {}).get("enabled") is not False:
                failures.append("{}: multi-AZ zonesSelection eligibility is "
                                "enabled".format(case["id"]))
            single = options.get("single", {}).get("zonesSelection", {})
            if single.get("min") != 0 or single.get("max") != 1:
                failures.append("{}: single-AZ selection range is {}..{}, "
                                "expected 0..1".format(
                                    case["id"], single.get("min"),
                                    single.get("max")))
            for expected in case.get("expected_zones",
                                     [case.get("expected_zone")]):
                if expected and expected not in zones:
                    failures.append("{}: {} not in {}".format(
                        case["id"], expected, zones))
            if "max_zones" in case and len(zones) > case["max_zones"]:
                failures.append("{}: expected at most {} zones, got {}".format(
                    case["id"], case["max_zones"], zones))
        self.report(failures, len(cases))

    def test_create_with_zones(self):
        """Deploys real clusters, all at once so they deploy in parallel."""
        cases = self.selected(self.v4_valid_cases)
        failures = list()
        for case in cases:
            self.log.info("Executing test: {} {}".format(
                case["id"], case["description"]))
            res = self.create_v4_cluster(case)
            if res is None or res.status_code != 202:
                self.log.warning("Result : {}".format(
                    None if res is None else res.content))
                failures.append("{} {}: create returned {}".format(
                    case["id"], case["description"],
                    None if res is None else res.status_code))
                continue
            case["cluster_id"] = self.track_created_cluster(res)

        get_zones = dict()
        for case in cases:
            if not case.get("cluster_id"):
                continue
            label = "{} {}".format(case["id"], case["description"])
            zones = self.validate_deployed_zones(
                case["cluster_id"], case.get("provider", "aws"),
                case["availability"], case.get("zones"), label, failures)
            if zones is not None:
                get_zones[case["cluster_id"]] = zones

        failures.extend(self.list_zones_failures(get_zones))
        # +1 for the list check.
        self.report(failures, len(cases) + 1)

    def test_create_with_invalid_zones(self):
        cases = self.selected(self.v4_invalid_cases)
        failures = list()
        for case in cases:
            self.log.info("Executing test: {} {}".format(
                case["id"], case["description"]))
            res = self.create_v4_cluster(case)
            self.validate_rejection(
                case, res, case.get("expected_status_codes", [422]), failures)
        self.report(failures, len(cases))

    def test_get_and_list_zones(self):
        """Checks the zones field on the wrapper cluster and the list."""
        failures = list()
        res = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
            self.organisation_id, self.project_id, self.cluster_id)
        if res.status_code != 200:
            self.fail("GET wrapper cluster returned {}: {}".format(
                res.status_code, res.content))
        self.validate_deployed_zones(
            self.cluster_id, res.json()["cloudProvider"]["type"],
            res.json()["availability"]["type"], None, "Wrapper cluster",
            failures)
        failures.extend(self.list_zones_failures())
        self.report(failures, 2)

    def test_support_api_create_with_zones(self):
        if not self.internal_support_token():
            self.fail("No internal support token: pass "
                      "internal_support_token or export {}".format(
                          self._internal_support_token_env()))
        cases = self.selected(self.support_valid_cases)
        failures = list()
        for case in cases:
            self.log.info("Executing test: {} /deploy/v2 multi-AZ, 3 valid "
                          "zones".format(case["id"]))
            res = self.support_deploy(case)
            if res is None or res.status_code != 202:
                self.log.warning("Result : {}".format(
                    None if res is None else res.content))
                failures.append("{}: deploy returned {}".format(
                    case["id"], None if res is None else res.status_code))
                continue
            cluster_id = self.track_created_cluster(res)
            if cluster_id:
                self.validate_deployed_zones(
                    cluster_id, "aws", case["availability"], case["zones"],
                    case["id"], failures)
        self.report(failures, len(cases))

    def test_support_api_invalid_zones(self):
        if not self.internal_support_token():
            self.fail("No internal support token: pass "
                      "internal_support_token or export {}".format(
                          self._internal_support_token_env()))
        cases = self.selected(self.support_invalid_cases)
        failures = list()
        for case in cases:
            self.log.info("Executing test: {} /deploy/v2 {}".format(
                case["id"], case["description"]))
            res = self.support_deploy(case)
            self.validate_rejection(
                case, res, [case["expected_status_code"]], failures)
        self.report(failures, len(cases))
