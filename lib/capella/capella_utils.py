import time
import json

from constants.cloud_constants.capella_constants import AWS, Cluster
from global_vars import logger
from capellaAPI.CapellaAPI import CapellaAPI
import uuid


class Pod:
    def __init__(self, url_public):
        self.url_public = url_public


class Tenant:
    def __init__(self, id, user, pwd,
                 secret=None, access=None):
        self.id = id
        self.user = user
        self.pwd = pwd
        self.api_secret_key = secret
        self.api_access_key = access
        self.project_id = None
        self.clusters = dict()


class CapellaUtils(object):
    cidr = "10.0.0.0"
    memcached_port = "11207"
    log = logger.get("infra")

    @staticmethod
    def get_cluster_config(environment="hosted",
                           provider=AWS.__str__,
                           region=AWS.Region.US_WEST_2,
                           single_az=False,
                           plan=Cluster.Plan.DEV_PRO,
                           timezone=Cluster.Timezone.PT,
                           cluster_name="taf_cluster",
                           description=""):
        return {"environment": environment,
                "clusterName": cluster_name,
                "projectId": "",
                "description": description,
                "place": {"singleAZ": single_az,
                          "hosted": {"provider": provider,
                                     "region": region,
                                     "CIDR": None
                                     }
                          },
                "servers": list(),
                "supportPackage": {"timezone": timezone,
                                   "type": plan}
                }

    @staticmethod
    def get_cluster_config_spec(services, count,
                                compute=AWS.ComputeNode.VCPU4_RAM16,
                                storage_type=AWS.StorageType.GP3,
                                storage_size_gb=AWS.StorageSize.MIN,
                                storage_iops=AWS.StorageIOPS.MIN):
        return {
            "services": services,
            "size": count,
            "compute": compute,
            "storage": {"type": storage_type,
                        "size": storage_size_gb,
                        "iops": storage_iops,
                        }
        }

    @staticmethod
    def create_project(pod, tenant, name):
        capella_api = CapellaAPI(pod.url_public,
                                 tenant.api_secret_key,
                                 tenant.api_access_key,
                                 tenant.user,
                                 tenant.pwd)
        resp = capella_api.create_project(tenant.id, name)
        if resp.status_code != 201:
            raise Exception("Creating capella project failed: {}".
                            format(resp.content))
        project_id = json.loads(resp.content).get("id")
        tenant.project_id = project_id
        CapellaUtils.log.info("Project ID: {}".format(project_id))

    @staticmethod
    def delete_project(pod, tenant):
        capella_api = CapellaAPI(pod.url_public,
                                 tenant.api_secret_key,
                                 tenant.api_access_key,
                                 tenant.user,
                                 tenant.pwd)
        capella_api.delete_project(tenant.id, tenant.project_id)
        CapellaUtils.log.info("Project Deleted: {}".format(tenant.project_id))

    @staticmethod
    def get_next_cidr():
        addr = CapellaUtils.cidr.split(".")
        if int(addr[1]) < 255:
            addr[1] = str(int(addr[1]) + 1)
        elif int(addr[2]) < 255:
            addr[2] = str(int(addr[2]) + 1)
        CapellaUtils.cidr = ".".join(addr)
        return CapellaUtils.cidr

    @staticmethod
    def create_cluster(pod, tenant, cluster_details, timeout=1800):
        end_time = time.time() + timeout
        while time.time() < end_time:
            subnet = CapellaUtils.get_next_cidr() + "/20"
            CapellaUtils.log.info("Trying with cidr: {}".format(subnet))
            capella_api = CapellaAPI(pod.url_public,
                                     tenant.api_secret_key,
                                     tenant.api_access_key,
                                     tenant.user,
                                     tenant.pwd)
            if cluster_details.get("overRide"):
                cluster_details.update({"cidr": subnet})
                cluster_details.update({"projectId": tenant.project_id})
                capella_api_resp = capella_api.create_cluster_customAMI(tenant.id, cluster_details)
                if capella_api_resp.status_code == 202:
                    cluster_id = json.loads(capella_api_resp.content).get("id")
                    break
            else:
                cluster_details["place"]["hosted"].update({"CIDR": subnet})
                cluster_details.update({"projectId": tenant.project_id})
                capella_api_resp = capella_api.create_cluster(cluster_details)
                if capella_api_resp.status_code == 202:
                    cluster_id = capella_api_resp.headers['Location'].split("/")[-1]
                    break

            CapellaUtils.log.critical("Create capella cluster failed.")
            CapellaUtils.log.critical("Capella API returned " + str(
                capella_api_resp.status_code))
            CapellaUtils.log.critical(capella_api_resp.json()["message"])

        CapellaUtils.log.info("Cluster created with cluster ID: {}"\
                              .format(cluster_id))
        CapellaUtils.wait_until_done(pod, tenant, cluster_id,
                                     "Creating Cluster {}".format(
                                         cluster_details.get("clusterName")),
                                     timeout=timeout)
        cluster_srv = CapellaUtils.get_cluster_srv(pod, tenant, cluster_id)
        CapellaUtils.allow_my_ip(pod, tenant, cluster_id)
        servers = CapellaUtils.get_nodes(pod, tenant, cluster_id)
        return cluster_id, cluster_srv, servers

    @staticmethod
    def wait_until_done(pod, tenant, cluster_id, msg="", prnt=False,
                        timeout=1800):
        end_time = time.time() + timeout
        capella_api = CapellaAPI(pod.url_public,
                                 tenant.api_secret_key,
                                 tenant.api_access_key,
                                 tenant.user,
                                 tenant.pwd)
        while time.time() < end_time:
            content = CapellaUtils.jobs(capella_api, pod, tenant, cluster_id)
            state = CapellaUtils.get_cluster_state(pod, tenant, cluster_id)
            if state in ["deployment_failed",
                         "deploymentFailed",
                         "redeploymentFailed",
                         "rebalance_failed"]:
                raise Exception("{} for cluster {}".format(
                    state, cluster_id))
            if prnt:
                CapellaUtils.log.info(content)
            if content.get("data") or state != "healthy":
                for data in content.get("data"):
                    data = data.get("data")
                    if data.get("clusterId") == cluster_id:
                        step, progress = data.get("currentStep"), \
                                         data.get("completionPercentage")
                        CapellaUtils.log.info(
                            "{}: Status=={}, State=={}, Progress=={}%"
                            .format(msg, state, step, progress))
                time.sleep(5)
            else:
                CapellaUtils.log.info("{} Ready!!!".format(msg))
                break

    @staticmethod
    def destroy_cluster(cluster):
        capella_api = CapellaAPI(cluster.pod.url_public,
                                 cluster.tenant.api_secret_key,
                                 cluster.tenant.api_access_key,
                                 cluster.tenant.user,
                                 cluster.tenant.pwd)
        resp = capella_api.delete_cluster(cluster.id)
        if resp.status_code != 202:
            raise Exception("Deleting Capella Cluster Failed.")

        time.sleep(10)
        while True:
            resp = capella_api.get_cluster_internal(cluster.tenant.id,
                                                    cluster.tenant.project_id,
                                                    cluster.id)
            content = json.loads(resp.content)
            if content.get("data"):
                CapellaUtils.log.info(
                    "Cluster status %s: %s"
                    % (cluster.cluster_config.get("name"),
                       content.get("data").get("status").get("state")))
                if content.get("data").get("status").get("state") == "destroying":
                    time.sleep(5)
                    continue
            elif content.get("message") == 'Not Found.':
                CapellaUtils.log.info("Cluster is destroyed.")
                cluster.tenant.clusters.pop(cluster.id)
                break

    @staticmethod
    def get_all_buckets(cluster):
        capella_api = CapellaAPI(cluster.pod.url_public,
                                 cluster.tenant.api_secret_key,
                                 cluster.tenant.api_access_key,
                                 cluster.tenant.user,
                                 cluster.tenant.pwd)
        resp = capella_api.get_buckets(
            cluster.tenant.id, cluster.tenant.project_id, cluster.id)
        return resp

    @staticmethod
    def create_bucket(cluster, bucket_params):
        while True:
            state = CapellaUtils.get_cluster_state(
                cluster.pod, cluster.tenant, cluster.id)
            if state == "healthy":
                break
            time.sleep(1)
        capella_api = CapellaAPI(cluster.pod.url_public,
                                 cluster.tenant.api_secret_key,
                                 cluster.tenant.api_access_key,
                                 cluster.tenant.user,
                                 cluster.tenant.pwd)
        resp = capella_api.create_bucket(cluster.tenant.id,
                                         cluster.tenant.project_id,
                                         cluster.id, bucket_params)
        if resp.status_code in [200, 201, 202]:
            CapellaUtils.log.info("Bucket create successfully!")
        else:
            CapellaUtils.log.critical("Bucket creation failed: {}, {}".
                                      format(resp.status_code, resp.content))
            raise Exception("Bucket creation failed")

    @staticmethod
    def get_bucket_id(cluster, name):
        capella_api = CapellaAPI(cluster.pod.url_public,
                                 cluster.tenant.api_secret_key,
                                 cluster.tenant.api_access_key,
                                 cluster.tenant.user,
                                 cluster.tenant.pwd)
        resp = capella_api.get_buckets(
            cluster.tenant.id, cluster.tenant.project_id, cluster.id)
        content = json.loads(resp.content)
        bucket_id = None
        for bucket in content.get("buckets").get("data"):
            if bucket.get("data").get("name") == name:
                bucket_id = bucket.get("data").get("id")
        return bucket_id

    @staticmethod
    def flush_bucket(cluster, name):
        bucket_id = CapellaUtils.get_bucket_id(cluster, name)
        if bucket_id:
            capella_api = CapellaAPI(cluster.pod.url_public,
                                     cluster.tenant.api_secret_key,
                                     cluster.tenant.api_access_key,
                                     cluster.tenant.user,
                                     cluster.tenant.pwd)
            resp = capella_api.flush_bucket(cluster.tenant.id,
                                            cluster.tenant.project_id,
                                            cluster.id,
                                            bucket_id)
            if resp.status >= 200 and resp.status < 300:
                CapellaUtils.log.info("Bucket deleted successfully!")
            else:
                CapellaUtils.log.info(resp.content)
        else:
            CapellaUtils.log.info("Bucket not found.")

    @staticmethod
    def delete_bucket(cluster, name):
        bucket_id = CapellaUtils.get_bucket_id(cluster, name)
        if bucket_id:
            capella_api = CapellaAPI(cluster.pod.url_public,
                                     cluster.tenant.api_secret_key,
                                     cluster.tenant.api_access_key,
                                     cluster.tenant.user,
                                     cluster.tenant.pwd)
            resp = capella_api.delete_bucket(cluster.tenant.id,
                                             cluster.tenant.project_id,
                                             cluster.id,
                                             bucket_id)
            if resp.status_code == 204:
                CapellaUtils.log.info("Bucket deleted successfully!")
            else:
                CapellaUtils.log.critical(resp.content)
                raise Exception("Bucket {} cannot be deleted".format(name))
        else:
            CapellaUtils.log.info("Bucket not found.")

    @staticmethod
    def update_bucket_settings(cluster, bucket_id, bucket_params):
        capella_api = CapellaAPI(cluster.pod.url_public,
                                 cluster.tenant.api_secret_key,
                                 cluster.tenant.api_access_key,
                                 cluster.tenant.user,
                                 cluster.tenant.pwd)
        resp = capella_api.update_bucket_settings(cluster.tenant.id,
                                                  cluster.tenant.project_id,
                                                  cluster.id, bucket_id,
                                                  bucket_params)
        code = resp.status
        if 200 > code or code >= 300:
            CapellaUtils.log.critical("Bucket update failed: %s" % resp.content)
        return resp.status

    @staticmethod
    def scale(cluster, new_config):
        capella_api = CapellaAPI(cluster.pod.url_public,
                                 cluster.tenant.api_secret_key,
                                 cluster.tenant.api_access_key,
                                 cluster.tenant.user,
                                 cluster.tenant.pwd)
        while True:
            resp = capella_api.update_cluster_servers(cluster.id, new_config)
            if resp.status_code != 202:
                result = json.loads(resp.content)
                if result["errorType"] == "ClusterModifySpecsInvalidState":
                    CapellaUtils.wait_until_done(
                        cluster.pod, cluster.tenant, cluster.id,
                        "Wait for healthy cluster state")
            else:
                break

    @staticmethod
    def jobs(capella_api, pod, tenant, cluster_id):
        resp = capella_api.jobs(tenant.project_id, tenant.id, cluster_id)
        if resp.status_code != 200:
            CapellaUtils.log.critical("LOG A BUG: Internal API returns :\
            {}".format(resp.status_code))
            print(resp.content)
            time.sleep(5)
            return CapellaUtils.jobs(capella_api, pod, tenant, cluster_id)
        try:
            content = json.loads(resp.content)
        except Exception as e:
            CapellaUtils.log.critical("LOG A BUG: Internal API returns :\
            {}".format(resp.status_code))
            print(resp.content)
            time.sleep(5)
            return CapellaUtils.jobs(capella_api, pod, tenant, cluster_id)
        return content

    @staticmethod
    def get_cluster_info(pod, tenant, cluster_id):
        capella_api = CapellaAPI(pod.url_public,
                                 tenant.api_secret_key,
                                 tenant.api_access_key,
                                 tenant.user,
                                 tenant.pwd)
        resp = capella_api.get_cluster_info(cluster_id)
        if resp.status_code != 200:
            CapellaUtils.log.critical("LOG A BUG: Fetch Cluster API returns :\
            {}".format(resp.status_code))
            print(resp.content)
            time.sleep(5)
            return CapellaUtils.get_cluster_info(pod, tenant, cluster_id)
        return json.loads(resp.content)

    @staticmethod
    def get_cluster_state(pod, tenant, cluster_id):
        content = CapellaUtils.get_cluster_info(pod, tenant, cluster_id)
        return content.get("status")

    @staticmethod
    def get_cluster_srv(pod, tenant, cluster_id):
        content = CapellaUtils.get_cluster_info(pod, tenant, cluster_id)
        return content.get("endpointsSrv")

    @staticmethod
    def get_nodes(pod, tenant, cluster_id):
        capella_api = CapellaAPI(pod.url_public,
                                 tenant.api_secret_key,
                                 tenant.api_access_key,
                                 tenant.user,
                                 tenant.pwd)
        resp = capella_api.get_nodes(tenant.id, tenant.project_id,
                                     cluster_id)
        if resp.status_code != 200:
            CapellaUtils.log.critical("LOG A BUG: Fetch Cluster Node API returns :\
            {}".format(resp.status_code))
            print(resp.content)
            time.sleep(5)
            return CapellaUtils.get_nodes(pod, tenant, cluster_id)
        CapellaUtils.log.info(json.loads(resp.content))
        return [server.get("data")
                for server in json.loads(resp.content).get("data")]

    @staticmethod
    def get_db_users(pod, tenant, cluster_id, page=1, limit=100):
        capella_api = CapellaAPI(pod.url_public,
                                 tenant.api_secret_key,
                                 tenant.api_access_key,
                                 tenant.user,
                                 tenant.pwd)
        resp = capella_api.get_db_users(tenant.id, tenant.project_id,
                                        cluster_id, page, limit)
        return json.loads(resp.content)

    @staticmethod
    def delete_db_user(pod, tenant, cluster_id, user_id):
        uri = "{}/v2/organizations/{}/projects/{}/clusters/{}/users/{}" \
              .format(tenant.id, tenant.project_id, cluster_id,
                      user_id)
        print(uri)

    @staticmethod
    def create_db_user(pod, tenant, cluster_id, user, pwd):
        capella_api = CapellaAPI(pod.url_public,
                                 tenant.api_secret_key,
                                 tenant.api_access_key,
                                 tenant.user,
                                 tenant.pwd)
        resp = capella_api.create_db_user(tenant.id, tenant.project_id,
                                          cluster_id, user, pwd)
        if resp.status_code != 200:
            result = json.loads(resp.content)
            CapellaUtils.log.critical("Add capella cluster user failed: (}".format(
                resp.status_code))
            CapellaUtils.log.critical(result)
            if result["errorType"] == "ErrDataplaneUserNameExists":
                CapellaUtils.log.warn("User is already added: %s" % result["message"])
                return
            CapellaUtils.create_db_user(pod, tenant, cluster_id, user, pwd)
            CapellaUtils.log.critical(json.loads(resp.content))
        CapellaUtils.log.info(json.loads(resp.content))
        return json.loads(resp.content)

    @staticmethod
    def allow_my_ip(pod, tenant, cluster_id):
        capella_api = CapellaAPI(pod.url_public,
                                 tenant.api_secret_key,
                                 tenant.api_access_key,
                                 tenant.user,
                                 tenant.pwd)
        resp = capella_api.allow_my_ip(tenant.id, tenant.project_id,
                                          cluster_id)
        if resp.status_code != 202:
            result = json.loads(resp.content)
            if result["errorType"] == "ErrAllowListsCreateDuplicateCIDR":
                CapellaUtils.log.warn("IP is already added: %s" % result["message"])
                return
            CapellaUtils.log.critical(resp.content)
            raise Exception("Adding allowed IP failed.")

    @staticmethod
    def load_sample_bucket(pod, tenant, cluster_id, bucket_name):
        capella_api = CapellaAPI(pod.url_public,
                                 tenant.api_secret_key,
                                 tenant.api_access_key,
                                 tenant.user,
                                 tenant.pwd)
        resp = capella_api.load_sample_bucket(tenant.id, tenant.project_id,
                                              cluster_id, bucket_name)
