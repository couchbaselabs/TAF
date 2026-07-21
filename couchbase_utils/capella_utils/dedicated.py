import time
import json
import uuid

from constants.cloud_constants.capella_constants import AWS, Cluster
from global_vars import logger
from capellaAPI.capella.dedicated.CapellaAPI import CapellaAPI
from capellaAPI.capella.dedicated.CapellaAPI_v4 import ClusterOperationsAPIs as ClusterOpsAPIv4
from capellaAPI.capella.dedicated.CapellaAPI_v4 import CapellaAPI as CapellaAPIv4
from capella_utils.common_utils import User


class CapellaUtils(object):
    cidr = "10.0.0.0"
    memcached_port = "11207"
    log = logger.get("infra")

    @staticmethod
    def get_cluster_config(provider=AWS.__str__,
                           region=AWS.Region.US_WEST_2,
                           single_az=False,
                           plan=Cluster.Plan.DEV_PRO,
                           timezone=Cluster.Timezone.PT,
                           cluster_name="taf_cluster",
                           version=None,
                           description=""):
        config = {"cidr": None,
                  "description": description,
                  "name": cluster_name,
                  "plan": plan,
                  "projectId": "",
                  "provider": provider,
                  "region": region,
                  "singleAZ": single_az,
                  "specs": list(),
                  "timezone": timezone
                  }
        if version:
            config.update({"server": version})
        return config

    @staticmethod
    def get_cluster_config_spec(provider, services, count,
                                compute=AWS.ComputeNode.VCPU4_RAM16,
                                storage_type=AWS.StorageType.GP3,
                                storage_size_gb=AWS.StorageSize.MIN,
                                storage_iops=AWS.StorageIOPS.MIN,
                                diskAutoScaling=False):
        return {
            "provider": provider,
            "services": services,
            "count": count,
            "compute": compute,
            "disk": {"type": storage_type,
                     "sizeInGb": storage_size_gb,
                     "iops": storage_iops
                     },
            "diskAutoScaling": {"enabled": diskAutoScaling}
        }

    @staticmethod
    def create_project(pod, tenant, name, num=1):
        capella_api = CapellaAPI(pod.url_public,
                                 tenant.api_secret_key,
                                 tenant.api_access_key,
                                 tenant.user,
                                 tenant.pwd)
        for i in range(num):
            resp = capella_api.create_project(tenant.id, name+"_{}".format(i))
            if resp.status_code != 201:
                raise Exception("Creating capella_utils project failed: {}".
                                format(resp.content))
            project_id = json.loads(resp.content).get("id")
            tenant.projects.append(project_id)
            CapellaUtils.log.info("Project {} is created. PID: {}".format(name, project_id))

    @staticmethod
    def delete_project(pod, tenant, project_id):
        capella_api = CapellaAPI(pod.url_public,
                                 tenant.api_secret_key,
                                 tenant.api_access_key,
                                 tenant.user,
                                 tenant.pwd)
        if type(project_id) == list:
            for _id in project_id:
                capella_api.delete_project(tenant.id, _id)
                CapellaUtils.log.info("Project Deleted: {}".format(_id))
        else:
            capella_api.delete_project(tenant.id, project_id)
            CapellaUtils.log.info("Project Deleted: {}".format(project_id))

    @staticmethod
    def invite_users(pod, tenant, num):
        capella_api = CapellaAPI(pod.url_public,
                                 tenant.api_secret_key,
                                 tenant.api_access_key,
                                 tenant.user,
                                 tenant.pwd)
        for i in range(num):
            i = str(i)
            prefix, suffix = tenant.user.split("@")
            user = prefix+"-" +i+"@"+suffix
            resp = capella_api.create_user(tenant.id,
                                           prefix+"-" +i,
                                           user,
                                           tenant.pwd)
            tenant.users.append(User(user, tenant.pwd))
            if resp.status_code != 200:
                raise Exception("User invitations failed: {}".
                                format(resp.content))
            CapellaUtils.log.info("Tenant: {}, User Invited: {}/{}".format(tenant.id, user, tenant.pwd))

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
    def create_access_secret_key(pod, tenant, name):
        capella_api = CapellaAPI(pod.url_public,
                                 tenant.api_secret_key,
                                 tenant.api_access_key,
                                 tenant.user,
                                 tenant.pwd)
        resp = capella_api.create_access_secret_key(name, tenant.id)
        if resp.status_code != 201:
            raise Exception("Creating Tenant Access/Secret Failed: %s" % resp.content)
        return json.loads(resp.content)

    @staticmethod
    def revoke_access_secret_key(pod, tenant, key_id):
        capella_api = CapellaAPI(pod.url_public,
                                 tenant.api_secret_key,
                                 tenant.api_access_key,
                                 tenant.user,
                                 tenant.pwd)
        resp = capella_api.revoke_access_secret_key(tenant.id, key_id)
        if resp.status_code != 204:
            raise Exception(
                "Revoking Tenant Access/Secret Failed: %s" % resp.content)

    @staticmethod
    def create_cluster(pod, tenant, cluster_details, timeout=1800):
        end_time = time.time() + timeout
        subnet = CapellaUtils.get_next_cidr() + "/20"
        while time.time() < end_time:
            CapellaUtils.log.info("Trying with cidr: {}".format(subnet))
            capella_api = CapellaAPI(pod.url_public,
                                     tenant.api_secret_key,
                                     tenant.api_access_key,
                                     tenant.user,
                                     tenant.pwd)
            cluster_details.update({"cidr": subnet})
            cluster_details.update({"projectId": tenant.projects[0]})
            CapellaUtils.log.info(cluster_details)
            if cluster_details.get("overRide"):
                resp = capella_api.create_cluster_customAMI(tenant.id, cluster_details)
            else:
                resp = capella_api.create_cluster_CPUI(tenant.id, cluster_details)
            if resp.status_code == 202:
                cluster_id = json.loads(resp.content).get("id")
                break
            elif resp.status_code == 500:
                CapellaUtils.log.critical(str(resp.content))
                raise Exception(str(resp.content))
            elif resp.status_code == 422:
                content = resp.content.decode("utf-8")
                if (content.find("not allowed based on your activation status") !=
                        -1):
                    CapellaUtils.log.critical("Tenant is not activated yet...retrying")
                    time.sleep(5)
                if content.find("CIDR") != -1:
                    subnet = CapellaUtils.get_next_cidr() + "/20"
                else:
                    CapellaUtils.log.critical(content)
                    raise Exception("Cluster deployment failed.")
            else:
                CapellaUtils.log.critical("Create capella_utils cluster failed.")
                CapellaUtils.log.critical("Capella API returned " + str(
                    resp.status_code))
                CapellaUtils.log.critical(resp.json()["message"])


        CapellaUtils.log.info("Cluster created with cluster ID: {}"\
                              .format(cluster_id))
        CapellaUtils.wait_until_done(pod, tenant, cluster_id,
                                     "Creating Cluster {}".format(cluster_id),
                                     timeout=timeout)
        cluster_srv = CapellaUtils.get_cluster_srv(pod, tenant, cluster_id)
        retry = 0
        while retry < 5:
            try:
                CapellaUtils.allow_my_ip(pod, tenant, cluster_id, True)
                break
            except Exception as err:
                CapellaUtils.log.error(str(err))
                retry += 1
                if retry < 5:
                    CapellaUtils.log.info("Retrying to add IP to allow list")
                    time.sleep(30 * retry)
                else:
                    raise Exception(str(err))
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
        check_healthy_state = 0
        while time.time() < end_time and check_healthy_state >= 0:
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
                check_healthy_state -= 1
                time.sleep(10)

    @staticmethod
    def destroy_cluster(pod, tenant, cluster, timeout=1800):
        """
        Delete cluster.id via the internal API and block until it either
        disappears (get_cluster_internal returns "Not Found.") or *timeout*
        seconds elapse.

        Any status other than "destroying" (e.g. the CP reverting the
        cluster to "healthy"/"destroyFailed" because the async destroy job
        errored out) is treated as still-in-progress and polled with a
        sleep rather than looping straight back to the top with no delay --
        previously such a state fell through the if/elif with no sleep and
        no exit condition, hot-looping against the Capella API forever.
        That state is also not silently accepted as success: if the cluster
        never reaches "Not Found." within *timeout*, this raises instead of
        polling indefinitely, so a stuck/failed destroy surfaces as a clear
        test failure rather than an unbounded hang.
        """
        capella_api = CapellaAPI(pod.url_public,
                                 tenant.api_secret_key,
                                 tenant.api_access_key,
                                 tenant.user,
                                 tenant.pwd)
        resp = capella_api.delete_cluster_internal(tenant.id, tenant.projects[0], cluster.id)
        if resp.status_code != 202:
            raise Exception("Deleting Capella Cluster Failed: {} {}".format(
                resp.status_code, resp.content))

        time.sleep(10)
        end_time = time.time() + timeout
        last_state = None
        while time.time() < end_time:
            resp = capella_api.get_cluster_internal(tenant.id,
                                                    tenant.projects[0],
                                                    cluster.id)
            content = json.loads(resp.content)
            if content.get("data"):
                last_state = content.get("data").get("status").get("state")
                CapellaUtils.log.info(
                    "Cluster status %s: %s" % (cluster.id, last_state))
                if last_state == "destroying":
                    time.sleep(5)
                    continue
                # Any other non-"destroying" state (e.g. a reverted
                # "healthy", or a hypothetical "destroyFailed") means the
                # async destroy job did not proceed as expected. Keep
                # polling (bounded by the overall timeout below) instead of
                # either hot-looping or treating it as success.
                time.sleep(10)
                continue
            elif content.get("message") == 'Not Found.':
                CapellaUtils.log.info("Cluster is destroyed.")
                # Guard against a concurrent/duplicate destroy_cluster call
                # for the same cluster racing this one to "Not Found." --
                # list.remove() on an already-removed item raises ValueError.
                if cluster in tenant.clusters:
                    tenant.clusters.remove(cluster)
                return

        raise Exception(
            "Cluster {} did not finish destroying within {}s (last known "
            "state: {})".format(cluster.id, timeout, last_state))

    @staticmethod
    def get_all_buckets(pod, tenant, cluster):
        capella_api = CapellaAPI(pod.url_public,
                                 tenant.api_secret_key,
                                 tenant.api_access_key,
                                 tenant.user,
                                 tenant.pwd)
        resp = capella_api.get_buckets(
            tenant.id, tenant.projects[0], cluster.id)
        return resp

    @staticmethod
    def create_bucket(pod, tenant, cluster, bucket_params, retries=5, retry_wait=60):
        deadline = time.time() + 600
        while time.time() < deadline:
            state = CapellaUtils.get_cluster_state(pod, tenant, cluster.id)
            if state == "healthy":
                break
            CapellaUtils.log.info(f"Cluster {cluster.id} is not healthy hence cannot create bucket, waiting for 10 seconds..")
            time.sleep(10)
        else:
            raise Exception(f"Cluster {cluster.id} did not reach healthy state within 600s")
        capella_api = CapellaAPI(pod.url_public,
                                 tenant.api_secret_key,
                                 tenant.api_access_key,
                                 tenant.user,
                                 tenant.pwd)
        for attempt in range(1, retries + 1):
            resp = capella_api.create_bucket(tenant.id,
                                             tenant.projects[0],
                                             cluster.id, bucket_params)
            if resp.status_code in [200, 201, 202]:
                CapellaUtils.log.info("Bucket {} created successfully on cluster {}!".format(
                    bucket_params.get("name"), cluster.id))
                return
            CapellaUtils.log.critical("Bucket creation failed (attempt {}/{}): {}, {}".format(
                attempt, retries, resp.status_code, resp.content))
            if attempt < retries:
                time.sleep(retry_wait)
        raise Exception("Bucket creation failed")

    @staticmethod
    def get_bucket_id(pod, tenant, cluster, name):
        capella_api = CapellaAPI(pod.url_public,
                                 tenant.api_secret_key,
                                 tenant.api_access_key,
                                 tenant.user,
                                 tenant.pwd)
        resp = capella_api.get_buckets(
            tenant.id, tenant.projects[0], cluster.id)
        content = json.loads(resp.content)
        bucket_id = None
        for bucket in content.get("buckets").get("data"):
            if bucket.get("data").get("name") == name:
                bucket_id = bucket.get("data").get("id")
        return bucket_id

    @staticmethod
    def flush_bucket(pod, tenant, cluster, name):
        bucket_id = CapellaUtils.get_bucket_id(pod, tenant, cluster, name)
        if bucket_id:
            capella_api = CapellaAPI(pod.url_public,
                                     tenant.api_secret_key,
                                     tenant.api_access_key,
                                     tenant.user,
                                     tenant.pwd)
            resp = capella_api.flush_bucket(tenant.id,
                                            tenant.projects[0],
                                            cluster.id,
                                            bucket_id)
            if resp.status_code >= 200 and resp.status_code < 300:
                CapellaUtils.log.info("Bucket deleted successfully!")
            else:
                CapellaUtils.log.info(resp.content)
        else:
            CapellaUtils.log.info("Bucket not found.")

    @staticmethod
    def delete_bucket(pod, tenant, cluster, name):
        bucket_id = CapellaUtils.get_bucket_id(pod, tenant, cluster, name)
        if bucket_id:
            capella_api = CapellaAPI(pod.url_public,
                                     tenant.api_secret_key,
                                     tenant.api_access_key,
                                     tenant.user,
                                     tenant.pwd)
            resp = capella_api.delete_bucket(tenant.id,
                                             tenant.projects[0],
                                             cluster.id,
                                             bucket_id)
            if resp.status_code == 204:
                CapellaUtils.log.info("Bucket deleted successfully!")
                cluster.buckets = [b for b in cluster.buckets if b.name != name]
            else:
                try:
                    error_type = resp.json().get("errorType", "")
                except Exception:
                    error_type = ""
                if error_type == "BucketNotFound":
                    CapellaUtils.log.info(
                        "Bucket {} already gone (BucketNotFound) — "
                        "treating delete as success".format(name))
                    cluster.buckets = [b for b in cluster.buckets if b.name != name]
                else:
                    CapellaUtils.log.critical(resp.content)
                    raise Exception(
                        "Bucket {} cannot be deleted: HTTP {} — {}".format(
                            name, resp.status_code, resp.content))
        else:
            CapellaUtils.log.info("Bucket not found.")

    @staticmethod
    def update_bucket_settings(pod, tenant, cluster, bucket_id, bucket_params):
        capella_api = CapellaAPI(pod.url_public,
                                 tenant.api_secret_key,
                                 tenant.api_access_key,
                                 tenant.user,
                                 tenant.pwd)
        resp = capella_api.update_bucket_settings(tenant.id,
                                                  tenant.projects[0],
                                                  cluster.id, bucket_id,
                                                  bucket_params)
        code = resp.status
        if 200 > code or code >= 300:
            CapellaUtils.log.critical("Bucket update failed: %s" % resp.content)
        return resp.status

    @staticmethod
    def scale(pod, tenant, cluster, specs, timeout=600):
        capella_api = CapellaAPI(pod.url_public,
                                 tenant.api_secret_key,
                                 tenant.api_access_key,
                                 tenant.user,
                                 tenant.pwd)
        while True:
            resp = capella_api.update_cluster_sepcs(tenant.id,
                                                    tenant.projects[0], cluster.id, specs)
            if resp.status_code != 202:
                result = json.loads(resp.content)
                CapellaUtils.log.critical(result)
                if result["errorType"] in ["ClusterModifySpecsInvalidState", "EntityNotWritable", "EntityStateInvalid"]:
                    CapellaUtils.wait_until_done(
                        pod, tenant, cluster.id,
                        "Wait for healthy cluster state", timeout=timeout)
                else:
                    raise Exception(result)
            else:
                break

    @staticmethod
    def upgrade(pod, tenant, cluster, config):
        capella_api = CapellaAPI(pod.url_public,
                                 tenant.api_secret_key,
                                 tenant.api_access_key,
                                 tenant.user,
                                 tenant.pwd)
        while True:
            resp = capella_api.upgrade_cluster(tenant.id,
                                               tenant.projects[0],
                                               cluster.id, config)
            if resp.status_code != 202:
                result = json.loads(resp.content)
                if result["errorType"] == "ClusterModifySpecsInvalidState":
                    CapellaUtils.wait_until_done(
                        pod, tenant, cluster.id,
                        "Wait for healthy cluster state")
                else:
                    CapellaUtils.log.critical(result)
                    raise Exception(result)
            else:
                break

    @staticmethod
    def jobs(capella_api, pod, tenant, cluster_id, timeout=120):
        deadline = time.time() + timeout
        while True:
            resp = capella_api.jobs(tenant.projects[0], tenant.id, cluster_id)
            if resp.status_code == 404:
                # Cluster genuinely gone (already destroyed) -- retrying
                # forever here is the same unbounded-recursion bug fixed in
                # get_cluster_info/get_nodes (Jenkins build 16458): this sibling
                # was missed in that pass and produced the identical stuck
                # ThreadPool worker hammering /jobs every 5s on build 16471.
                raise Exception(
                    f"Cluster {cluster_id} not found (404) -- it may "
                    f"already be destroyed")
            if resp.status_code == 200:
                try:
                    return json.loads(resp.content)
                except Exception:
                    pass
            CapellaUtils.log.critical("LOG A BUG: Internal API returns :\
            {}".format(resp.status_code))
            print(resp.content)
            if time.time() >= deadline:
                raise Exception(
                    f"jobs() for {cluster_id} kept failing (last status "
                    f"{resp.status_code}) for over {timeout}s")
            time.sleep(5)

    @staticmethod
    def get_cluster_info(pod, tenant, cluster_id, timeout=120):
        capella_api = CapellaAPI(pod.url_public,
                                 tenant.api_secret_key,
                                 tenant.api_access_key,
                                 tenant.user,
                                 tenant.pwd)
        deadline = time.time() + timeout
        while True:
            resp = capella_api.get_cluster_info(tenant.id, tenant.projects[0], cluster_id)
            if resp.status_code == 200:
                return json.loads(resp.content)
            if resp.status_code == 404:
                # Cluster genuinely gone (already destroyed) -- retrying
                # forever here is exactly what produced an orphaned worker
                # thread hammering this endpoint indefinitely on Jenkins
                # build 16458. Fail fast instead.
                raise Exception(
                    f"Cluster {cluster_id} not found (404) -- it may "
                    f"already be destroyed")
            CapellaUtils.log.critical("LOG A BUG: Fetch Cluster API returns :\
            {}".format(resp.status_code))
            print(resp.content)
            if time.time() >= deadline:
                raise Exception(
                    f"get_cluster_info for {cluster_id} kept failing "
                    f"(last status {resp.status_code}) for over {timeout}s")
            time.sleep(5)

    @staticmethod
    def get_cluster_info_internal(pod, tenant, cluster_id, timeout=120):
        capella_api = CapellaAPI(pod.url_public,
                                 tenant.api_secret_key,
                                 tenant.api_access_key,
                                 tenant.user,
                                 tenant.pwd,
                                 pod.TOKEN)
        deadline = time.time() + timeout
        while True:
            resp = capella_api.get_cluster_info_internal(cluster_id)
            if resp.status_code == 200:
                return json.loads(resp.content)
            if resp.status_code == 404:
                raise Exception(
                    f"Cluster {cluster_id} not found (404) -- it may "
                    f"already be destroyed")
            CapellaUtils.log.critical("LOG A BUG: Fetch Cluster API returns :\
            {}".format(resp.status_code))
            print(resp.content)
            if time.time() >= deadline:
                raise Exception(
                    f"get_cluster_info_internal for {cluster_id} kept "
                    f"failing (last status {resp.status_code}) for over "
                    f"{timeout}s")
            time.sleep(5)

    @staticmethod
    def get_cluster_nodes_internal(pod, tenant, cluster_id, timeout=120):
        capella_api = CapellaAPI(pod.url_public,
                                 tenant.api_secret_key,
                                 tenant.api_access_key,
                                 tenant.user,
                                 tenant.pwd,
                                 pod.TOKEN)
        deadline = time.time() + timeout
        while True:
            resp = capella_api.get_cluster_nodes_internal(cluster_id)
            if resp.status_code == 200:
                return json.loads(resp.content)
            if resp.status_code == 404:
                raise Exception(
                    f"Cluster {cluster_id} not found (404) -- it may "
                    f"already be destroyed")
            CapellaUtils.log.critical("LOG A BUG: Fetch Cluster API returns :\
            {}".format(resp.status_code))
            print(resp.content)
            if time.time() >= deadline:
                raise Exception(
                    f"get_cluster_nodes_internal for {cluster_id} kept "
                    f"failing (last status {resp.status_code}) for over "
                    f"{timeout}s")
            time.sleep(5)

    @staticmethod
    def get_cluster_state(pod, tenant, cluster_id):
        content = CapellaUtils.get_cluster_info(pod, tenant, cluster_id)
        return content.get("data").get("status").get("state")

    @staticmethod
    def get_cluster_srv(pod, tenant, cluster_id):
        content = CapellaUtils.get_cluster_info(pod, tenant, cluster_id)
        return content.get("data").get("connect").get("srv")

    @staticmethod
    def get_nodes(pod, tenant, cluster_id, timeout=120):
        capella_api = CapellaAPI(pod.url_public,
                                 tenant.api_secret_key,
                                 tenant.api_access_key,
                                 tenant.user,
                                 tenant.pwd)
        deadline = time.time() + timeout
        while True:
            resp = capella_api.get_nodes(tenant.id, tenant.projects[0],
                                         cluster_id)
            if resp.status_code == 200:
                return [server.get("data")
                        for server in json.loads(resp.content).get("data")]
            if resp.status_code == 404:
                raise Exception(
                    f"Cluster {cluster_id} not found (404) -- it may "
                    f"already be destroyed")
            CapellaUtils.log.critical("LOG A BUG: Fetch Cluster Node API returns :\
            {}".format(resp.status_code))
            print(resp.content)
            if time.time() >= deadline:
                raise Exception(
                    f"get_nodes for {cluster_id} kept failing (last "
                    f"status {resp.status_code}) for over {timeout}s")
            time.sleep(5)

    @staticmethod
    def get_db_users(pod, tenant, cluster_id, page=1, limit=100):
        capella_api = CapellaAPI(pod.url_public,
                                 tenant.api_secret_key,
                                 tenant.api_access_key,
                                 tenant.user,
                                 tenant.pwd)
        resp = capella_api.get_db_users(tenant.id, tenant.projects[0],
                                        cluster_id, page, limit)
        return json.loads(resp.content)

    @staticmethod
    def delete_db_user(pod, tenant, cluster_id, user_id):
        uri = "{}/v2/organizations/{}/projects/{}/clusters/{}/users/{}" \
              .format(tenant.id, tenant.projects[0], cluster_id,
                      user_id)
        print(uri)

    @staticmethod
    def create_db_user(pod, tenant, cluster_id, user, pwd):
        capella_api = CapellaAPI(pod.url_public,
                                 tenant.api_secret_key,
                                 tenant.api_access_key,
                                 tenant.user,
                                 tenant.pwd)
        resp = capella_api.create_db_user(tenant.id, tenant.projects[0],
                                          cluster_id, user, pwd)
        if resp.status_code != 200:
            result = json.loads(resp.content)
            CapellaUtils.log.critical("Add capella_utils cluster user failed: {}".format(
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
    def allow_my_ip(pod, tenant, cluster_id, allowall=False):
        capella_api = CapellaAPI(pod.url_public,
                                 tenant.api_secret_key,
                                 tenant.api_access_key,
                                 tenant.user,
                                 tenant.pwd)
        resp = capella_api.allow_my_ip(tenant.id, tenant.projects[0],
                                        cluster_id, allowall)
        if resp.status_code != 202:
            result = json.loads(resp.content)
            if result["errorType"] == "ErrAllowListsCreateDuplicateCIDR":
                CapellaUtils.log.warn("IP is already added: %s" % result["message"])
            else:
                CapellaUtils.log.critical(resp.content)
                raise Exception("Adding allowed IP failed.")

    @staticmethod
    def load_sample_bucket(pod, tenant, cluster_id, bucket_name):
        capella_api = CapellaAPI(pod.url_public,
                                 tenant.api_secret_key,
                                 tenant.api_access_key,
                                 tenant.user,
                                 tenant.pwd)
        resp = capella_api.load_sample_bucket(tenant.id, tenant.projects[0],
                                              cluster_id, bucket_name)

    @staticmethod
    def create_xdcr_replication(pod, tenant, cluster_id, payload):
        capella_api = CapellaAPI(pod.url_public,
                                 tenant.api_secret_key,
                                 tenant.api_access_key,
                                 tenant.user,
                                 tenant.pwd)
        resp = capella_api.create_xdcr_replication(tenant.id, tenant.projects[0],
                                              cluster_id, payload)
        CapellaUtils.log.info("Response from create xdcr replication API: {}".format(resp))

    @staticmethod
    def backup_now(pod, tenant, cluster_id, bucket_name):
        capella_api = CapellaAPI(pod.url_public,
                                 tenant.api_secret_key,
                                 tenant.api_access_key,
                                 tenant.user,
                                 tenant.pwd)
        resp = capella_api.backup_now(tenant_id=tenant.id, project_id=tenant.projects[0],
                                      cluster_id=cluster_id, bucket_name=bucket_name)
        CapellaUtils.log.info("Response from backup_now method: {}".format(resp))

    @staticmethod
    def restore_from_backup(pod, tenant, cluster_id, bucket_name):
        capella_api = CapellaAPI(pod.url_public,
                                 tenant.api_secret_key,
                                 tenant.api_access_key,
                                 tenant.user,
                                 tenant.pwd)
        resp = capella_api.restore_from_backup(tenant_id=tenant.id, project_id=tenant.projects[0],
                                               cluster_id=cluster_id, bucket_name=bucket_name)
        CapellaUtils.log.info("Response from restore_from_backup method: {}".format(resp))

    @staticmethod
    def list_all_backups(pod, tenant, cluster, bucket_name):
        capella_api = CapellaAPI(pod.url_public,
                                 tenant.api_secret_key,
                                 tenant.api_access_key,
                                 tenant.user,
                                 tenant.pwd)
        bucket_id = capella_api.get_backups_bucket_id(tenant_id=tenant.id, project_id=tenant.projects[0],
                                                      cluster_id=cluster.id, bucket_name=bucket_name)
        resp = capella_api.list_all_bucket_backups(tenant_id=tenant.id,
                                                   project_id=tenant.projects[0],
                                                   cluster_id=cluster.id, bucket_id=bucket_id)
        CapellaUtils.log.info("Response from list_all_backups method: {}".format(resp))
        return resp

    @staticmethod
    def trigger_log_collection(pod, tenant, cluster_id, log_id=""):
        capella_api = CapellaAPI(pod.url_public,
                                 tenant.api_secret_key,
                                 tenant.api_access_key,
                                 tenant.user,
                                 tenant.pwd,
                                 pod.TOKEN)
        resp = capella_api.trigger_log_collection(cluster_id,
                                                  ticketId=log_id)
        if resp.status_code != 201:
            CapellaUtils.log.critical("Logs collection failed:{}".
                                      format(resp.status_code))
            raise Exception("Logs collection failed: {}".
                            format(resp.content))

    @staticmethod
    def check_logs_collect_status(pod, tenant, cluster_id, timeout=1200):
        capella_api = CapellaAPI(pod.url_public,
                                 tenant.api_secret_key,
                                 tenant.api_access_key,
                                 tenant.user,
                                 tenant.pwd,
                                 pod.TOKEN)
        timeout = timeout
        start_time = time.time()
        while time.time() < start_time + timeout:
            resp = capella_api.get_cluster_tasks(cluster_id)
            tasks = json.loads(resp.content)
            if resp.status_code != 200:
                CapellaUtils.log.critical("Logs collection failed:{}".
                                          format(resp.status_code))
                raise Exception("Logs collection failed: {}".
                                format(resp.content))
            task = [task for task in tasks if task["type"] == "clusterLogsCollection"][0]
            CapellaUtils.log.info("Logs for Cluster {}: Status {} - Progress {}%".
                                  format(cluster_id, task["status"], task["progress"]))
            if task["status"] == "completed":
                return task
            time.sleep(10)

    @staticmethod
    def get_cluster_tasks(pod, tenant, cluster_id):
        capella_api = CapellaAPI(pod.url_public,
                                 tenant.api_secret_key,
                                 tenant.api_access_key,
                                 tenant.user,
                                 tenant.pwd)
        resp = capella_api.get_cluster_tasks(cluster_id)
        if resp.status_code != 200:
            CapellaUtils.log.critical("Logs collection failed:{}".
                                      format(resp.status_code))
            raise Exception("Logs collection failed: {}".
                            format(resp.content))
        return json.loads(resp.content)

    @staticmethod
    def get_root_ca(pod, tenant, cluster_id):
        capella_api = CapellaAPI(pod.url_public,
                                 tenant.api_secret_key,
                                 tenant.api_access_key,
                                 tenant.user,
                                 tenant.pwd)
        resp = capella_api.get_root_ca(cluster_id)
        if resp.status_code != 200:
            CapellaUtils.log.critical("Fetching Root CA Cert failed for cluster {}:{}".
                                      format(cluster_id, resp.status_code))
            raise Exception("Fetching Root CA Cert failed: {}".
                            format(resp.content))
        return json.loads(resp.content)

    @staticmethod
    def enable_fusion(pod, tenant, cluster_id):
        capella_api = CapellaAPI(pod.url_public,
                                 tenant.api_secret_key,
                                 tenant.api_access_key,
                                 tenant.user,
                                 tenant.pwd)
        resp = capella_api.enable_fusion(tenant.id, tenant.projects[0], cluster_id)
        if resp.status_code != 200:
            # Return the response so callers can assert on expected rejections
            # (e.g. enable from enabled state).
            CapellaUtils.log.critical("Enabling Fusion failed for cluster {}:{}".
                                      format(cluster_id, resp.status_code))
        return resp

    @staticmethod
    def disable_fusion(pod, tenant, cluster_id):
        capella_api = CapellaAPI(pod.url_public,
                                 tenant.api_secret_key,
                                 tenant.api_access_key,
                                 tenant.user,
                                 tenant.pwd)
        resp = capella_api.disable_fusion(tenant.id, tenant.projects[0], cluster_id)
        if resp.status_code != 200:
            # Return the response so callers can assert on expected rejections
            # (e.g. disable during a leased/rebalancing window).
            CapellaUtils.log.critical("Disabling Fusion failed for cluster {}:{}".
                                      format(cluster_id, resp.status_code))
        return resp

    @staticmethod
    def get_fusion_status(pod, tenant, cluster_id):
        capella_api = CapellaAPI(pod.url_public,
                                 tenant.api_secret_key,
                                 tenant.api_access_key,
                                 tenant.user,
                                 tenant.pwd,
                                 pod.TOKEN)
        resp = capella_api.fusion_status_internal(cluster_id=cluster_id)
        if resp.status_code != 200:
            CapellaUtils.log.critical("Getting Fusion status failed for cluster {}:{}".
                                      format(cluster_id, resp.status_code))
            raise Exception("Getting Fusion status failed: {}".
                            format(resp.content))
        return json.loads(resp.content)

    @staticmethod
    def stop_fusion(pod, tenant, cluster_id):
        capella_api = CapellaAPI(pod.url_public,
                                 tenant.api_secret_key,
                                 tenant.api_access_key,
                                 tenant.user,
                                 tenant.pwd)
        resp = capella_api.stop_fusion(tenant_id=tenant.id, project_id=tenant.projects[0], cluster_id=cluster_id)
        if resp.status_code != 200:
            CapellaUtils.log.critical("Stopping Fusion failed for cluster {}:{}".
                                      format(cluster_id, resp.status_code))
            raise Exception("Stopping Fusion failed: {}".
                            format(resp.content))
        return json.loads(resp.content)

    @staticmethod
    def stop_fusion_internal(pod, tenant, cluster_id):
        """Stop fusion via the internal support API: POST /internal/support/clusters/{id}/fusion/stop."""
        capella_api = CapellaAPI(pod.url_public,
                                 tenant.api_secret_key,
                                 tenant.api_access_key,
                                 tenant.user,
                                 tenant.pwd,
                                 pod.TOKEN)
        resp = capella_api.stop_fusion_internal(cluster_id=cluster_id)
        if resp.status_code != 200:
            CapellaUtils.log.critical("Stopping Fusion (internal) failed for cluster {}:{}".
                                      format(cluster_id, resp.status_code))
        return resp

    @staticmethod
    def override_fusion_rebalances(pod, tenant, cluster_id, override):
        """Set/unset fusion rebalance override via POST /internal/support/clusters/{id}/fusion/overrideRebalances.

        When override=True, the control plane skips fusion for the next rebalance even
        though fusion remains enabled. Set override=False to restore normal fusion behaviour.
        """
        capella_api = CapellaAPI(pod.url_public,
                                 tenant.api_secret_key,
                                 tenant.api_access_key,
                                 tenant.user,
                                 tenant.pwd,
                                 pod.TOKEN)
        resp = capella_api.override_fusion_rebalances(cluster_id=cluster_id, override=override)
        if resp.status_code != 200:
            CapellaUtils.log.critical(
                "Override fusion rebalances (override={}) failed for cluster {}: {}".format(
                    override, cluster_id, resp.status_code))
            raise Exception("Override fusion rebalances failed: {}".format(resp.content))
        return json.loads(resp.content) if resp.content else {}

    @staticmethod
    def get_fusion_config(pod, tenant, resource_id):
        """GET /internal/support/configs/{resource_id}/fusion.

        resource_id can be a tenant ID, cluster ID, or node ID.
        Returns the parsed JSON config dict, or {} if none is set.
        """
        capella_api = CapellaAPI(pod.url_public,
                                 tenant.api_secret_key,
                                 tenant.api_access_key,
                                 tenant.user,
                                 tenant.pwd,
                                 pod.TOKEN)
        resp = capella_api.get_fusion_config(resource_id=resource_id)
        if resp.status_code != 200:
            CapellaUtils.log.critical(
                "Getting fusion config failed for resource {}: {}".format(
                    resource_id, resp.status_code))
            raise Exception("Getting fusion config failed: {}".format(resp.content))
        return json.loads(resp.content) if resp.content else {}

    @staticmethod
    def _build_fusion_config(min_split_size=None, max_slots=None,
                             iops=None, throughput=None):
        config = {}
        if min_split_size is not None or max_slots is not None:
            config["manifest"] = {}
            if min_split_size is not None:
                config["manifest"]["minSplitSize"] = min_split_size
            if max_slots is not None:
                config["manifest"]["maxSlots"] = max_slots
        if iops is not None or throughput is not None:
            config["accelerator"] = {"guestVolumes": {}}
            if iops is not None:
                config["accelerator"]["guestVolumes"]["iops"] = iops
            if throughput is not None:
                config["accelerator"]["guestVolumes"]["throughput"] = throughput
        return config

    @staticmethod
    def set_fusion_config(pod, tenant, resource_id, min_split_size=None,
                          max_slots=None, iops=None, throughput=None):
        """PUT /internal/support/configs/{resource_id}/fusion.

        Replaces the entire fusion config for the resource.
        resource_id can be a tenant ID, cluster ID, or node ID.
        min_split_size: minimum shard size in bytes (default 50 GB).
        max_slots: maximum accelerator nodes per cluster node (default 22).
        iops: EBS volume IOPS (default 3000).
        throughput: EBS volume throughput in MB/s (default 125).
        """
        config = CapellaUtils._build_fusion_config(
            min_split_size=min_split_size, max_slots=max_slots,
            iops=iops, throughput=throughput)
        capella_api = CapellaAPI(pod.url_public,
                                 tenant.api_secret_key,
                                 tenant.api_access_key,
                                 tenant.user,
                                 tenant.pwd,
                                 pod.TOKEN)
        resp = capella_api.set_fusion_config(resource_id=resource_id, config=config)
        if resp.status_code != 204:
            CapellaUtils.log.critical(
                "Setting fusion config failed for resource {}: {}".format(
                    resource_id, resp.status_code))
            raise Exception("Setting fusion config failed: {}".format(resp.content))

    @staticmethod
    def patch_fusion_config(pod, tenant, resource_id, min_split_size=None,
                            max_slots=None, iops=None, throughput=None):
        """PATCH /internal/support/configs/{resource_id}/fusion.

        Merges the provided fields into the existing fusion config.
        Only supplied (non-None) fields are sent; unset fields are unchanged.
        Prefer this over set_fusion_config to avoid overwriting unrelated fields.
        resource_id can be a tenant ID, cluster ID, or node ID.
        min_split_size: minimum shard size in bytes (default 50 GB).
        max_slots: maximum accelerator nodes per cluster node (default 22).
        iops: EBS volume IOPS (default 3000).
        throughput: EBS volume throughput in MB/s (default 125).
        """
        config = CapellaUtils._build_fusion_config(
            min_split_size=min_split_size, max_slots=max_slots,
            iops=iops, throughput=throughput)
        capella_api = CapellaAPI(pod.url_public,
                                 tenant.api_secret_key,
                                 tenant.api_access_key,
                                 tenant.user,
                                 tenant.pwd,
                                 pod.TOKEN)
        resp = capella_api.patch_fusion_config(resource_id=resource_id, config=config)
        if resp.status_code != 204:
            CapellaUtils.log.critical(
                "Patching fusion config failed for resource {}: {}".format(
                    resource_id, resp.status_code))
            raise Exception("Patching fusion config failed: {}".format(resp.content))

    @staticmethod
    def delete_fusion_config(pod, tenant, resource_id):
        """DELETE /internal/support/configs/{resource_id}/fusion.

        Removes the fusion config for the resource, reverting to system defaults.
        """
        capella_api = CapellaAPI(pod.url_public,
                                 tenant.api_secret_key,
                                 tenant.api_access_key,
                                 tenant.user,
                                 tenant.pwd,
                                 pod.TOKEN)
        resp = capella_api.delete_fusion_config(resource_id=resource_id)
        if resp.status_code != 204:
            CapellaUtils.log.critical(
                "Deleting fusion config failed for resource {}: {}".format(
                    resource_id, resp.status_code))
            raise Exception("Deleting fusion config failed: {}".format(resp.content))

    @staticmethod
    def update_feature_flag_globally(pod, tenant, ff, value):
        capella_api = CapellaAPI(pod.url_public,
                                 tenant.api_secret_key,
                                 tenant.api_access_key,
                                 tenant.user,
                                 tenant.pwd,
                                 pod.TOKEN)
        resp = capella_api.update_global_feature_flag(ff, {"value": value})
        if resp.status_code != 204:
            CapellaUtils.log.critical(f"Updating  Feature \
                                       Flag {ff} failed for pod {resp.status_code}")
            raise Exception("Updating Fusion Feature Flag failed: {}".
                            format(resp.content))
        CapellaUtils.log.info(f"Updated the {ff} feature flag successfully")
        return
    
    @staticmethod
    def create_cluster_feature_flag(pod, tenant, cluster_id, ff, value):
        capella_api = CapellaAPI(pod.url_public,
                                 tenant.api_secret_key,
                                 tenant.api_access_key,
                                 tenant.user,
                                 tenant.pwd,
                                 pod.TOKEN)
        resp = capella_api.create_cluster_feature_flag(tenant.id, cluster_id, ff, {"value": value})
        if resp.status_code not in [200, 201, 204]:
            try:
                error = json.loads(resp.content)
            except Exception:
                error = {}
            if error.get("errorType") == "FeatureFlagAlreadyExists":
                CapellaUtils.log.info(
                    f"Cluster feature flag {ff} already exists for cluster {cluster_id}, updating")
                resp = capella_api.update_cluster_feature_flag(
                    tenant.id, cluster_id, ff, {"value": value})
                if resp.status_code not in [200, 204]:
                    CapellaUtils.log.critical(
                        f"Updating cluster feature flag {ff} failed: {resp.status_code}")
                    raise Exception("Updating cluster feature flag failed: {}".format(resp.content))
                CapellaUtils.log.info(
                    f"Updated cluster feature flag {ff} for cluster {cluster_id}")
                return
            CapellaUtils.log.critical(
                f"Creating cluster feature flag {ff} failed: {resp.status_code}")
            raise Exception("Creating cluster feature flag failed: {}".format(resp.content))
        CapellaUtils.log.info(f"Set cluster feature flag {ff}={value} for cluster {cluster_id}")

    @staticmethod
    def create_tenant_feature_flag(pod, tenant, ff, value):
        capella_api = CapellaAPI(pod.url_public,
                                 tenant.api_secret_key,
                                 tenant.api_access_key,
                                 tenant.user,
                                 tenant.pwd,
                                 pod.TOKEN)
        resp = capella_api.create_tenant_feature_flag(tenant.id, ff, {"value": value})
        if resp.status_code not in [200, 201, 204]:
            try:
                error = json.loads(resp.content)
            except Exception:
                error = {}
            if error.get("errorType") == "FeatureFlagAlreadyExists":
                CapellaUtils.log.info(
                    f"Tenant feature flag {ff} already exists for tenant {tenant.id}, updating")
                resp = capella_api.update_tenant_feature_flag(
                    tenant.id, ff, {"value": value})
                if resp.status_code not in [200, 204]:
                    CapellaUtils.log.critical(
                        f"Updating tenant feature flag {ff} failed: {resp.status_code}")
                    raise Exception("Updating tenant feature flag failed: {}".format(resp.content))
                CapellaUtils.log.info(
                    f"Updated tenant feature flag {ff}={value} for tenant {tenant.id}")
                return
            CapellaUtils.log.critical(
                f"Creating tenant feature flag {ff} failed: {resp.status_code}")
            raise Exception("Creating tenant feature flag failed: {}".format(resp.content))
        CapellaUtils.log.info(f"Set tenant feature flag {ff}={value} for tenant {tenant.id}")

    # ---------------------------------------------------------------------------
    # Cloud snapshot backup methods (v2 internal API)
    # ---------------------------------------------------------------------------

    @staticmethod
    def create_cloud_snapshot_backup(pod, tenant, project_id, cluster_id,
                                     retention=None, regions_to_copy=None):
        capella_api = CapellaAPI(pod.url_public, tenant.api_secret_key, tenant.api_access_key,
                                 tenant.user, tenant.pwd)
        resp = capella_api.create_cloud_snapshot_backup(
            tenant.id, project_id, cluster_id,
            retention=retention, regions_to_copy=regions_to_copy)
        if resp.status_code == 202:
            return resp.json()
        CapellaUtils.log.error(
            "Failed to create cloud snapshot backup for cluster {}, "
            "status: {}".format(cluster_id, resp.status_code))
        return None

    @staticmethod
    def list_cloud_snapshot_backups(pod, tenant, project_id, cluster_id):
        capella_api = CapellaAPI(pod.url_public, tenant.api_secret_key, tenant.api_access_key,
                                 tenant.user, tenant.pwd)
        page = 1
        backups = []
        while True:
            resp = capella_api.list_cloud_snapshot_backups(
                tenant.id, project_id, cluster_id, page=page)
            if resp.status_code == 200:
                info = resp.json()
                backups.extend(info.get("data", []))
                pages = info.get("cursor", {}).get("pages", {})
                if pages.get("last", page) > page:
                    page += 1
                else:
                    break
            else:
                break
        return backups

    @staticmethod
    def get_cloud_snapshot_backup_info(pod, tenant, project_id, cluster_id,
                                       backup_id):
        backups = CapellaUtils.list_cloud_snapshot_backups(
            pod=pod, tenant=tenant, project_id=project_id,
            cluster_id=cluster_id)
        for backup in backups:
            if backup.get("data", {}).get("id") == backup_id:
                return backup.get("data")
        return None

    @staticmethod
    def destroy_cloud_snapshot_backups(pod, tenant, project_id, cluster_id):
        capella_api = CapellaAPI(pod.url_public, tenant.api_secret_key, tenant.api_access_key,
                                 tenant.user, tenant.pwd)
        resp = capella_api.destroy_cloud_snapshot_backups(tenant.id, project_id, cluster_id)
        if resp.status_code == 202:
            return True
        CapellaUtils.log.error(
            "Failed to destroy cloud snapshot backups for cluster {}, "
            "status: {}".format(cluster_id, resp.status_code))
        return False

    @staticmethod
    def edit_cloud_snapshot_backup_retention(pod, tenant, project_id, cluster_id,
                                             backup_id, retention):
        capella_api = CapellaAPI(pod.url_public, tenant.api_secret_key, tenant.api_access_key,
                                 tenant.user, tenant.pwd)
        resp = capella_api.edit_cloud_snapshot_backup_retention(
            tenant.id, project_id, cluster_id, backup_id, retention)
        if resp.status_code == 204:
            return True
        CapellaUtils.log.error(
            "Failed to edit retention for cloud snapshot backup {}, "
            "status: {}".format(backup_id, resp.status_code))
        return False

    @staticmethod
    def delete_cloud_snapshot_backup(pod, tenant, project_id, cluster_id,
                                     backup_id):
        capella_api = CapellaAPI(pod.url_public, tenant.api_secret_key, tenant.api_access_key,
                                 tenant.user, tenant.pwd)
        resp = capella_api.delete_cloud_snapshot_backup(
            tenant.id, project_id, cluster_id, backup_id)
        if resp.status_code == 202:
            return True
        CapellaUtils.log.error(
            "Failed to delete cloud snapshot backup {}, "
            "status: {}".format(backup_id, resp.status_code))
        return False

    @staticmethod
    def restore_cloud_snapshot_backup(pod, tenant, project_id, cluster_id,
                                      backup_id,
                                      cross_region_restore_preference=None):
        capella_api = CapellaAPI(pod.url_public, tenant.api_secret_key, tenant.api_access_key,
                                 tenant.user, tenant.pwd)
        resp = capella_api.restore_cloud_snapshot_backup(
            tenant.id, project_id, cluster_id, backup_id,
            cross_region_restore_preference=cross_region_restore_preference)
        if resp.status_code == 202:
            return resp.json()
        CapellaUtils.log.error(
            "Failed to restore cloud snapshot backup {}, "
            "status: {}".format(backup_id, resp.status_code))
        return None

    @staticmethod
    def list_cloud_snapshot_restores(pod, tenant, project_id, cluster_id):
        capella_api = CapellaAPI(pod.url_public, tenant.api_secret_key, tenant.api_access_key,
                                 tenant.user, tenant.pwd)
        page = 1
        restores = []
        while True:
            resp = capella_api.list_cloud_snapshot_restores(
                tenant.id, project_id, cluster_id, page=page)
            if resp.status_code == 200:
                info = resp.json()
                restores.extend(info.get("data", []))
                pages = info.get("cursor", {}).get("pages", {})
                if pages.get("last", page) > page:
                    page += 1
                else:
                    break
            else:
                break
        return restores

    @staticmethod
    def list_cloud_snapshot_regions(pod, tenant, project_id, cluster_id):
        capella_api = CapellaAPI(pod.url_public, tenant.api_secret_key, tenant.api_access_key,
                                 tenant.user, tenant.pwd)
        resp = capella_api.list_cloud_snapshot_regions(tenant.id, project_id, cluster_id)
        if resp.status_code == 200:
            return resp.json()
        CapellaUtils.log.error(
            "Failed to list cloud snapshot regions for cluster {}, "
            "status: {}".format(cluster_id, resp.status_code))
        return None

    @staticmethod
    def create_v4_api_key(pod, tenant, name_prefix="fusion"):
        """
        Mint a v4 organization API key and return its bearer token, for use
        with clone_cloud_snapshot_backup() (the only v4 call in this file).

        v4 calls in the capellaAPI submodule require a bearer token --
        APIAuth (lib/capellaAPI/capella/lib/APIAuth.py) unconditionally
        signs any URL containing "v4" as 'Bearer <token>', never falling
        back to HMAC secret/access signing the way v2/v3 calls in this file
        do. tenant.api_secret_key/api_access_key (used everywhere else here)
        are therefore not usable for v4 calls at all.

        Two-step mint, matching the working pattern in
        pytests/Capella/RestAPIv4/api_base.py (create_v2_control_plane_api_key
        + update_auth_with_api_token):
          1. create_control_plane_api_key() -- a v2 internal endpoint,
             authenticated via the existing username/password session
             (do_internal_request), so no bearer token is needed yet.
          2. Use that key's token as a bearer to call
             org_ops_apis.create_api_key() -- itself a v4 endpoint -- to mint
             a real v4 organizationOwner API key.

        Returns (v2_key_id, v4_key_id, v4_bearer_token); the two ids are for
        delete_v4_api_key() cleanup. Returns (v2_key_id_or_None, None, None)
        on failure at either step.
        """
        capella_api_v4 = CapellaAPIv4(pod.url_public, tenant.api_secret_key,
                                      tenant.api_access_key, tenant.user,
                                      tenant.pwd, "")
        resp = capella_api_v4.create_control_plane_api_key(
            tenant.id, "{}-v2-{}".format(name_prefix, uuid.uuid4().hex[:6]))
        if resp.status_code != 201:
            CapellaUtils.log.error(
                "Failed to create v2 control-plane API key: {}".format(
                    resp.content))
            return None, None, None
        v2_key = resp.json()
        capella_api_v4.org_ops_apis.bearer_token = v2_key["token"]

        resp = capella_api_v4.org_ops_apis.create_api_key(
            organizationId=tenant.id,
            name="{}-v4-{}".format(name_prefix, uuid.uuid4().hex[:6]),
            organizationRoles=["organizationOwner"],
            description="Bootstrap key for fusion secondary-cluster clone")
        if resp.status_code != 201:
            CapellaUtils.log.error(
                "Failed to create v4 API key: {}".format(resp.content))
            return v2_key["id"], None, None
        v4_key = resp.json()
        return v2_key["id"], v4_key["id"], v4_key["token"]

    @staticmethod
    def delete_v4_api_key(pod, tenant, v2_key_id, v4_key_id, v4_bearer_token):
        """Tear down the API keys minted by create_v4_api_key()."""
        capella_api_v4 = CapellaAPIv4(pod.url_public, tenant.api_secret_key,
                                      tenant.api_access_key, tenant.user,
                                      tenant.pwd, "")
        if v4_key_id:
            capella_api_v4.org_ops_apis.bearer_token = v4_bearer_token
            resp = capella_api_v4.org_ops_apis.delete_api_key(tenant.id, v4_key_id)
            if resp.status_code != 204:
                CapellaUtils.log.error(
                    "Failed to delete v4 API key {}: {}".format(
                        v4_key_id, resp.content))
        if v2_key_id:
            resp = capella_api_v4.delete_control_plane_api_key(tenant.id, v2_key_id)
            if resp.status_code != 204:
                CapellaUtils.log.error(
                    "Failed to delete v2 control-plane API key {}: {}".format(
                        v2_key_id, resp.content))

    @staticmethod
    def clone_cloud_snapshot_backup(pod, tenant, project_id, backup_id, name,
                                    region, bearer_token, description="",
                                    plan="enterprise", cidr=None,
                                    single_az=True, zones=None,
                                    provider="aws", timeout=1800):
        """
        Clone a cloud snapshot backup into a brand-new cluster (v4 public
        clusterrecovery API) -- this both provisions the destination cluster
        AND restores the backup's data onto it in a single call.

        Uses v4, not v2: v2's clone response only returns a restoreId, never
        a clusterId, and (confirmed against couchbase-cloud's actual
        CreateClone implementation, internal/backup/provisioned/cluster/
        service/service.go) the resulting restore.Record's ClusterID is set
        to the NEW clone cluster's id (cluster creation is synchronous
        inside CreateClone), never the source/primary cluster's id. Since
        ListRestores filters strictly by that ClusterID, there is no way to
        discover the new cluster's id via v2's list-restores endpoint scoped
        by primary -- that would require already knowing the new cluster's
        id to query for it. v4's clone response
        (oapi.CreateCloudSnapshotCloneResponse) returns clusterId directly
        alongside restoreId, avoiding that lookup entirely.

        *bearer_token* must come from create_v4_api_key() -- v4 calls need a
        real bearer token, not tenant.api_secret_key/api_access_key (see
        create_v4_api_key()'s docstring for why).

        Uses ClusterOperationsAPIs (aliased ClusterOpsAPIv4 in this file),
        the submodule class whose clone_cloud_snapshot_backup() already
        builds the correct nested cloudProvider/availability/support payload
        for this v4 endpoint (verified against
        cmd/cp-open-api/specs/schemas/clusterrecovery/
        CreateCloudSnapshotCloneRequest.yaml) -- unlike the v2-shaped
        CapellaAPI.py wrapper, this one was already correct, just unused.

        CIDR collisions are retried with a fresh CIDR the same way
        create_cluster() does, since the CIDR pool is shared org-wide and
        another concurrent deployment can grab it first.
        """
        cluster_ops = ClusterOpsAPIv4(pod.url_public, tenant.api_secret_key,
                                      tenant.api_access_key, bearer_token)
        subnet = cidr if cidr is not None else CapellaUtils.get_next_cidr() + "/20"
        end_time = time.time() + timeout
        while time.time() < end_time:
            cloud_provider = {"type": provider, "region": region, "cidr": subnet}
            availability = {"type": "single" if single_az else "multi"}
            support = {"plan": plan}
            CapellaUtils.log.info(
                "Cloning cloud snapshot backup {} with cidr: {}".format(
                    backup_id, subnet))
            resp = cluster_ops.clone_cloud_snapshot_backup(
                tenant.id, project_id, backup_id, name=name,
                cloudProvider=cloud_provider, availability=availability,
                support=support, description=description, zones=zones)
            if resp.status_code == 202:
                return resp.json()
            if resp.status_code == 422:
                content = resp.content.decode("utf-8")
                if "CIDR" in content:
                    CapellaUtils.log.warning(
                        "CIDR {} not unique, retrying with a new one: {}"
                        .format(subnet, content))
                    subnet = CapellaUtils.get_next_cidr() + "/20"
                    continue
            CapellaUtils.log.error(
                "Failed to clone cloud snapshot backup {}, "
                "status: {}, content: {}".format(
                    backup_id, resp.status_code, resp.content))
            return None
        CapellaUtils.log.error(
            "Failed to clone cloud snapshot backup {}: could not find a "
            "unique CIDR within {}s".format(backup_id, timeout))
        return None

    @staticmethod
    def upsert_cloud_snapshot_backup_schedule(pod, tenant, project_id,
                                              cluster_id, interval, retention,
                                              start_time, copy_to_regions=None):
        capella_api = CapellaAPI(pod.url_public, tenant.api_secret_key, tenant.api_access_key,
                                 tenant.user, tenant.pwd)
        resp = capella_api.upsert_cloud_snapshot_backup_schedule(
            tenant.id, project_id, cluster_id,
            interval=interval, retention=retention, start_time=start_time,
            copy_to_regions=copy_to_regions)
        if resp.status_code == 204:
            return True
        CapellaUtils.log.error(
            "Failed to upsert cloud snapshot backup schedule for cluster {}, "
            "status: {}".format(cluster_id, resp.status_code))
        return False

    @staticmethod
    def get_cloud_snapshot_backup_schedule(pod, tenant, project_id, cluster_id):
        capella_api = CapellaAPI(pod.url_public, tenant.api_secret_key, tenant.api_access_key,
                                 tenant.user, tenant.pwd)
        resp = capella_api.get_cloud_snapshot_backup_schedule(
            tenant.id, project_id, cluster_id)
        if resp.status_code == 200:
            return resp.json()
        if resp.status_code == 204:
            return None
        CapellaUtils.log.error(
            "Failed to get cloud snapshot backup schedule for cluster {}, "
            "status: {}".format(cluster_id, resp.status_code))
        return None

    @staticmethod
    def delete_cloud_snapshot_backup_schedule(pod, tenant, project_id,
                                              cluster_id):
        capella_api = CapellaAPI(pod.url_public, tenant.api_secret_key, tenant.api_access_key,
                                 tenant.user, tenant.pwd)
        resp = capella_api.delete_cloud_snapshot_backup_schedule(
            tenant.id, project_id, cluster_id)
        if resp.status_code == 204:
            return True
        CapellaUtils.log.error(
            "Failed to delete cloud snapshot backup schedule for cluster {}, "
            "status: {}".format(cluster_id, resp.status_code))
        return False

    @staticmethod
    def list_project_level_cloud_snapshot_backups(pod, tenant, project_id):
        capella_api = CapellaAPI(pod.url_public, tenant.api_secret_key, tenant.api_access_key,
                                 tenant.user, tenant.pwd)
        page = 1
        backups = []
        while True:
            resp = capella_api.list_project_level_cloud_snapshot_backups(
                tenant.id, project_id, page=page)
            if resp.status_code == 200:
                info = resp.json()
                backups.extend(info.get("data", []))
                pages = info.get("cursor", {}).get("pages", {})
                if pages.get("last", page) > page:
                    page += 1
                else:
                    break
            else:
                break
        return backups

    @staticmethod
    def wait_for_cloud_snapshot_backup_to_complete(pod, tenant, project_id,
                                                   cluster_id, backup_id,
                                                   timeout=3600):
        start_time = time.time()
        backup_state = None
        not_found_count = 0
        while backup_state != "complete" and time.time() < start_time + timeout:
            backup_info = CapellaUtils.get_cloud_snapshot_backup_info(
                pod=pod, tenant=tenant, project_id=project_id,
                cluster_id=cluster_id, backup_id=backup_id)
            if not backup_info:
                CapellaUtils.log.error(
                    "Cloud snapshot backup {} not found".format(backup_id))
                not_found_count += 1
                if not_found_count > 30:
                    raise Exception(
                        "Cloud snapshot backup {} not found after 10 "
                        "retries".format(backup_id))
                time.sleep(60)
                continue
            backup_state = backup_info.get("progress", {}).get("status")
            CapellaUtils.log.info(
                "Waiting for cloud snapshot backup to complete, current "
                "state: {}".format(backup_state))
            time.sleep(60)
        if backup_state != "complete":
            CapellaUtils.log.error(
                "Cloud snapshot backup {} did not complete within {} "
                "seconds".format(backup_id, timeout))
            return False
        CapellaUtils.log.info(
            "Cloud snapshot backup {} completed in {} seconds".format(
                backup_id, time.time() - start_time))
        return True

    @staticmethod
    def wait_for_cloud_snapshot_restore_to_complete(pod, tenant, project_id,
                                                    cluster_id, restore_id,
                                                    timeout=3600):
        # Restore status values (couchbase-cloud:
        # internal/backup/provisioned/cluster/restore/status.go):
        # "queued", "processing", "complete", "failed". "failed" is terminal
        # -- fail fast on it instead of polling the full timeout for a
        # restore that has already given up retrying.
        start_time = time.time()
        restore_state = None
        while time.time() < start_time + timeout:
            restores = CapellaUtils.list_cloud_snapshot_restores(
                pod=pod, tenant=tenant, project_id=project_id,
                cluster_id=cluster_id)
            restore_info = next(
                (r.get("data") for r in restores
                 if r.get("data", {}).get("id") == restore_id),
                None)
            if not restore_info:
                CapellaUtils.log.error(
                    "Cloud snapshot restore {} not found".format(restore_id))
                time.sleep(60)
                continue
            restore_state = restore_info.get("status")
            CapellaUtils.log.info(
                "Waiting for cloud snapshot restore to complete, current "
                "state: {}".format(restore_state))
            if restore_state == "complete":
                CapellaUtils.log.info(
                    "Cloud snapshot restore {} completed in {} seconds".format(
                        restore_id, time.time() - start_time))
                return True
            if restore_state == "failed":
                CapellaUtils.log.error(
                    "Cloud snapshot restore {} failed after {} seconds".format(
                        restore_id, time.time() - start_time))
                return False
            time.sleep(60)
        CapellaUtils.log.error(
            "Cloud snapshot restore {} did not complete within {} seconds "
            "(last state: {})".format(restore_id, timeout, restore_state))
        return False

