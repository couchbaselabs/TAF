import time
import json

from constants.cloud_constants.capella_constants import AWS, Cluster
from global_vars import logger
from capellaAPI.capella.dedicated.CapellaAPI import CapellaAPI
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
                if resp.content.find("not allowed based on your activation status") != -1:
                    CapellaUtils.log.critical("Tenant is not activated yet...retrying")
                if resp.content.find("CIDR") != -1:
                    subnet = CapellaUtils.get_next_cidr() + "/20"
                else:
                    CapellaUtils.log.critical(resp.content)
                    raise Exception("Cluster deployment failed.")
            else:
                CapellaUtils.log.critical("Create capella_utils cluster failed.")
                CapellaUtils.log.critical("Capella API returned " + str(
                    resp.status_code))
                CapellaUtils.log.critical(resp.json()["message"])
            time.sleep(5)

        CapellaUtils.log.info("Cluster created with cluster ID: {}"\
                              .format(cluster_id))
        CapellaUtils.wait_until_done(pod, tenant, cluster_id,
                                     "Creating Cluster {}".format(cluster_id),
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
    def destroy_cluster(pod, tenant, cluster):
        capella_api = CapellaAPI(pod.url_public,
                                 tenant.api_secret_key,
                                 tenant.api_access_key,
                                 tenant.user,
                                 tenant.pwd)
        resp = capella_api.delete_cluster(cluster.id)
        if resp.status_code != 202:
            raise Exception("Deleting Capella Cluster Failed.")

        time.sleep(10)
        while True:
            resp = capella_api.get_cluster_internal(tenant.id,
                                                    tenant.projects[0],
                                                    cluster.id)
            content = json.loads(resp.content)
            if content.get("data"):
                CapellaUtils.log.info(
                    "Cluster status %s: %s"
                    % (cluster.id,
                       content.get("data").get("status").get("state")))
                if content.get("data").get("status").get("state") == "destroying":
                    time.sleep(5)
                    continue
            elif content.get("message") == 'Not Found.':
                CapellaUtils.log.info("Cluster is destroyed.")
                tenant.clusters.remove(cluster)
                break

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
    def create_bucket(pod, tenant, cluster, bucket_params):
        while True:
            state = CapellaUtils.get_cluster_state(
                pod, tenant, cluster.id)
            if state == "healthy":
                break
            time.sleep(1)
        capella_api = CapellaAPI(pod.url_public,
                                 tenant.api_secret_key,
                                 tenant.api_access_key,
                                 tenant.user,
                                 tenant.pwd)
        resp = capella_api.create_bucket(tenant.id,
                                         tenant.projects[0],
                                         cluster.id, bucket_params)
        if resp.status_code in [200, 201, 202]:
            CapellaUtils.log.info("Bucket {} create successfully on cluster {}!".format(
                bucket_params.get("name"), cluster.id))
        else:
            CapellaUtils.log.critical("Bucket creation failed: {}, {}".
                                      format(resp.status_code, resp.content))
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
        bucket_id = CapellaUtils.get_bucket_id(pod, cluster, name)
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
        bucket_id = CapellaUtils.get_bucket_id(pod, cluster, name)
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
            else:
                CapellaUtils.log.critical(resp.content)
                raise Exception("Bucket {} cannot be deleted".format(name))
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
                if result["errorType"] in ["ClusterModifySpecsInvalidState", "EntityNotWritable"]:
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
    def jobs(capella_api, pod, tenant, cluster_id):
        resp = capella_api.jobs(tenant.projects[0], tenant.id, cluster_id)
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
        resp = capella_api.get_nodes(tenant.id, tenant.projects[0],
                                     cluster_id)
        if resp.status_code != 200:
            CapellaUtils.log.critical("LOG A BUG: Fetch Cluster Node API returns :\
            {}".format(resp.status_code))
            print(resp.content)
            time.sleep(5)
            return CapellaUtils.get_nodes(pod, tenant, cluster_id)
        return [server.get("data")
                for server in json.loads(resp.content).get("data")]

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
    def allow_my_ip(pod, tenant, cluster_id):
        capella_api = CapellaAPI(pod.url_public,
                                 tenant.api_secret_key,
                                 tenant.api_access_key,
                                 tenant.user,
                                 tenant.pwd)
        resp = capella_api.allow_my_ip(tenant.id, tenant.projects[0],
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
        log_id = {"ticketId": log_id}
        resp = capella_api.trigger_log_collection(cluster_id,
                                                  log_id=log_id)
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
