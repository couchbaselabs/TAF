import base64
import time
import hashlib
import hmac
import json
from global_vars import logger
import requests
from capella.capellaAPI.CapellaAPI import CapellaAPI


def _urllib_request(api, method='GET', headers=None,
                    params='', timeout=300, verify=False):
    session = requests.Session()
    try:
        if method == "GET":
            resp = session.get(api, params=params, headers=headers,
                               timeout=timeout, verify=verify)
        elif method == "POST":
            resp = session.post(api, data=params, headers=headers,
                                timeout=timeout, verify=verify)
        elif method == "DELETE":
            resp = session.delete(api, data=params, headers=headers,
                                  timeout=timeout, verify=verify)
        elif method == "PUT":
            resp = session.put(api, data=params, headers=headers,
                               timeout=timeout, verify=verify)
        return resp
    except requests.exceptions.HTTPError as errh:
        CapellaUtils.log.error("HTTP Error {0}".format(errh))
    except requests.exceptions.ConnectionError as errc:
        CapellaUtils.log.error("Error Connecting {0}".format(errc))
    except requests.exceptions.Timeout as errt:
        CapellaUtils.log.error("Timeout Error: {0}".format(errt))
    except requests.exceptions.RequestException as err:
        CapellaUtils.log.error("Something else: {0}".format(err))


class Pod:
    def __init__(self, url, url_public):
        self.url = url
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
    jwt = None

    @staticmethod
    def get_authorization_internal(pod, tenant):
        if CapellaUtils.jwt is None:
            basic = base64.encodestring('{}:{}'.format(tenant.user, tenant.pwd)).strip("\n")
            header = {'Authorization': 'Basic %s' % basic}
            resp = _urllib_request(
                "{}/sessions".format(pod.url), method="POST",
                headers=header)
            CapellaUtils.jwt = json.loads(resp.content).get("jwt")
        cbc_api_request_headers = {
           'Authorization': 'Bearer %s' % CapellaUtils.jwt,
           'Content-Type': 'application/json'
        }
        return cbc_api_request_headers

    @staticmethod
    def get_authorization_v3(tenant, method, endpoint):
        # Epoch time in milliseconds
        cbc_api_now = int(time.time() * 1000)

        # Form the message string for the Hmac hash
        cbc_api_message = method + '\n' + endpoint + '\n' + str(cbc_api_now)

        # Calculate the hmac hash value with secret key and message
        cbc_api_signature = base64.b64encode(
            hmac.new(bytes(tenant.api_secret_key),
                     bytes(cbc_api_message),
                     digestmod=hashlib.sha256).digest())

        # Values for the header
        cbc_api_request_headers = {
           'Authorization': 'Bearer ' + tenant.api_access_key + ':' + cbc_api_signature.decode(),
           'Couchbase-Timestamp': str(cbc_api_now),
           'Content-Type': 'application/json'
        }
        return cbc_api_request_headers

    @staticmethod
    def create_project(pod, tenant, name):
        project_details = {"name": name, "tenantId": tenant.id}

        uri = '{}/v2/organizations/{}/projects'.format(pod.url, tenant.id)
        capella_header = CapellaUtils.get_authorization_internal(pod, tenant)
        resp = _urllib_request(uri, method="POST",
                               params=json.dumps(project_details),
                               headers=capella_header)
        if resp.status_code != 201:
            raise Exception("Creating capella project failed.")
        project_id = json.loads(resp.content).get("id")
        tenant.project_id = project_id
        CapellaUtils.log.info("Project ID: {}".format(project_id))

    @staticmethod
    def delete_project(pod, tenant):
        header = CapellaUtils.get_authorization_internal(pod, tenant)
        uri = '{}/v2/organizations/{}/projects/{}'.format(pod.url, tenant.id,
                                                          tenant.project_id)
        _ = _urllib_request(uri, method="DELETE", params='',
                            headers=header)
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
            cluster_details["place"]["hosted"].update({"CIDR": subnet})
            cluster_details.update({"projectId": tenant.project_id})
            capella_api = CapellaAPI(pod.url_public,
                                     tenant.api_secret_key,
                                     tenant.api_access_key)
            capella_api_resp = capella_api.create_cluster(cluster_details)

            # Check resp code , 202 is success
            if capella_api_resp.status_code == 202:
                break
            else:
                CapellaUtils.log.critical("Create capella cluster failed.")
                CapellaUtils.log.critical("Capella API returned " + str(
                    capella_api_resp.status_code))
                CapellaUtils.log.critical(capella_api_resp.json()["message"])

        cluster_id = capella_api_resp.headers['Location'].split("/")[-1]
        CapellaUtils.log.info("Cluster created with cluster ID: {}"\
                              .format(cluster_id))
        CapellaUtils.wait_until_done(pod, tenant, cluster_id,
                                     "Creating Cluster {}".format(
                                         cluster_details.get("clusterName")))
        cluster_srv = CapellaUtils.get_cluster_srv(pod, tenant, cluster_id)
        CapellaUtils.add_allowed_ip(pod, tenant, cluster_id)
        servers = CapellaUtils.get_nodes(pod, tenant, cluster_id)
        return cluster_id, cluster_srv, servers

    @staticmethod
    def wait_until_done(pod, tenant, cluster_id, msg="", prnt=False,
                        timeout=1800):
        end_time = time.time() + timeout
        while time.time() < end_time:
            content = CapellaUtils.jobs(pod, tenant, cluster_id)
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
                time.sleep(2)
            else:
                CapellaUtils.log.info("{} Ready!!!".format(msg))
                break

    @staticmethod
    def destroy_cluster(pod, tenant, cluster):
        capella_api = CapellaAPI(pod.url_public, tenant.api_secret_key, tenant.api_access_key)
        resp = capella_api.delete_cluster(cluster.id)
        if resp.status_code != 202:
            raise Exception("Deleting Capella Cluster Failed.")
        base_url_internal = '{}/v2/organizations/{}/projects/{}/clusters/{}'\
            .format(pod.url, tenant.id, tenant.project_id, cluster.id)

        time.sleep(10)
        header = CapellaUtils.get_authorization_internal(pod, tenant)
        while True:
            resp = _urllib_request(base_url_internal, method="GET",
                                   params='', headers=header)
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
                tenant.clusters.pop(cluster.id)
                break

    @staticmethod
    def create_bucket(pod, tenant, cluster, bucket_params={}):
        while True:
            state = CapellaUtils.get_cluster_state(pod, tenant, cluster.id)
            if state == "healthy":
                break
            time.sleep(1)
        base_url_internal = '{}/v2/organizations/{}/projects/{}/clusters/{}'\
            .format(pod.url, tenant.id, tenant.project_id, cluster.id)
        uri = '{}/buckets'.format(base_url_internal)
        default = {"name": "default", "bucketConflictResolution": "seqno",
                   "memoryAllocationInMb": 100, "flush": False, "replicas": 0,
                   "durabilityLevel": "none", "timeToLive": None}
        default.update(bucket_params)
        header = CapellaUtils.get_authorization_internal(pod, tenant)
        resp = _urllib_request(uri, method="POST",
                               params=json.dumps(default),
                               headers=header)
        if resp.status_code in [200, 201, 202]:
            CapellaUtils.log.info("Bucket create successfully!")

    @staticmethod
    def get_bucket_id(pod, tenant, cluster, name):
        base_url_internal = '{}/v2/organizations/{}/projects/{}/clusters/{}'\
            .format(pod.url, tenant.id, tenant.project_id, cluster.id)
        uri = '{}/buckets'.format(base_url_internal)
        header = CapellaUtils.get_authorization_internal(pod, tenant)
        resp = _urllib_request(uri, method="GET", params='', headers=header)
        content = json.loads(resp.content)
        bucket_id = None
        for bucket in content.get("buckets").get("data"):
                if bucket.get("data").get("name") == name:
                        bucket_id = bucket.get("data").get("id")
        return bucket_id

    @staticmethod
    def flush_bucket(pod, tenant, cluster, name):
        base_url_internal = '{}/v2/organizations/{}/projects/{}/clusters/{}'\
            .format(pod.url, tenant.id, tenant.project_id, cluster.id)
        uri = '{}/buckets'.format(base_url_internal)
        bucket_id = CapellaUtils.get_bucket_id(tenant, cluster, name)
        if bucket_id:
            uri = uri + "/" + bucket_id + "/flush"
            header = CapellaUtils.get_authorization_internal(pod, tenant)
            resp = _urllib_request(uri, method="POST",
                                   headers=header)
            if resp.status >= 200 and resp.status < 300:
                CapellaUtils.log.info("Bucket deleted successfully!")
            else:
                CapellaUtils.log.info(resp.content)
        else:
            CapellaUtils.log.info("Bucket not found.")

    @staticmethod
    def delete_bucket(pod, tenant, cluster, name):
        base_url_internal = '{}/v2/organizations/{}/projects/{}/clusters/{}'\
            .format(pod.url, tenant.id, tenant.project_id, cluster.id)
        uri = '{}/buckets'.format(base_url_internal)
        bucket_id = CapellaUtils.get_bucket_id(pod, tenant, cluster, name)
        if bucket_id:
            uri = uri + "/" + bucket_id
            header = CapellaUtils.get_authorization_internal(pod, tenant)
            resp = _urllib_request(uri, method="DELETE",
                                   headers=header)
            if resp.status_code == 204:
                CapellaUtils.log.info("Bucket deleted successfully!")
            else:
                CapellaUtils.log.critical(resp.content)
                raise Exception("Bucket {} cannot be deleted".format(name))
        else:
            CapellaUtils.log.info("Bucket not found.")

    @staticmethod
    def get_all_buckets(pod, tenant, cluster):
        uri = "{}/v2/organizations/{}/projects/{}/clusters/{}/buckets" \
            .format(pod.url, tenant.id, tenant.project_id, cluster.id)
        header = CapellaUtils.get_authorization_internal(pod, tenant)
        resp = _urllib_request(uri, method="GET", params='', headers=header)
        return json.loads(resp.content)["buckets"]["data"]

    @staticmethod
    def update_bucket_settings(pod, tenant, cluster, bucket_id, bucket_params):
        uri = "{}/v2/organizations/{}/projects/{}/clusters/{}/buckets/{}" \
            .format(pod.url, tenant.id, tenant.project_id,
                    cluster.id, bucket_id)
        header = CapellaUtils.get_authorization_internal(pod, tenant)
        resp = _urllib_request(uri, method="PUT", headers=header,
                               params=json.dumps(bucket_params))
        code = resp.status
        if 200 > code or code >= 300:
            CapellaUtils.log.critical("Bucket update failed: %s" % resp.content)
        return resp.status

    @staticmethod
    def scale(pod, tenant, cluster, new_config):
        capella_api = CapellaAPI(pod.url_public, tenant.api_secret_key, tenant.api_access_key)
        while True:
            resp = capella_api.update_cluster_servers(cluster.id, new_config)
            if resp.status_code != 202:
                result = json.loads(resp.content)
                if result["errorType"] == "ClusterModifySpecsInvalidState":
                    CapellaUtils.wait_until_done(pod, tenant, cluster.id,
                                                 "Wait for healthy cluster state")
            else:
                break

    @staticmethod
    def jobs(pod, tenant, cluster_id):
        base_url_internal = '{}/v2/organizations/{}/projects/{}/clusters/{}'\
            .format(pod.url, tenant.id, tenant.project_id, cluster_id)
        uri = '{}/jobs'.format(base_url_internal)
        header = CapellaUtils.get_authorization_internal(pod, tenant)
        resp = _urllib_request(uri, method="GET", params='',
                               headers=header)
        if resp.status_code != 200:
            raise Exception("Fetch capella cluster jobs failed!")
        return json.loads(resp.content)

    @staticmethod
    def get_cluster_info(pod, tenant, cluster_id):
        capella_api = CapellaAPI(pod.url_public, tenant.api_secret_key, tenant.api_access_key)
        resp = capella_api.get_cluster_info(cluster_id)
        if resp.status_code != 200:
            raise Exception("Fetch capella cluster details failed!")
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
        base_url_internal = '{}/v2/organizations/{}/projects/{}/clusters/{}'\
            .format(pod.url, tenant.id, tenant.project_id, cluster_id)
        uri = '{}/nodes'.format(base_url_internal)
        header = CapellaUtils.get_authorization_internal(pod, tenant)
        resp = _urllib_request(uri, method="GET", params='',
                               headers=header)
        if resp.status_code != 200:
            raise Exception("Fetch capella cluster nodes failed!")
        CapellaUtils.log.info(json.loads(resp.content))
        return [server.get("data")
                for server in json.loads(resp.content).get("data")]

    @staticmethod
    def get_db_users(pod, tenant, cluster_id, page=1, limit=100):
        header = CapellaUtils.get_authorization_internal(pod, tenant)
        uri = '{}/v2/organizations/{}/projects/{}/clusters/{}' \
              .format(pod.url, tenant.id, tenant.project_id, cluster_id)
        uri = uri + '/users?page=%s&perPage=%s' % (page, limit)
        resp = _urllib_request(uri, method="GET", headers=header)
        return json.loads(resp.content)

    @staticmethod
    def delete_db_user(pod, tenant, cluster_id, user_id):
        uri = "{}/v2/organizations/{}/projects/{}/clusters/{}/users/{}" \
              .format(pod.url, tenant.id, tenant.project_id, cluster_id,
                      user_id)
        print(uri)

    @staticmethod
    def create_db_user(pod, tenant, cluster_id, user, pwd):
        base_url_internal = '{}/v2/organizations/{}/projects/{}/clusters/{}'\
            .format(pod.url, tenant.id, tenant.project_id, cluster_id)
        body = {"name": user, "password": pwd,
                "permissions": {"data_reader": {}, "data_writer": {}}}
        uri = '{}/users'.format(base_url_internal)
        header = CapellaUtils.get_authorization_internal(pod, tenant)
        resp = _urllib_request(uri, method="POST",
                               params=json.dumps(body),
                               headers=header)
        if resp.status_code != 200:
            result = json.loads(resp.content)
            if result["errorType"] == "ErrDataplaneUserNameExists":
                CapellaUtils.log.warn("User is already added: %s" % result["message"])
                return
            raise Exception("Add capella cluster user failed!")
            CapellaUtils.log.critical(json.loads(resp.content))
        CapellaUtils.log.info(json.loads(resp.content))
        return json.loads(resp.content)

    @staticmethod
    def add_allowed_ip(pod, tenant, cluster_id):
        base_url_internal = '{}/v2/organizations/{}/projects/{}/clusters/{}'\
            .format(pod.url, tenant.id, tenant.project_id, cluster_id)
        resp = _urllib_request("https://ifconfig.me", method="GET")
        if resp.status_code != 200:
            raise Exception("Fetch public IP failed!")
        body = {"create": [{"cidr": "{}/32".format(resp.content),
                            "comment": ""}]}
        uri = '{}/allowlists-bulk'.format(base_url_internal)
        header = CapellaUtils.get_authorization_internal(pod, tenant)
        resp = _urllib_request(uri, method="POST", params=json.dumps(body),
                               headers=header)
        if resp.status_code != 202:
            result = json.loads(resp.content)
            if result["errorType"] == "ErrAllowListsCreateDuplicateCIDR":
                CapellaUtils.log.warn("IP is already added: %s" % result["message"])
                return
            CapellaUtils.log.critical(resp.content)
            raise Exception("Adding allowed IP failed.")

    @staticmethod
    def load_sample_bucket(pod, tenant, cluster_id, bucket_name):
        header = CapellaUtils.get_authorization_internal(pod, tenant)
        uri = "{}/v2/organizations/{}/projects/{}/clusters/{}/buckets/samples"\
              .format(pod.url, tenant.id, tenant.project_id, cluster_id)
        param = {'name': bucket_name}
        _ = _urllib_request(uri, method="POST",
                            params=json.dumps(param),
                            headers=header)
