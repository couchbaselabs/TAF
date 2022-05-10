import base64
import datetime
import time
import hashlib
import hmac
import json
import httplib2
from global_vars import logger
http = httplib2.Http(timeout=600, disable_ssl_certificate_validation=True)


class Pod:
    def __init__(self, url, url_public):
        self.url = url
        self.url_public = url_public


class Tenant:
    def __init__(self, id, user, pwd):
        self.id = id
        self.user = user
        self.pwd = pwd
        self.api_secret_key = None
        self.api_access_key = None
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
            basic = base64.encodestring('{}:{}'.format(tenant.user, tenant.pwd)
                                        .encode('utf-8')).decode('utf-8')
            _, content = http.request(
                "{}/sessions".format(pod.url), method="POST",
                headers={"Authorization": "Basic %s" % basic})
            CapellaUtils.jwt = json.loads(content).get("jwt")
        cbc_api_request_headers = {
           'Authorization': 'Bearer %s' % CapellaUtils.jwt,
           'Content-Type': 'application/json'
        }
        return cbc_api_request_headers

    @staticmethod
    def get_authorization_v3(tenant, method, endpoint):
        # Epoch time in milliseconds
        cbc_api_now = int(datetime.datetime.now().timestamp() * 1000)

        # Form the message string for the Hmac hash
        cbc_api_message = method + '\n' + endpoint + '\n' + str(cbc_api_now)

        # Calculate the hmac hash value with secret key and message
        cbc_api_signature = base64.b64encode(
            hmac.new(bytes(tenant.api_secret_key, 'utf-8'),
                     bytes(cbc_api_message, 'utf-8'),
                     digestmod=hashlib.sha256).digest())

        # Values for the header
        cbc_api_request_headers = {
           'Authorization': 'Bearer ' + tenant.api_access_key + ':' + cbc_api_signature.decode(),
           'Couchbase-Timestamp': str(cbc_api_now),
        }
        return cbc_api_request_headers

    @staticmethod
    def create_project(pod, tenant, name):
        project_details = {"name": name, "tenantId": tenant.id}

        uri = '{}/v2/organizations/{}/projects'.format(pod.url, tenant.id)
        capella_header = CapellaUtils.get_authorization_internal(pod, tenant)
        _, content = http.request(uri, method="POST",
                                  body=json.dumps(project_details),
                                  headers=capella_header)
        project_id = json.loads(content).get("id")
        tenant.project_id = project_id
        CapellaUtils.log.info("Project ID: {}".format(project_id))

    @staticmethod
    def delete_project(pod, tenant):
        header = CapellaUtils.get_authorization_internal(pod, tenant)
        uri = '{}/v2/organizations/{}/projects/{}'.format(pod.url, tenant.id,
                                                          tenant.project_id)
        _, _ = http.request(uri, method="DELETE", body='',
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
    def create_cluster(pod, tenant, cluster_details):
        header = CapellaUtils.get_authorization_internal(pod, tenant)
        while True:
            subnet = CapellaUtils.get_next_cidr() + "/20"
            CapellaUtils.log.info("Trying with cidr: {}".format(subnet))
            cluster_details.update({"cidr": subnet,
                                    "projectId": tenant.project_id})
            print(cluster_details)
            uri = '{}/v2/organizations/{}/clusters'.format(pod.url, tenant.id)
            response, content = http.request(uri, method="POST",
                                             body=json.dumps(cluster_details),
                                             headers=header)
            CapellaUtils.log.info(content)
            if 200 <= int(response.get("status")) < 300:
                CapellaUtils.log.info("Cluster created successfully!")
                break

        cluster_id = json.loads(content).get("id")
        CapellaUtils.log.info("Cluster created with cluster ID: {}".format(cluster_id))
        CapellaUtils.wait_until_done(pod, tenant, cluster_id,
                                     "Creating Cluster {}".format(cluster_details.get("name")))
        cluster_srv = CapellaUtils.get_cluster_srv(pod, tenant, cluster_id)
        CapellaUtils.add_allowed_ip(pod, tenant, cluster_id)
        servers = CapellaUtils.get_nodes(pod, tenant, cluster_id)
        return cluster_id, cluster_srv, servers

    @staticmethod
    def wait_until_done(pod, tenant, cluster_id, msg="", prnt=False):
        while True:
            try:
                content = CapellaUtils.jobs(pod, tenant, cluster_id)
                state = CapellaUtils.get_cluster_state(pod, tenant, cluster_id)
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
            except Exception:
                CapellaUtils.log.info("ERROR!!!")
                break

    @staticmethod
    def destroy_cluster(pod, tenant, cluster):
        base_url_internal = '{}/v2/organizations/{}/projects/{}/clusters/{}'\
            .format(pod.url, tenant.id, tenant.project_id, cluster.id)
        header = CapellaUtils.get_authorization_internal(pod, tenant)
        _, content = http.request(base_url_internal, method="DELETE", body='',
                                  headers=header)
        time.sleep(10)

        header = CapellaUtils.get_authorization_internal(pod, tenant)
        while True:
            _, content = http.request(base_url_internal, method="GET",
                                      body='', headers=header)
            content = json.loads(content)
            if content.get("data"):
                CapellaUtils.log.info("Cluster status {}: {}"
                                      .format(cluster.details.get("name"),
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
        response, _ = http.request(uri, method="POST",
                                   body=json.dumps(default),
                                   headers=header)
        if 200 <= int(response.get("status")) < 300:
            CapellaUtils.log.info("Bucket create successfully!")

    @staticmethod
    def get_bucket_id(pod, tenant, cluster, name):
        base_url_internal = '{}/v2/organizations/{}/projects/{}/clusters/{}'\
            .format(pod.url, tenant.id, tenant.project_id, cluster.id)
        uri = '{}/buckets'.format(base_url_internal)
        header = CapellaUtils.get_authorization_internal(pod, tenant)
        _, content = http.request(uri, method="GET", body='', headers=header)
        content = json.loads(content)
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
            response, content = http.request(uri, method="POST",
                                             headers=header)
            if int(response.get("status")) >= 200 and int(response.get("status")) < 300 :
                CapellaUtils.log.info("Bucket deleted successfully!")
            else:
                CapellaUtils.log.info(content)
        else:
            CapellaUtils.log.info("Bucket not found.")

    @staticmethod
    def delete_bucket(pod, tenant, cluster, name):
        base_url_internal = '{}/v2/organizations/{}/projects/{}/clusters/{}'\
            .format(pod.url, tenant.id, tenant.project_id, cluster.id)
        uri = '{}/buckets'.format(base_url_internal)
        bucket_id = CapellaUtils.get_bucket_id(tenant, cluster, name)
        if bucket_id:
            uri = uri + "/" + bucket_id
            header = CapellaUtils.get_authorization_internal(pod, tenant)
            response, content = http.request(uri, method="DELETE",
                                             headers=header)
            if int(response.get("status")) >= 200 and int(response.get("status")) < 300 :
                CapellaUtils.log.info("Bucket deleted successfully!")
            else:
                CapellaUtils.log.info(content)
        else:
            CapellaUtils.log.info("Bucket not found.")

    @staticmethod
    def get_all_buckets(pod, tenant, cluster):
        uri = "{}/v2/organizations/{}/projects/{}/clusters/{}/buckets" \
            .format(pod.url, tenant.id, tenant.project_id, cluster.id)
        header = CapellaUtils.get_authorization_internal(pod, tenant)
        _, content = http.request(uri, method="GET", body='', headers=header)
        return json.loads(content)["buckets"]["data"]

    @staticmethod
    def update_bucket_settings(pod, tenant, cluster, bucket_id, bucket_params):
        uri = "{}/v2/organizations/{}/projects/{}/clusters/{}/buckets/{}" \
            .format(pod.url, tenant.id, tenant.project_id,
                    cluster.id, bucket_id)
        header = CapellaUtils.get_authorization_internal(pod, tenant)
        response, content = http.request(uri, method="PUT", headers=header,
                                         body=json.dumps(bucket_params))
        code = int(response.get("status"))
        if 200 > code or code >= 300:
            CapellaUtils.log.critical("Bucket update failed: %s" % content)
        return response

    @staticmethod
    def scale(pod, tenant, cluster, scale_params):
        base_url_internal = '{}/v2/organizations/{}/projects/{}/clusters/{}'\
            .format(pod.url, tenant.id, tenant.project_id, cluster.id)
        uri = '{}/specs'.format(base_url_internal)
        scale_params = json.dumps(scale_params)
        print(scale_params, uri)
        header = CapellaUtils.get_authorization_internal(pod, tenant)
        response, content = http.request(uri, method="POST", body=scale_params,
                                         headers=header)
        return response, content

    @staticmethod
    def jobs(pod, tenant, cluster_id):
        base_url_internal = '{}/v2/organizations/{}/projects/{}/clusters/{}'\
            .format(pod.url, tenant.id, tenant.project_id, cluster_id)
        uri = '{}/jobs'.format(base_url_internal)
        header = CapellaUtils.get_authorization_internal(pod, tenant)
        _, content = http.request(uri, method="GET", body='',
                                  headers=header)
        return json.loads(content)

    @staticmethod
    def get_cluster_details(pod, cluster_id):
        endpoint = '/v3/clusters/{}'.format(cluster_id)
        uri = pod.url_public + endpoint
        header = CapellaUtils.get_authorization_v3("GET", endpoint)
        _, content = http.request(uri, method="GET", body='',
                                  headers=header)
        return json.loads(content)

    @staticmethod
    def get_cluster_info(pod, tenant, cluster_id):
        uri = '{}/v2/organizations/{}/projects/{}/clusters/{}'\
            .format(pod.url, tenant.id, tenant.project_id, cluster_id)
        header = CapellaUtils.get_authorization_internal(pod, tenant)
        _, content = http.request(uri, method="GET", body='',
                                  headers=header)
        return json.loads(content)

    @staticmethod
    def get_cluster_state(pod, tenant, cluster_id):
        content = CapellaUtils.get_cluster_info(pod, tenant, cluster_id)
        return content.get("data").get("status").get("state")

    @staticmethod
    def get_cluster_srv(pod, tenant, cluster_id):
        content = CapellaUtils.get_cluster_info(pod, tenant, cluster_id)
        return content.get("data").get("connect").get("srv")

    @staticmethod
    def get_nodes(pod, tenant, cluster_id):
        base_url_internal = '{}/v2/organizations/{}/projects/{}/clusters/{}'\
            .format(pod.url, tenant.id, tenant.project_id, cluster_id)
        uri = '{}/nodes'.format(base_url_internal)
        header = CapellaUtils.get_authorization_internal(pod, tenant)
        _, content = http.request(uri, method="GET", body='',
                                  headers=header)
        CapellaUtils.log.info(json.loads(content))
        return [server.get("data")
                for server in json.loads(content).get("data")]

    @staticmethod
    def get_db_users(pod, tenant, cluster_id, page=1, limit=100):
        header = CapellaUtils.get_authorization_internal(pod, tenant)
        uri = '{}/v2/organizations/{}/projects/{}/clusters/{}' \
              .format(pod.url, tenant.id, tenant.project_id, cluster_id)
        uri = uri + '/users?page=%s&perPage=%s' % (page, limit)
        _, content = http.request(uri, method="GET", headers=header)
        return json.loads(content)

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
        _, content = http.request(uri, method="POST",
                                  body=json.dumps(body),
                                  headers=header)
        CapellaUtils.log.info(json.loads(content))
        return json.loads(content)

    @staticmethod
    def add_allowed_ip(pod, tenant, cluster_id):
        base_url_internal = '{}/v2/organizations/{}/projects/{}/clusters/{}'\
            .format(pod.url, tenant.id, tenant.project_id, cluster_id)
        _, content = http.request("https://ifconfig.me/all.json", method="GET")
        ip = json.loads(content).get("ip_addr")
        body = {"create": [{"cidr": "{}/32".format(ip), "comment": ""}]}
        uri = '{}/allowlists-bulk'.format(base_url_internal)
        header = CapellaUtils.get_authorization_internal(pod, tenant)
        _, content = http.request(uri, method="POST", body=json.dumps(body),
                                  headers=header)

    @staticmethod
    def load_sample_bucket(pod, tenant, cluster_id, bucket_name):
        header = CapellaUtils.get_authorization_internal(pod, tenant)
        uri = "{}/v2/organizations/{}/projects/{}/clusters/{}/buckets/samples"\
              .format(pod.url, tenant.id, tenant.project_id, cluster_id)
        param = {'name': bucket_name}
        _, _ = http.request(uri, method="POST",
                            body=json.dumps(param),
                            headers=header)
