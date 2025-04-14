from datetime import timedelta
from global_vars import logger
from couchbase.options import AnalyticsOptions
from couchbase.analytics import AnalyticsScanConsistency, AnalyticsMetrics
from couchbase.management.logic.analytics_logic import (CouchbaseRemoteAnalyticsLink, S3ExternalAnalyticsLink,
                                                        AnalyticsEncryptionLevel, CouchbaseAnalyticsEncryptionSettings,
                                                        AnalyticsLinkType)
from couchbase.management.analytics import AnalyticsIndexManager
from couchbase.management.options import CreateLinkAnalyticsOptions, GetLinksAnalyticsOptions
from couchbase.logic.analytics import AnalyticsStatus
from couchbase.exceptions import (CouchbaseException, InvalidArgumentException, AnalyticsLinkExistsException,
                                  AuthenticationException, DataverseNotFoundException)


class CBASHelper(object):

    def __init__(self, cluster):
        self.cluster = cluster
        self.sdk_client_pool = cluster.sdk_client_pool
        self.log = logger.get("infra")

    def execute_statement_on_cbas(
            self, statement, mode, pretty=True, timeout=300,
            client_context_id=None, username=None, password=None,
            analytics_timeout=300, time_out_unit="s",
            scan_consistency=None, scan_wait=None, max_warning=0):

        client = self.sdk_client_pool.get_cluster_client(self.cluster)

        options = AnalyticsOptions()
        options.readonly = False

        if scan_consistency and scan_consistency != "not_bounded":
            options.scanConsistency = AnalyticsScanConsistency.REQUEST_PLUS
        else:
            options.scanConsistency = AnalyticsScanConsistency.NOT_BOUNDED

        if client_context_id:
            options.clientContextId = client_context_id

        if scan_wait:
            options.scanWait = timedelta(seconds=scan_wait)

        if mode:
            options.raw = ("mode", mode)

        options.raw = ("pretty", pretty)
        options.timeout(timedelta(seconds=timeout))

        output = {}
        try:
            result = client.cluster.analytics_query(statement, options)
            output["results"] = [row for row in result.rows()]
            output["status"] = result.metadata().status().name
            output["metrics"] = result.metadata().metrics()
            output["errors"] = result.metadata().errors()
        except CouchbaseException as ex:
            output["errors"] = self.parse_error(ex)
            output["status"] = AnalyticsStatus.FATAL.name
        except Exception as err:
            self.log.error(str(err))
            output["errors"]["msg"] = str(err)
            output["status"] = AnalyticsStatus.FATAL.name

        finally:
            self.sdk_client_pool.release_cluster_client(self.cluster, client)
            if output['status'] == AnalyticsStatus.FATAL.name:
                self.log.error(str(output))
                msg = output['errors']['msg']
                if "Job requirement" in msg and "exceeds capacity" in msg:
                    raise Exception("Capacity cannot meet job requirement")
            elif output['status'] == AnalyticsStatus.SUCCESS.name:
                output["errors"] = None
            else:
                raise Exception("Analytics Service API failed")

        self.generate_response_object(output)
        return output

    def generate_response_object(self, output):
        for key, value in output.items():
            if key == "metrics" and isinstance(value, AnalyticsMetrics):
                output["metrics"] = value.__dict__["_raw"]
            elif key == "status":
                output["status"] = value.lower()

    def parse_error(self, error):
        errors = {"msg": "", "code": ""}
        try:
            errors["code"] = error.context.first_error_code
            errors["msg"] = error.context.first_error_message
        except:
            try:
                errors["msg"] = error.context.first_error_message
            except:
                errors["msg"] = error
        return errors

    def create_remote_link_obj(self, link_properties):
        try:
            _hostname = link_properties["hostname"]
            _username = link_properties["username"]
            _password = link_properties["password"]
            encryption = AnalyticsEncryptionLevel.FULL \
                if link_properties["encryption"] == "full" \
                else AnalyticsEncryptionLevel.NONE
            _encryption = CouchbaseAnalyticsEncryptionSettings(encryption_level=encryption,
                                                                               certificate=link_properties[
                                                                                   "certificate"])
            remote_link_obj = CouchbaseRemoteAnalyticsLink(link_properties["dataverse"],
                                                           link_properties["name"],
                                                           _hostname, _encryption,
                                                           _username, _password,)
            self.log.debug("Remote Link SDK object - {0}".format(remote_link_obj.__dict__))
            return remote_link_obj
        except Exception as err:
            self.log.error(err)
            return None

    def create_s3_link_obj(self, link_properties):
        try:
            s3_link_obj = S3ExternalAnalyticsLink(link_properties["dataverse"], link_properties["name"])
            s3_link_obj._access_key_id = link_properties["accessKeyId"]
            s3_link_obj._secret_access_key = link_properties["secretAccessKey"]
            s3_link_obj._region = link_properties["region"]
            s3_link_obj._session_token = link_properties.get("sessionToken", None)
            s3_link_obj._serviceEndpoint = link_properties.get("serviceEndpoint", None)
            self.log.debug("S3 SDK object = {0}".format(s3_link_obj.__dict__))
            return s3_link_obj
        except Exception as err:
            self.log.error(str(err))
            return None

    def create_link(self, link_properties={}, timeout=300):
        status = False
        errors = []
        content = None

        try:
            if link_properties.get("type") == "s3":
                link_obj = self.create_s3_link_obj(link_properties)
                if not link_obj:
                    return status, content, [{"msg": "S3ExternalAnalyticsLink object creation failed", "code": ""}]
            else:
                link_obj = self.create_remote_link_obj(link_properties)
                if not link_obj:
                    return status, content, [{"msg": "RemoteLinkObject object creation failed", "code": ""}]

            client = self.sdk_client_pool.get_cluster_client(self.cluster)
            manager = AnalyticsIndexManager(client.cluster)

            link_options = CreateLinkAnalyticsOptions(timeout=timedelta(seconds=timeout))
            manager.create_link(link_obj, link_options)
            status = True
        except AuthenticationException as err:
            self.log.error(str(err))
            errors.append({"msg": "Some link arguments are invalid", "code": ""})
        except InvalidArgumentException as err:
            self.log.error(str(err))
            errors.append({"msg": "Some link arguments are invalid", "code": ""})
        except AnalyticsLinkExistsException as err:
            self.log.error(str(err))
            errors.append({"msg": "Link {0}.{1} already exists".format(link_properties.get("dataverse"),
                                                                       link_properties.get("name")), "code": ""})
        except CouchbaseException as err:
            errors.append(self.parse_error(err))
        except Exception as err:
            errors.append({"msg": str(err), "code": ""})
        finally:
            self.sdk_client_pool.release_cluster_client(self.cluster, client)
            return status, content, errors

    def get_link_info(self, dataverse_name=None, link_name=None, link_type=None):
        status = False
        errors = []
        content = []

        client = self.sdk_client_pool.get_cluster_client(self.cluster)
        manager = AnalyticsIndexManager(client.cluster)

        try:
            get_link_options = GetLinksAnalyticsOptions(dataverse_name=dataverse_name, name=link_name,
                                                        link_type=AnalyticsLinkType(link_type))
            result = manager.get_links(get_link_options)
            for link in result:
                if isinstance(link, (S3ExternalAnalyticsLink, CouchbaseRemoteAnalyticsLink)):
                    content.append(link.__dict__)
            status = True
        except DataverseNotFoundException as err:
            self.log.error(str(err))
            errors.append({"msg": "Dataverse {0} does not exist".format(dataverse_name), "code": ""})
        except CouchbaseException as err:
            errors.append(self.parse_error(err))
            status = False
        except Exception as err:
            self.log.error(str(err))
        finally:
            self.sdk_client_pool.release_cluster_client(self.cluster, client)
            return status, content, errors
