import json

from Rest_Connection import RestConnection
from common_lib import sleep
import urllib
from cb_constants import CbServer

class GsiHelper(RestConnection):
    def __init__(self, server, logger):
        super(GsiHelper, self).__init__(server)
        self.log = logger

    def trigger_index_compaction(self, timeout=120):
        api = self.indexUrl + 'triggerCompaction'
        status, content, header = self._http_request(api, timeout=timeout)
        if not status:
            raise Exception(content)

    def set_index_settings(self, setting_json):
        api = self.indexUrl + 'settings'
        status, content, header = self._http_request(api, 'POST',
                                                     json.dumps(setting_json))
        if not status:
            raise Exception(content)
        self.log.debug("{0} set".format(setting_json))

    def set_index_settings_internal(self, setting_json):
        api = self.indexUrl + 'internal/settings'
        status, content, header = self._http_request(api, 'POST',
                                                     json.dumps(setting_json))
        if not status:
            if header['status'] == '404':
                self.log.warn(
                    "This endpoint is introduced only in 5.5.0, hence not "
                    "found. Redirecting the request to the old endpoint")
                self.set_index_settings(setting_json)
            else:
                raise Exception(content)
        self.log.debug("{0} set".format(setting_json))

    def get_index_settings(self, timeout=120):
        api = self.indexUrl + 'settings'
        status, content, header = self._http_request(api, timeout=timeout)
        if not status:
            raise Exception(content)
        return json.loads(content)

    def get_index_storage_stats(self, timeout=120):
        api = self.indexUrl + 'stats/storage'
        status, content, header = self._http_request(api, timeout=timeout)
        if not status:
            raise Exception(content)
        json_parsed = json.loads(content)
        index_storage_stats = dict()
        for index_stats in json_parsed:
            bucket = index_stats["Index"].split(":")[0]
            index_name = index_stats["Index"].split(":")[1]
            if bucket not in index_storage_stats.keys():
                index_storage_stats[bucket] = {}
            index_storage_stats[bucket][index_name] = index_stats["Stats"]
        return index_storage_stats

    def get_indexer_internal_stats(self, timeout=120):
        api = self.indexUrl + 'settings?internal=ok'
        index_map = dict()
        status, content, header = self._http_request(api, timeout=timeout)
        if status:
            json_parsed = json.loads(content)
            for key in json_parsed.keys():
                tokens = key.split(":")
                val = json_parsed[key]
                if len(tokens) == 1:
                    field = tokens[0]
                    index_map[field] = val
        return index_map

    def index_status(self):
        result = dict()
        api = self.baseUrl + "indexStatus"
        status, content, header = self._http_request(api)
        if status:
            content = json.loads(content)
            for val in content["indexes"]:
                bucket_name = val['bucket'].encode('ascii', 'ignore')
                if bucket_name not in result.keys():
                    result[bucket_name] = dict()
                index_name = val['index'].encode('ascii', 'ignore')
                result[bucket_name][index_name] = dict()
                result[bucket_name][index_name]['status'] = \
                    val['status'].encode('ascii', 'ignore')
                result[bucket_name][index_name]['progress'] = \
                    str(val['progress']).encode('ascii', 'ignore')
                result[bucket_name][index_name]['definition'] = \
                    val['definition'].encode('ascii', 'ignore')
                if len(val['hosts']) == 1:
                    result[bucket_name][index_name]['hosts'] = \
                        val['hosts'][0].encode('ascii', 'ignore')
                else:
                    result[bucket_name][index_name]['hosts'] = val['hosts']
                result[bucket_name][index_name]['id'] = val['id']
        return result


    def get_index_id_map(self, timeout=120):
        api = self.baseUrl + 'indexStatus'
        index_map = dict()
        status, content, header = self._http_request(api, timeout=timeout)
        if status:
            json_parsed = json.loads(content)
            for i_map in json_parsed["indexes"]:
                bucket_name = i_map['bucket'].encode('ascii', 'ignore')
                if bucket_name not in index_map.keys():
                    index_map[bucket_name] = {}
                index_name = i_map['index'].encode('ascii', 'ignore')
                index_map[bucket_name][index_name] = {}
                index_map[bucket_name][index_name]['id'] = i_map['id']
        return index_map

    def set_indexer_num_replica(self, num_replica=0):
        api = self.indexUrl + 'settings'
        params = {'indexer.settings.num_replica': num_replica}
        params = json.dumps(params)
        status, _, _ = self._http_request(api, 'POST',
                                          params=params,
                                          timeout=60)
        error_message = ""
        self.log.debug('Settings params: {0}'.format(params))
        status, content, header = self._http_request(api, 'POST', params)
        if not status and error_message in content:
            # TODO: Currently it just acknowledges if there is an error.
            # And proceeds with further initialization.
            self.log.warning(content)
        return status

    def set_downgrade_storage_mode(self, downgrade=True):
        if downgrade:
            api = self.indexUrl + 'settings/storageMode?downgrade=true'
        else:
            api = self.indexUrl + 'settings/storageMode?downgrade=false'
        headers = self.get_headers_for_content_type_json()
        status, content, header = self._http_request(api, 'POST',
                                                     headers=headers)
        if not status:
            raise Exception(content)
        return json.loads(content)

    def create_index(self, create_info):
        self.log.debug("Create INDEX with params: %s" % create_info)
        api = self.indexUrl + 'api/indexes?create=true'
        headers = self.get_headers_for_content_type_json()
        params = json.loads("{0}".format(create_info).replace('\'', '"')
                            .replace('True', 'true').replace('False', 'false'))
        status, content, header = self._http_request(
            api, 'POST',
            headers=headers,
            params=json.dumps(params).encode("ascii", "ignore"))
        if not status:
            raise Exception(content)
        return json.loads(content)

    def build_index_with_rest(self, index_id):
        api = self.indexUrl + 'api/indexes?build=true'
        build_info = {'ids': [index_id]}
        headers = self.get_headers_for_content_type_json()
        status, content, header = self._http_request(
            api, 'PUT',
            headers=headers,
            params=json.dumps(build_info))
        if not status:
            raise Exception(content)
        return json.loads(content)

    def drop_index(self, index_id):
        url = 'api/index/{0}'.format(index_id)
        api = self.indexUrl + url
        headers = self.get_headers_for_content_type_json()
        status, content, header = self._http_request(api, 'DELETE',
                                                     headers=headers)
        if not status:
            self.log.error(content)
        return status

    def get_all_indexes(self):
        url = 'api/indexes'
        api = self.indexUrl + url
        headers = self.get_headers_for_content_type_json()
        status, content, header = self._http_request(api, 'GET',
                                                     headers=headers)
        if not status:
            raise Exception(content)
        return json.loads(content)

    def lookup_gsi_index(self, index_id, body):
        url = 'api/index/{0}?lookup=true'.format(index_id)
        api = self.indexUrl + url
        headers = self.get_headers_for_content_type_json()
        params = json.loads("{0}".format(body)
                            .replace('\'', '"')
                            .replace('True', 'true')
                            .replace('False', 'false'))
        status, content, header = self._http_request(
            api, 'GET',
            headers=headers,
            params=json.dumps(params).encode("ascii", "ignore"))
        if not status:
            raise Exception(content)
        return json.loads(content)

    def full_table_scan_gsi_index(self, index_id, body):
        if "limit" not in body.keys():
            body["limit"] = 900000
        url = 'internal/index/{0}?scanall=true'.format(index_id)
        api = self.indexUrl + url
        headers = self.get_headers_for_content_type_json()
        params = json.loads("{0}".format(body)
                            .replace('\'', '"')
                            .replace('True', 'true')
                            .replace('False', 'false'))
        status, content, header = self._http_request(
            api, 'GET', headers=headers,
            params=json.dumps(params).encode("ascii", "ignore"))
        if not status:
            raise Exception(content)
        # Following line is added since the content uses chunked encoding
        chunkless_content = content.replace("][", ", \n")
        return json.loads(chunkless_content)

    def range_scan_gsi_index(self, index_id, body):
        if "limit" not in body.keys():
            body["limit"] = 300000
        url = 'internal/index/{0}?range=true'.format(index_id)
        api = self.indexUrl + url
        headers = self.get_headers_for_content_type_json()
        params = json.loads("{0}".format(body).replace(
            '\'', '"').replace('True', 'true').replace('False', 'false'))
        status, content, header = self._http_request(
            api, 'GET', headers=headers,
            params=json.dumps(params).encode("ascii", "ignore"))
        if not status:
            raise Exception(content)
        # Below line is there because of MB-20758
        content = content.split("[]")[0]
        # Following line is added since the content uses chunked encoding
        chunkless_content = content.replace("][", ", \n")
        return json.loads(chunkless_content)

    def multiscan_for_gsi_index(self, index_id, body):
        url = 'api/index/{0}?multiscan=true'.format(index_id)
        api = self.indexUrl + url
        headers = self.get_headers_for_content_type_json()
        params = json.loads("{0}".format(body).replace('\'', '"').replace(
            'True', 'true').replace('False', 'false').replace(
            "~[]{}UnboundedtruenilNA~", "~[]{}UnboundedTruenilNA~"))
        params = json.dumps(params).encode("ascii", "ignore") \
            .replace("\\\\", "\\")
        self.log.debug(json.dumps(params).encode("ascii", "ignore"))
        status, content, header = self._http_request(api, 'GET',
                                                     headers=headers,
                                                     params=params)
        if not status:
            raise Exception(content)
        # Below line is there because of MB-20758
        content = content.split("[]")[0]
        # Following line is added since the content uses chunked encoding
        chunkless_content = content.replace("][", ", \n")
        if chunkless_content:
            return json.loads(chunkless_content)
        else:
            return content

    def multiscan_count_for_gsi_index(self, index_id, body):
        url = 'internal/index/{0}?multiscancount=true'.format(index_id)
        api = self.indexUrl + url
        headers = self.get_headers_for_content_type_json()
        count_cmd_body = body.replace('\'', '"').replace('True', 'true') \
            .replace('False', 'false')
        count_cmd_body = count_cmd_body.replace("~[]{}UnboundedtruenilNA~",
                                                "~[]{}UnboundedTruenilNA~")
        params = json.loads(count_cmd_body)
        params = json.dumps(params).encode("ascii", "ignore").replace("\\\\",
                                                                      "\\")
        self.log.debug(json.dumps(params).encode("ascii", "ignore"))
        status, content, header = self._http_request(api, 'GET',
                                                     headers=headers,
                                                     params=params)
        if not status:
            raise Exception(content)
        # Below line is there because of MB-20758
        content = content.split("[]")[0]
        # Following line is added since the content uses chunked encoding
        chunkless_content = content.replace("][", ", \n")
        if chunkless_content:
            return json.loads(chunkless_content)
        else:
            return content

    def get_index_status(self):
        """
        Fetches index stats using localhost:9102/getIndexStatus api

        :return result: Dictionary of index status as returned by the system
        """
        api = "{0}getIndexStatus".format(self.indexUrl)
        status, content, _ = self._http_request(api)
        result = dict()
        if status:
            result = json.loads(content)
        else:
            self.log.error("Failure during get_index_status: %s" % content)
        return result

    def get_index_stats(self, URL=None):
        """
        Fetches index stats using localhost:9102/stats api

        :return result: Dictionary of stats in format,
                        result[bucket_name][index_name][stat_name] = value
        """
        if URL is None:
            api = "{0}stats".format(self.indexUrl)
        else:
            api = "{0}stats".format(URL)
        status, content, _ = self._http_request(api)
        result = dict()
        if status:
            content = json.loads(content)
            result = dict()
            for key, value in content.items():
                t_key = key.split(":")
                if len(t_key) == 3:
                    if t_key[0] not in result:
                        result[t_key[0]] = dict()
                    if t_key[1] not in result[t_key[0]]:
                        result[t_key[0]][t_key[1]] = dict()
                    result[t_key[0]][t_key[1]][t_key[2]] = value
                else:
                    result[key] = value
        else:
            self.log.error("Failure during get_index_stats: %s" % content)

        return result

    def wait_for_indexing_to_complete(self, bucket_name,
                                      target_index=None, timeout=60):
        """
        Waits till the indexes 'num_docs_queued' to reach '0',
        meaning all docs are indexed.
        :param bucket_name: Name of the bucket to validate
        :param target_index: Index_name to wait for. None means wait for all
        :param timeout:
        :return index_completed: Boolean value to tell the index is done or not
        """
        self.log.info("Wait for indexing queue to reach '0'")
        timer = 0
        index_completed = False
        while timer < timeout and index_completed is False:
            index_completed = True
            stats = self.get_index_stats()
            if bucket_name in stats.keys():
                for index_name, index_stats in stats[bucket_name].items():
                    if target_index is not None and index_name != target_index:
                        continue
                    if index_stats["num_docs_queued"] != 0:
                        index_completed = False
                        break
                sleep(2, "Wait before next indexer stats query")
            else:
                self.log.debug("{} is not present in stats key".format(bucket_name))
        return index_completed

    def polling_for_index_training(self, bucket=None, index=None, timeout=60, sleep_time=10):
        self.log.info("Starting polling for index:"+str(index))
        for x in range(timeout):
            result = self.index_status()
            if bucket.name in result:
                if result[bucket.name].has_key(index):
                    self.log.debug("Check {}, {}: {}".format(str(x), index, result[bucket.name][index]['status']))
                    if result[bucket.name][index]['status'] not in ("Training", "Created"):
                        self.log.info("2i index is trained: {}".format(index))
                        self.log.info("Index {} training is completed in {}".format(index, str(x*sleep_time)))
                        return True
            else:
                self.log.info("Index {} not found with iteration {}".format(index, str(x)))
            sleep(sleep_time)
        return False

    def polling_create_index_status(self, bucket=None, index=None, timeout=60, sleep_time=10, status="Ready"):
        self.polling_for_index_training(bucket, index, timeout=timeout/10)
        self.log.info("Starting polling for index:"+str(index))
        for x in range(timeout):
            result = self.index_status()
            if bucket.name in result:
                if result[bucket.name].has_key(index):
                    self.log.debug("Check {}, {}: {}".format(str(x), index, result[bucket.name][index]['status']))
                    if result[bucket.name][index]['status'] == status or result[bucket.name][index]['status'] == "Ready":
                        self.log.info("2i index is ready: {}".format(index))
                        self.log.info("Index {} build is completed in {}".format(index, str(x*sleep_time)))
                        return True
            else:
                self.log.info("Index {} not found with iteration {}".format(index, str(x)))
            sleep(sleep_time)
        return False

    def polling_delete_index(self, bucket=None, index=None, timeout=100):
        for x in range(timeout):
            result = self.index_status()
            if result[bucket.name].get(index) is None:
                return True
            sleep(1)
            self.log.info("Index found with iteration {}".format(index, str(x)))
        return False

    def get_plasma_stats(self, nodes_list=None):
        """
        Fetches index stats using localhost:9102/stats/storage api
        :return result: Dictionary of stats in format,
                        result[bucket_name][index_name][stat_name] = value
        """
        result = dict()
        for node in nodes_list:
            generic_url = "http://%s:%s/"
            ip = node.ip
            port = "9102"
            if CbServer.use_https:
                generic_url = "https://%s:%s/"
                port = "19102"
            baseURL = generic_url % (ip, port)
            api = "{0}stats/storage".format(baseURL)
            status, content, _ = self._http_request(api)
            if status:
                content = json.loads(content)
                for key in content:
                    indexName = str(key.get('Index'))
                    backStoreStats = key.get('Stats').get('BackStore')
                    mainStoreStats = key.get('Stats').get('MainStore')
                    for item, itemValue in mainStoreStats.items():
                        result[indexName + "_" + item] = itemValue
            else:
                self.log.error("Failure during get_index_stats: %s" % content)
        return result

    def execute_query(self, query,  contentType='application/x-www-form-urlencoded',
                      connection='keep-alive', isIndexerQuery=False, retry=10, is_scan_consistency=True):
        status = None
        content = None
        header = None
        for x in range(retry):
            try:
                url = "%squery" % self.queryUrl
                if isIndexerQuery:
                    params = {'statement': query}
                    params = urllib.urlencode(params)
                else:
                    if is_scan_consistency:
                        params = urllib.urlencode({'scan_consistency': 'request_plus', 'statement': query})
                    else:
                        params = urllib.urlencode({'statement': query})
                status, content, header = self._http_request(url, 'POST', params,
                                                             headers=self._create_capi_headers(contentType=contentType,
                                                                              connection=connection))
                break
            except Exception as e:
                self.log.info("Got exception:{0} with index name".format(str(e)))
                sleep(10, "wait after exception")
        return status, content, header

    def get_bucket_index_stats(self, timeout=120):
        api = self.indexUrl + 'stats'
        status, content, _ = self._http_request(api, timeout=timeout)
        parsed = dict()
        if status:
            parsed = json.loads(content)
        index_map = {}
        for key in list(parsed.keys()):
            tokens = key.split(":")
            val = parsed[key]
            if len(tokens) == 1:
                field = tokens[0]
                index_map[field] = val
            elif len(tokens) == 3:
                bucket = tokens[0]
                index_name = tokens[1]
                stats_name = tokens[2]
                if bucket not in list(index_map.keys()):
                    index_map[bucket] = {}
                if index_name not in list(index_map[bucket].keys()):
                    index_map[bucket][index_name] = {}
                index_map[bucket][index_name][stats_name] = val
            elif len(tokens) == 5:
                bucket = tokens[0]
                scope_name = tokens[1]
                collection_name = tokens[2]
                index_name = tokens[3]
                stats_name = tokens[4]
                if bucket not in index_map:
                    index_map[bucket] = dict()
                if scope_name not in index_map[bucket]:
                    index_map[bucket][scope_name] = dict()
                if collection_name not in index_map[bucket][scope_name]:
                    index_map[bucket][scope_name][collection_name] = dict()
                if index_name not in index_map[bucket][scope_name][collection_name]:
                    index_map[bucket][scope_name][collection_name][index_name] = dict()
                index_map[bucket][scope_name][collection_name][index_name][stats_name] = val
        return index_map
