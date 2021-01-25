""" Functions for editing JSON format files which deal with log updates,
    to persist uuids and seq_nos for each vbucket.
    Currently stored within folder called 'logs'
    Potential scope to save more data about vbucket if needed
"""

import json
import os
from bucket_collections.collections_base import CollectionBase
from dcp_new.constants import *
from memcacheConstants import *
from dcp_bin_client import DcpClient
from mc_bin_client import MemcachedClient as McdClient
from dcp_data_persist import LogData
import uuid
from mc_bin_client import MemcachedError


class DCPBase(CollectionBase):
    def setUp(self):
        super(DCPBase, self).setUp()
        self.dictstore = {}
        self.verification_seqno = None
        self.collections = self.input.param("collections", True)
        self.xattrs = self.input.param("xattrs", False)
        self.compression = self.input.param("compression", 0)
        self.delete_times = self.input.param("delete_times", False)
        self.manifest = self.input.param("manifest", None)
        self.timeout = self.input.param("timeout", True)
        self.bucket = self.bucket_util.buckets[0]
        self.noop_interval = self.input.param("noop_interval", 0)
        self.opcode_dump = self.input.param("opcode_dump", False)
        self.enable_expiry = self.input.param("enable_expiry", False)
        self.enable_stream_id = self.input.param("enable_stream_id", False)
        #         vbuckets = self.input.param("vbuckets", 0)
        #         if len(vbuckets) == 1:
        self.vbuckets = range(1024)
        #         else:
        #             self.vbuckets = vbuckets.split(",")
        self.start_seq_no_list = self.input.param("start", [0] * len(self.vbuckets))
        self.end_seq_no = self.input.param("end", 0xffffffffffffffff)
        self.vb_uuid_list = self.input.param("vb_uuid_list", ['0'] * len(self.vbuckets))
        self.vb_retry = self.input.param("retry_limit", 10)
        self.filter_file = self.input.param("filter", None)
        self.stream_req_info = self.input.param("stream_req_info", False)
        self.failover_logging = self.input.param("failover_logging", False)
        self.keep_logs = self.input.param("keep_logs", False)
        self.log_path = self.input.param("log_path", None)
        self.keys = self.input.param("keys", True)
        self.docs = self.input.param("docs", False)
        self.dcp_client = self.initialise_cluster_connections()
        self.output_string = list()

    def tearDown(self):
        super(DCPBase, self).tearDown()

    def initialise_cluster_connections(self):
        init_dcp_client = self.initiate_connection()

        config_json = json.loads(DcpClient.get_config(init_dcp_client)[2])
        self.vb_map = config_json['vBucketServerMap']['vBucketMap']

        self.dcp_client_dict = dict()

        if self.log_path:
            self.log_path = os.path.normpath(self.log_path)
        self.dcp_log_data = LogData(self.log_path, self.vbuckets, self.keep_logs)

        # TODO: Remove globals and restructure (possibly into a class) to allow for multiple
        #       instances of the client (allowing multiple bucket connections)
        node_list = []
        for index, server in enumerate(config_json['vBucketServerMap']['serverList']):
            host = self.check_valid_host(server.split(':')[0], 'Server Config Cluster Map')
            node_list.append(host)
            port = config_json['nodesExt'][index]['services'].get('kv')

            if port is not None:
                node = '{0}:{1}'.format(host, port)
                if 'thisNode' in config_json['nodesExt'][index]:
                    self.dcp_client_dict[index] = {'stream': init_dcp_client,
                                                   'node': node}
                else:
                    self.dcp_client_dict[index] = {'stream': self.initiate_connection(),
                                                   'node': node}
        return init_dcp_client

    def initiate_connection(self):
        self.host = self.cluster.master.ip
        self.port = int(11210)
        timeout = self.timeout

        dcp_client = DcpClient(self.host, self.port, timeout=timeout, do_auth=False)
        self.log.info("DCP client created")

        try:
            response = dcp_client.sasl_auth_plain(
                self.cluster.master.rest_username,
                self.cluster.master.rest_password)
        except MemcachedError as err:
            self.log.info("DCP connection failure")

        self.check_for_features(dcp_client)

        dcp_client.bucket_select(str(self.bucket.name))
        self.log.info("Successfully AUTHed to %s" % self.bucket)

        name = "simple_dcp_client " + str(uuid.uuid4())
        response = dcp_client.open_producer(name,
                                            xattr=self.xattrs,
                                            delete_times=self.delete_times,
                                            collections=self.collections)
        assert response['status'] == SUCCESS
        self.log.info("Opened DCP consumer connection")

        response = dcp_client.general_control("enable_noop", "true")
        assert response['status'] == SUCCESS
        self.log.info("Enabled NOOP")

        if self.noop_interval:
            noop_interval = str(self.noop_interval)
            response2 = dcp_client.general_control("set_noop_interval", noop_interval)
            assert response2['status'] == SUCCESS
            self.log.info("NOOP interval set to %s" % noop_interval)

        if self.opcode_dump:
            dcp_client.opcode_dump_control(True)

        if (self.compression > 1):
            response = dcp_client.general_control("force_value_compression", "true")
            assert response['status'] == SUCCESS
            self.log.info("Forcing compression on connection")

        if self.enable_expiry:
            response = dcp_client.general_control("enable_expiry_opcode", "true")
            assert response['status'] == SUCCESS
            self.log.info("Enabled Expiry Output")

        if self.enable_stream_id:
            response = dcp_client.general_control("enable_stream_id", "true")
            assert response['status'] == SUCCESS
            self.log.info("Enabled Stream-ID")
        return dcp_client

    def check_for_features(self, dcp_client):
        features = [HELO_XERROR]
        if self.xattrs:
            features.append(HELO_XATTR)
        if self.collections:
            features.append(HELO_COLLECTIONS)
        if self.compression:
            features.append(HELO_SNAPPY)
        resp = dcp_client.hello(features, "pydcp feature HELO")
        for feature in features:
            assert feature in resp

    def handle_stream_create_response(self, dcpStream):
        if dcpStream.status == SUCCESS:
            self.log.debug('Stream Opened Successfully on vb %s' % dcpStream.vbucket)

            if self.failover_logging and not self.keep_logs:
                # keep_logs implies that there is a set of JSON log files
                self.dcp_log_data.upsert_failover(dcpStream.vbucket,
                                                  dcpStream.failover_log)
            return None

        elif dcpStream.status == ERR_NOT_MY_VBUCKET:
            self.log_failure('NOT MY VBUCKET -%s does not live on this node' \
                             % dcpStream.vbucket)
            # TODO: Handle that vbucket not entering the stream list

        elif dcpStream.status == ERR_ROLLBACK:
            self.log.info("Server requests Rollback to sequence number: %s" \
                          % dcpStream.rollback_seqno)
            dcpStream = self.handle_rollback(dcpStream)
            return dcpStream

        elif dcpStream.status == ERR_NOT_SUPPORTED:
            self.log_failure("Error: Stream Create Request Not Supported")

        else:
            self.log_failure("Unhandled Stream Create Response %s" \
                             % (dcpStream.status))

    def handleSystemEvent(self, response, manifest):
        # Unpack a DCP system event
        if response['event'] == EVENT_CREATE_COLLECTION:
            if response['version'] == 0:
                uid, sid, cid = struct.unpack(">QII", response['value'])
                uid = format(uid, 'x')
                sid = format(sid, 'x')
                cid = format(cid, 'x')
                string = "DCP Event: vb:{}, sid:{}, what:CollectionCREATED, name:{}, id:{}, scope:{}, manifest:{}," \
                         " seqno:{}".format(response['vbucket'],
                                            response['streamId'],
                                            response['key'],
                                            cid,
                                            sid,
                                            uid,
                                            response['by_seqno'])
                self.output_string.append(string)
                manifest['uid'] = uid
                for e in manifest['scopes']:
                    if e['uid'] == sid:
                        e['collections'].append({'name': response['key'],
                                                 'uid': cid});

            elif response['version'] == 1:
                uid, sid, cid, ttl = struct.unpack(">QIII", response['value'])
                uid = format(uid, 'x')
                sid = format(sid, 'x')
                cid = format(cid, 'x')
                string = "DCP Event: vb:{}, sid:{}, what:CollectionCREATED, name:{}, id:{}, scope:{}, ttl:{}, " \
                         "manifest:{}, seqno:{}".format(response['vbucket'],
                                                        response['streamId'],
                                                        response['key'],
                                                        cid,
                                                        sid,
                                                        ttl,
                                                        uid,
                                                        response['by_seqno'])
                self.output_string.append(string)
                manifest['uid'] = uid
                for e in manifest['scopes']:
                    if e['uid'] == sid:
                        e['collections'].append({'name': response['key'],
                                                 'uid': cid,
                                                 'max_ttl': 0});
            else:
                self.log.info("Unknown DCP Event version: %s" % response['version'])

        elif response['event'] == EVENT_DELETE_COLLECTION:
            # We can receive delete collection without a corresponding create, this
            # will happen when only the tombstone of a collection remains
            uid, sid, cid = struct.unpack(">QII", response['value'])
            uid = format(uid, 'x')
            sid = format(sid, 'x')
            cid = format(cid, 'x')
            string = "DCP Event: vb:{}, sid:{}, what:CollectionDROPPED, id:{}, scope:{},  manifest:{}, " \
                     "seqno:{}".format(response['vbucket'],
                                       response['streamId'],
                                       cid, sid, uid,
                                       response['by_seqno'])
            self.output_string.append(string)
            manifest['uid'] = uid
            collections = []
            for e in manifest['scopes']:
                update = False
                for c in e['collections']:
                    if c['uid'] != cid:
                        collections.append(c)
                        update = True
                if update:
                    e['collections'] = collections
                    break

        elif response['event'] == EVENT_CREATE_SCOPE:
            uid, sid = struct.unpack(">QI", response['value'])
            uid = format(uid, 'x')
            sid = format(sid, 'x')
            string = "DCP Event: vb:{}, sid:{}, what:ScopeCREATED, name:{}, id:{}, manifest:{}, " \
                     "seqno:{}".format(response['vbucket'],
                                       response['streamId'],
                                       response['key'],
                                       sid,
                                       uid,
                                       response['by_seqno'])
            self.output_string.append(string)

            # Record the scope
            manifest['uid'] = uid
            manifest['scopes'].append({'uid': sid,
                                       'name': response['key'],
                                       'collections': []})

        elif response['event'] == EVENT_DELETE_SCOPE:
            # We can receive delete scope without a corresponding create, this
            # will happen when only the tombstone of a scope remains
            uid, sid = struct.unpack(">QI", response['value'])
            uid = format(uid, 'x')
            sid = format(sid, 'x')
            string = "DCP Event: vb:{}, sid:{}, what:ScopeDROPPED, id:{}, manifest:{}, " \
                     "seqno:{}".format(response['vbucket'], response['streamId'],
                                       sid, uid, response['by_seqno'])
            self.output_string.append(string)
            manifest['uid'] = uid
            scopes = []
            for e in manifest['scopes']:
                if e['uid'] != sid:
                    scopes.append(e)
            manifest['scopes'] = scopes
        else:
            self.log.info("Unknown DCP Event:%s " % response['event'])
        return manifest

    def handleMutation(self, response):
        vb = response['vbucket']
        seqno = response['by_seqno']
        action = response['opcode']
        sid = response['streamId']
        output_string = ""
        if self.keys:
            output_string += "KEY:" + response['key'] + " from collection:" + str(
                response['collection_id']) + ", vb:" + str(vb) + " sid:" + str(sid) + " "
        if self.docs:
            output_string += "BODY:" + response['value']
        if self.xattrs:
            if 'xattrs' in response and response['xattrs'] != None:
                output_string += " XATTRS:" + response['xattrs']
            else:
                output_string += " XATTRS: - "
        if output_string != "":
            output_string = str(DCP_Opcode_Dictionary[action]) + " -> " + output_string
            return seqno, output_string

    def handleMarker(self, response):
        self.log.info("Snapshot Marker vb:{}, sid:{}, " \
                      "start:{}, end:{}, flag:{}".format(response['vbucket'],
                                                         response['streamId'],
                                                         response['snap_start_seqno'],
                                                         response['snap_end_seqno'],
                                                         response['flag']))
        return int(response['snap_start_seqno']), int(response['snap_end_seqno'])

    def checkSnapshot(self, vb, se, current, stream):
        if se == current:
            self.log.info("Snapshot for vb:%s has completed, end:%s, " \
                          "stream.mutation_count:%s" % (vb, se, stream.mutation_count))

    def process_dcp_traffic(self, streams):
        active_streams = len(streams)
        self.log.info("no of active streams %s" % active_streams)

        while active_streams > 0:

            for vb in streams:
                stream = vb['stream']
                if not vb['complete']:
                    if stream.has_response():
                        response = stream.next_response()
                        if response == None:
                            self.log.debug("No response from vbucket %s" % vb['id'])
                            vb['complete'] = True
                            active_streams -= 1
                            self.dcp_log_data.push_sequence_no(vb['id'])
                            continue

                        opcode = response['opcode']
                        if (opcode == CMD_MUTATION or
                                opcode == CMD_DELETION or
                                opcode == CMD_EXPIRATION):
                            seqno, output_string = self.handleMutation(response)
                            self.output_string.append(output_string)
                            if self.failover_logging:
                                self.dcp_log_data.upsert_sequence_no(response['vbucket'],
                                                                     response['by_seqno'])

                            vb['timed-out'] = self.vb_retry

                            self.checkSnapshot(response['vbucket'],
                                               vb['snap_end'],
                                               response['by_seqno'],
                                               stream)
                        elif opcode == CMD_SNAPSHOT_MARKER:
                            vb['snap_start'], vb['snap_end'] = self.handleMarker(response)
                        elif opcode == CMD_SYSTEM_EVENT:
                            vb['manifest'] = self.handleSystemEvent(response,
                                                                    vb['manifest'])
                            self.checkSnapshot(response['vbucket'],
                                               vb['snap_end'],
                                               response['by_seqno'],
                                               stream)
                        elif opcode == CMD_STREAM_END:
                            self.log.info("Received stream end. Stream complete with " \
                                          "reason {}.".format(response['flags']))
                            vb['complete'] = True
                            active_streams -= 1
                            self.dcp_log_data.push_sequence_no(response['vbucket'])
                        else:
                            self.log.info("Unexpected and unhandled opcode:{}".format(opcode))
                    else:
                        self.log.debug('No response')

                if vb['complete']:
                    # Second-tier timeout - after the stream close to allow other vbuckets to execute
                    if vb['timed-out'] > 0:
                        vb['complete'] = False
                        active_streams += 1
                        vb['timed-out'] -= 1
                    else:
                        if vb['stream_open']:
                            # Need to close stream to vb - TODO: use a function of mc client instead of raw socket
                            header = struct.pack(RES_PKT_FMT,
                                                 REQ_MAGIC_BYTE,
                                                 CMD_CLOSE_STREAM,
                                                 0, 0, 0, vb['id'], 0, 0, 0)
                            self.select_dcp_client(vb['id']).s.sendall(header)
                            vb['stream_open'] = False
                            if self.stream_req_info:
                                self.log.info('Stream to vbucket(s) closed' % str(vb['id']))

        # Dump each VB manifest if collections were enabled
        if self.collections:
            for vb in streams:
                self.log.info("vb:{} The following manifest state was created from the " \
                              "system events".format(vb['id']))
                self.log.info(json.dumps(vb['manifest'], sort_keys=True, indent=2))
                break
        return self.output_string

    def add_streams(self, vbuckets, start, end, uuid, retry_limit=15, filter=None):
        self.vb_list = vbuckets
        self.start_seq_no_list = start
        self.end_seq_no = end
        self.vb_uuid_list = uuid
        self.vb_retry = retry_limit
        self.filter_file = filter
        filter_json = []
        streams = []

        # Filter is a file containing JSON, it can either be a single DCP
        # stream-request value, or an array of many values. Use of many values
        # is intended to be used in conjunction with enable_stream_id and sid
        if self.collections and self.filter_file != None:
            #             filter_file = open(self.filter_file, "r")
            #             jsonData = filter_file.read()
            parsed = json.loads(self.filter_file)

            # Is this an array or singular filter?
            if 'streams' in parsed:
                for f in parsed['streams']:
                    filter_json.append(json.dumps(f))
            else:
                # Assume entire document is the filter
                filter_json.append(self.filter_file)
            self.log.info("DCP Open filter: {}".format(filter_json))
        else:
            filter_json.append('')

        for f in filter_json:
            for index in xrange(0, len(self.vb_list)):
                if self.stream_req_info:
                    self.log.info('Stream to vbucket %s on node %s with seq no %s and uuid %s' \
                                  % (self.vb_list[index], self.get_node_of_dcp_client_connection(
                        self.vb_list[index]), self.start_seq_no_list[index],
                                     self.vb_uuid_list[index]))
                vb = int(self.vb_list[index])
                stream = self.select_dcp_client(vb).stream_req(vbucket=vb,
                                                               takeover=0,
                                                               start_seqno=int(self.start_seq_no_list[index]),
                                                               end_seqno=self.end_seq_no,
                                                               vb_uuid=int(self.vb_uuid_list[index]),
                                                               json=f)
                handle_response = self.handle_stream_create_response(stream)
                if handle_response is None:
                    vb_stream = {"id": self.vb_list[index],
                                 "complete": False,
                                 "keys_recvd": 0,
                                 "timed-out": self.vb_retry,  # Counts the amount of times that vb_stream gets timed out
                                 "stream_open": True,  # Details whether a vb stream is open to avoid repeatedly closing
                                 "stream": stream,
                                 # Set the manifest so we assume _default scope and collection exist
                                 # KV won't replicate explicit create events for these items
                                 "manifest":
                                     {'scopes': [
                                         {'uid': "0", 'name': '_default', 'collections': [
                                             {'uid': "0", 'name': '_default'}]}], 'uid': 0},
                                 "snap_start": 0,
                                 "snap_end": 0
                                 }
                    streams.append(vb_stream)
                else:
                    for vb_stream in handle_response:
                        streams.append(vb_stream)
        return streams

    def handle_rollback(self, dcpStream):
        updated_dcpStreams = []
        vb = dcpStream.vbucket
        requested_rollback_no = dcpStream.rollback_seqno

        # If argument to use JSON log files
        if self.failover_logging:
            log_fetch = self.dcp_log_data.get_failover_logs([vb])
            if log_fetch.get(str(vb), None) is not None:
                # If the failover log is not empty, use it
                data = log_fetch[str(vb)]
                failover_values = sorted(data, key=lambda x: x[1], reverse=True)
            else:
                failover_fetch = DcpClient.get_failover_log(self.select_dcp_client(vb), vb)
                failover_values = failover_fetch.get('value')

        # Otherwise get failover log from server
        else:
            failover_fetch = DcpClient.get_failover_log(self.select_dcp_client(vb), vb)
            failover_values = failover_fetch.get('value')

        new_seq_no = []
        new_uuid = []
        failover_seq_num = 0  # Default values so that if they don't get set
        failover_vbucket_uuid = 0  # inside the for loop, 0 is used.

        for failover_log_entry in failover_values:
            if failover_log_entry[1] <= requested_rollback_no:
                failover_seq_num = failover_log_entry[1]  # closest Seq number in log
                failover_vbucket_uuid = failover_log_entry[0]  # and its UUID
                break
        new_seq_no.append(failover_seq_num)
        new_uuid.append(failover_vbucket_uuid)

        self.log.info('Retrying stream add on vb %s with seqs %s and uuids %s' % (vb, new_seq_no, new_uuid))

        # NOTE: This can cause continuous rollbacks making client side recursive dependent on failover logs.
        self.log.info("check for recursive loop")
        return self.add_streams([vb], new_seq_no, self.end_seq_no, new_uuid, self.vb_retry, self.filter_file)

    def select_dcp_client(self, vb):
        main_node_id = self.vb_map[vb][0]

        # TODO: Adding support if not my vbucket received to use replica node.
        # if len(vb_map[vb]) > 1:
        #     replica1_node_id = vb_map[vb][1]
        # if len(vb_map[vb]) > 2:
        #     replica2_node_id = vb_map[vb][2]
        # if len(vb_map[vb]) > 3:
        #     replica3_node_id = vb_map[vb][3]

        return self.dcp_client_dict[main_node_id]['stream']

    def check_valid_host(self, host, errorOrigin):
        separate_host = host.split('.')
        if len(separate_host) == 4:
            for num in separate_host:
                if int(num) < 0 or int(num) > 255:
                    raise IndexError("The inputted host (",
                                     host,
                                     ") has an ip address outside the standardised range. "
                                     "Error origin:", errorOrigin)
            return host
        else:
            if host == 'localhost':
                return host
            elif host == '$HOST':
                self.log.info("using localhost")
                return 'localhost'
            else:
                raise StandardError("Invalid host input", host, "Error origin:", errorOrigin)

    def get_node_of_dcp_client_connection(self, vb):
        node_id = self.vb_map[vb][0]
        return self.dcp_client_dict[node_id]['node']

    def all_vbucket_ids(self, type_=None):
        vb_ids = []
        response = self.mcd_client.stats('vbucket')
        assert len(response) > 0

        for vb in response:
            if vb != '' and (type_ is None or response[vb] == type_):
                vb_id = int(vb.split('_')[-1])
                vb_ids.append(vb_id)

        return vb_ids

    def wait_for_persistence(self, client):
        if self.bucket_type == 'ephemeral':
            return
        while int(self.get_stat(client, 'ep_queue_size')) > 0:
            self.sleep(1)
        while int(self.get_stat(client, 'ep_commit_num')) == 0:
            self.sleep(1)

    @staticmethod
    def get_stat(client, stat, type=''):
        response = client.stats(type)
        return response[stat]

    def wait_for_warmup(self, host, port):
        if self.bucket_type == 'ephemeral': return
        while True:
            client = McdClient(host, port)
            try:
                client.bucket_select("default")
                response = client.stats()
                # check the old style or new style (as of 4.5) results
                mode = response.get('ep_degraded_mode')
                if mode is not None:
                    if mode == '0' or mode == 'false':
                        break
            except Exception as ex:
                pass
            self.sleep(1)
