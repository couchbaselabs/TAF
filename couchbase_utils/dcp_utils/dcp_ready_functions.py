import json
import os

from dcp_new.constants import *
from memcacheConstants import *
from dcp_bin_client import DcpClient
from global_vars import logger
import uuid
from mc_bin_client import MemcachedError
import user


class DCPUtils(object):
    def __init__(self, node, bucket, start_seq_no_list, end_seq_no, vb_uuid_list, port=11210,
                 timeout=True, do_auth=False, xattrs=False,
                 delete_times=False, collections=True,
                 noop_interval=0, opcode_dump=False,
                 compression=0, enable_expiry=False,
                 enable_stream_id=False, user="Administrator",
                 pwd="password", stream_req_info=False,
                 log_path=None, keep_logs=False, filter_file=None,
                 vb_retry=10, failover_logging=False,
                 keys=True, docs=False):
        self.node = node
        self.bucket = bucket
        self.host = self.node.ip
        self.start_seq_no_list = start_seq_no_list
        self.end_seq_no = end_seq_no
        self.vb_uuid_list = vb_uuid_list
        self.port = port
        self.timeout = timeout
        self.do_auth = do_auth
        self.xattrs = xattrs
        self.delete_times = delete_times
        self.collections = collections
        self.noop_interval = noop_interval
        self.opcode_dump = opcode_dump
        self.compression = compression
        self.enable_expiry = enable_expiry
        self.enable_stream_id = enable_stream_id
        self.stream_req_info = stream_req_info
        self.log_path = log_path
        self.keep_logs = keep_logs
        self.user = user
        self.pwd = pwd
        self.log = logger.get("test")
        self.vbuckets = range(1024)
        self.filter_file = filter_file
        self.vb_retry = vb_retry
        self.failover_logging = failover_logging
        self.vb_map = None
        self.keys = keys
        self.docs = docs
        self.output_string = list()

    def initialise_cluster_connections(self):
        self.dcp_client = self.connect()
        config_json = json.loads(DcpClient.get_config(self.dcp_client)[2])
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
                    self.dcp_client_dict[index] = {'stream': self.dcp_client,
                                                   'node': node}
                else:
                    self.dcp_client_dict[index] = {'stream': self.dcp_client,
                                                   'node': node}
        return self.dcp_client

    def connect(self):
        dcp_client = DcpClient(self.host,
                               self.port, timeout=self.timeout,
                               do_auth=self.do_auth)
        self.log.info("DCP client created")
        try:
            response = dcp_client.sasl_auth_plain(
                self.user,
                self.pwd)
        except MemcachedError as err:
            self.log.info("DCP connection failure")
        self.check_for_features(dcp_client)

        dcp_client.bucket_select(str(self.bucket.name))
        self.log.info("Successfully AUTHed to bucket %s" % self.bucket.name)

        name = "simple_dcp_client " + str(uuid.uuid4())
        response = dcp_client.open_producer(name,
                                            xattr=self.xattrs,
                                            delete_times=self.delete_times,
                                            collections=self.collections)
        assert response['status'] == SUCCESS
        self.log.info("Opened DCP consumer connection")

        response = dcp_client.general_control("enable_noop", "true")
        assert response['status'] == SUCCESS
        self.log.debug("Enabled NOOP")

        if self.noop_interval:
            noop_interval = str(self.noop_interval)
            response2 = dcp_client.general_control("set_noop_interval", noop_interval)
            assert response2['status'] == SUCCESS
            self.log.debug("NOOP interval set to %s" % noop_interval)

        if self.opcode_dump:
            dcp_client.opcode_dump_control(True)

        if (self.compression > 1):
            response = dcp_client.general_control("force_value_compression", "true")
            assert response['status'] == SUCCESS
            self.log.debug("Forcing compression on connection")

        if self.enable_expiry:
            response = dcp_client.general_control("enable_expiry_opcode", "true")
            assert response['status'] == SUCCESS
            self.log.debug("Enabled Expiry Output")

        if self.enable_stream_id:
            response = dcp_client.general_control("enable_stream_id", "true")
            assert response['status'] == SUCCESS
            self.log.debug("Enabled Stream-ID")
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

    def add_streams(self, vbuckets, start, end, uuid, retry_limit=15, filter=None):
        self.vb_list = vbuckets
        self.start_seq_no_list = start
        self.end_seq_no = end
        self.vb_uuid_list = uuid
        self.vb_retry = retry_limit
        self.filter_file = filter
        filter_json = []
        streams = []
        '''
        Filter is a file containing JSON, it can either be a single DCP
        stream-request value, or an array of many values. Use of many values
        is intended to be used in conjunction with enable_stream_id and sid
        '''
        if self.collections and self.filter_file != None:
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

    def set_vb_map(self, vb_map):
        self.vb_map = vb_map

    def get_node_of_dcp_client_connection(self, vb):
        node_id = self.vb_map[vb][0]
        return self.dcp_client_dict[node_id]['node']
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

    def checkSnapshot(self, vb, se, current, stream):
        if se == current:
            self.log.debug("Snapshot for vb:%s has completed, end:%s, " \
                          "stream.mutation_count:%s" % (vb, se, stream.mutation_count))

    def handleMarker(self, response):
        self.log.debug("Snapshot Marker vb:{}, sid:{}, " \
                      "start:{}, end:{}, flag:{}".format(response['vbucket'],
                                                         response['streamId'],
                                                         response['snap_start_seqno'],
                                                         response['snap_end_seqno'],
                                                         response['flag']))
        return int(response['snap_start_seqno']), int(response['snap_end_seqno'])

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

    def process_dcp_traffic(self, streams):
        active_streams = len(streams)

        while active_streams > 0:
            for vb in streams:
                stream = vb['stream']
                if not vb['complete']:
                    if stream.has_response():
                        response = stream.next_response()
                        if response == None:
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
                self.log.debug(json.dumps(vb['manifest'], sort_keys=True, indent=2))
                break
        return self.output_string

    def get_dcp_event(self, filter_file=None):
        self.filter_file = filter_file
        streams = self.add_streams(self.vbuckets,
                                   self.start_seq_no_list,
                                   self.end_seq_no,
                                   self.vb_uuid_list,
                                   self.vb_retry, self.filter_file)
        output = self.process_dcp_traffic(streams)
        self.close_dcp_streams()
        return output

    def select_dcp_client(self, vb):
        main_node_id = self.vb_map[vb][0]
        return self.dcp_client_dict[main_node_id]['stream']


    def close_dcp_streams(self):
        for client_stream in self.dcp_client_dict.values():
            client_stream['stream'].close()

class LogData(object):
    """ Class to control instance of data for vbuckets
        If internal use is requested, 'None' should be passed into dirpath"""

    def __init__(self, dirpath, vbucket_list, keep_logs):
        self.dictstore = {}
        if dirpath is not None:
            # Create with external file logging
            self.external = True
            self.path = os.path.join(dirpath, os.path.normpath('logs/'))
            if keep_logs:
                reset_list = []
                preset_list = []
                for vb in vbucket_list:
                    if os.path.exists(self.get_path(vb)):
                        preset_list.append(vb)
                    else:
                        reset_list.append(vb)
                self.setup_log_preset(preset_list)
            else:
                reset_list = vbucket_list
            self.reset(reset_list)
        else:
            self.external = False


    def setup_log_preset(self, vb_list):
        """ Used when --keep-logs is triggered, to move external data to dictstore """
        external_data = self.get_all_external(vb_list)
        for key in external_data.keys():
            self.dictstore[str(key)] = external_data[key]

    def get_path(self, vb):
        """ Retrieves path to log file for inputted virtual bucket number """
        # Make directory if it doesn't exist
        if self.external:
            fullpath = os.path.join(self.path, os.path.normpath('{}.json'.format(vb)))
            dirname = os.path.dirname(fullpath)
            if dirname and not os.path.exists(dirname):
                os.mkdir(dirname)
            elif os.path.isdir(dirname):
                pass  # Confirms that directory exists
            else:
                raise IOError("Cannot create directory inside a file")

            return fullpath

        else:
            raise RuntimeError('LogData specified as internal, no external path')

    def reset(self, vb_list):
        """ Clears/makes files for list of virtual bucket numbers"""
        for vb in vb_list:
            self.dictstore[str(vb)] = {}
            if self.external:
                path_string = self.get_path(vb)
                with open(path_string, 'w') as f:
                    json.dump({}, f)

    def upsert_failover(self, vb, failover_log):
        """ Insert / update failover log """
        if str(vb) in self.dictstore.keys():
            self.dictstore[str(vb)]['failover_log'] = failover_log
        else:
            self.dictstore[str(vb)] = {'failover_log': failover_log}

        if self.external:
            path_string = self.get_path(vb)

            with open(path_string, 'r') as vb_log:
                data = json.load(vb_log)

            data['failover_log'] = failover_log

            with open(path_string, 'w') as vb_log:
                json.dump(data, vb_log)

    def upsert_sequence_no(self, vb, seq_no):
        """ Insert / update sequence number, and move old sequence number to appropriate list """
        if str(vb) in self.dictstore.keys():
            if 'seq_no' in self.dictstore[str(vb)].keys():
                if 'old_seq_no' in self.dictstore[str(vb)].keys():
                    old_seq_no = self.dictstore[str(vb)]['old_seq_no']
                else:
                    old_seq_no = []
                old_seq_no.append(self.dictstore[str(vb)]['seq_no'])
                self.dictstore[str(vb)]['old_seq_no'] = old_seq_no
            self.dictstore[str(vb)]['seq_no'] = seq_no
        else:
            self.dictstore[str(vb)] = {'seq_no': seq_no}

    def push_sequence_no(self, vb):
        """ Push sequence number and old sequence number to external JSON files """
        if self.external:
            path_string = self.get_path(vb)

            with open(path_string, 'r') as vb_log:
                data = json.load(vb_log)

            data['old_seq_no'] = self.dictstore[str(vb)].get('old_seq_no')

            data['seq_no'] = self.dictstore[str(vb)].get('seq_no')

            with open(path_string, 'w') as vb_log:
                json.dump(data, vb_log)

    def get_all(self, vb_list):
        """ Return a dictionary where keys are vbuckets and the data is the total JSON for that vbucket """
        read_dict = {}
        for vb in vb_list:
            if str(vb) in self.dictstore.keys():
                read_dict[str(vb)] = self.dictstore[str(vb)]

        return read_dict

    def get_all_external(self, vb_list):
        read_dict = {}
        if self.external:
            for vb in vb_list:
                path_string = self.get_path(vb)
                if os.path.exists(path_string):
                    with open(path_string, 'r') as vb_log:
                        data = json.load(vb_log)
                    read_dict[str(vb)] = data
            return read_dict
        else:
            raise IOError("No external files setup")

    def get_seq_nos(self, vb_list):
        """ Return a dictionary where keys are vbuckets and the data is the sequence number """
        read_dict = {}
        for vb in vb_list:
            if str(vb) in self.dictstore:
                read_dict[str(vb)] = self.dictstore[str(vb)].get('seq_no')

        return read_dict

    def get_failover_logs(self, vb_list):
        """ Return a dictionary where keys are vbuckets and the data is the failover log list """
        read_dict = {}
        for vb in vb_list:
            if str(vb) in self.dictstore:
                read_dict[str(vb)] = self.dictstore[str(vb)].get('failover_log')

        return read_dict
