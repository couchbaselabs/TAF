""" Functions for editing JSON format files which deal with log updates,
    to persist uuids and seq_nos for each vbucket.
    Currently stored within folder called 'logs'
    Potential scope to save more data about vbucket if needed
"""

import json
import os
from basetestcase import BaseTestCase
from dcp_new.constants import *
from memcacheConstants import *
from dcp_bin_client import DcpClient
from mc_bin_client import MemcachedClient as McdClient

class DCPBase(BaseTestCase):
    def setUp(self):
        super(DCPBase, self).setUp()
        self.dictstore = {}
        self.verification_seqno = None

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

    def check_for_features(dcp_client, xattrs=False, collections=False, compression=False):
        features = [HELO_XERROR]
        if xattrs:
            features.append(HELO_XATTR)
        if collections:
            features.append(HELO_COLLECTIONS)
        if compression:
            features.append(HELO_SNAPPY)
        resp = dcp_client.hello(features, "pydcp feature HELO")
        for feature in features:
            assert feature in resp

    def handle_stream_create_response(dcpStream, args):
        if dcpStream.status == SUCCESS:
            print 'Stream Opened Successfully on vb', dcpStream.vbucket

            if args.failover_logging and not args.keep_logs:  # keep_logs implies that there is a set of JSON log files
                dcp_log_data.upsert_failover(dcpStream.vbucket, dcpStream.failover_log)
            return None

        elif dcpStream.status == ERR_NOT_MY_VBUCKET:
            print "NOT MY VBUCKET -", dcpStream.vbucket, 'does not live on this node'
            # TODO: Handle that vbucket not entering the stream list
            sys.exit(1)

        elif dcpStream.status == ERR_ROLLBACK:
            print "Server requests Rollback to sequence number:", dcpStream.rollback_seqno
            dcpStream = handle_rollback(dcpStream, args)
            return dcpStream

        elif dcpStream.status == ERR_NOT_SUPPORTED:
            print "Error: Stream Create Request Not Supported"
            sys.exit(1)

        else:
            print "Unhandled Stream Create Response {} {}".format(dcpStream.status, error_to_str(dcpStream.status))
            sys.exit(1)

    def handleSystemEvent(response, manifest):
        # Unpack a DCP system event
        if response['event'] == EVENT_CREATE_COLLECTION:
            if response['version'] == 0:
                uid, sid, cid = struct.unpack(">QII", response['value'])
                uid = format(uid, 'x')
                sid = format(sid, 'x')
                cid = format(cid, 'x')
                print "DCP Event: vb:{}, sid:{}, what:CollectionCREATED, name:\"{}\", id:{}, scope:{}, manifest:{}," \
                      " seqno:{}".format(response['vbucket'],
                                         response['streamId'],
                                         response['key'],
                                         cid,
                                         sid,
                                         uid,
                                         response['by_seqno'])
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
                print "DCP Event: vb:{}, sid:{}, what:CollectionCREATED, name:\"{}\", id:{}, scope:{}, ttl:{}, " \
                      "manifest:{}, seqno:{}".format(response['vbucket'],
                                                     response['streamId'],
                                                     response['key'],
                                                     cid,
                                                     sid,
                                                     ttl,
                                                     uid,
                                                     response['by_seqno'])
                manifest['uid'] = uid
                for e in manifest['scopes']:
                    if e['uid'] == sid:
                        e['collections'].append({'name': response['key'],
                                                 'uid': cid,
                                                 'max_ttl': 0});
            else:
                print "Unknown DCP Event version:", response['version']

        elif response['event'] == EVENT_DELETE_COLLECTION:
            # We can receive delete collection without a corresponding create, this
            # will happen when only the tombstone of a collection remains
            uid, sid, cid = struct.unpack(">QII", response['value'])
            uid = format(uid, 'x')
            sid = format(sid, 'x')
            cid = format(cid, 'x')
            print "DCP Event: vb:{}, sid:{}, what:CollectionDROPPED, id:{}, scope:{},  manifest:{}, " \
                  "seqno:{}".format(response['vbucket'], response['streamId'], cid, sid, uid, response['by_seqno'])
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
            print "DCP Event: vb:{}, sid:{}, what:ScopeCREATED, name:\"{}\", id:{}, manifest:{}, " \
                  "seqno:{}".format(response['vbucket'],
                                    response['streamId'],
                                    response['key'],
                                    sid,
                                    uid,
                                    response['by_seqno'])

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
            print "DCP Event: vb:{}, sid:{}, what:ScopeDROPPED, id:{}, manifest:{}, " \
                  "seqno:{}".format(response['vbucket'], response['streamId'], sid, uid, response['by_seqno'])
            manifest['uid'] = uid
            scopes = []
            for e in manifest['scopes']:
                if e['uid'] != sid:
                    scopes.append(e)
            manifest['scopes'] = scopes
        else:
            print "Unknown DCP Event:", response['event']
        return manifest

    def handleMutation(response):
        vb = response['vbucket']
        seqno = response['by_seqno']
        action = response['opcode']
        sid = response['streamId']
        output_string = ""
        if args.keys:
            output_string += "KEY:" + response['key'] + " from collection:" + str(
                response['collection_id']) + ", vb:" + str(vb) + " sid:" + str(sid) + " "
        if args.docs:
            output_string += "BODY:" + response['value']
        if args.xattrs:
            if 'xattrs' in response and response['xattrs'] != None:
                output_string += " XATTRS:" + response['xattrs']
            else:
                output_string += " XATTRS: - "
        if output_string != "":
            output_string = str(DCP_Opcode_Dictionary[action]) + " -> " + output_string
            print seqno, output_string

    def handleMarker(response):
        print "Snapshot Marker vb:{}, sid:{}, " \
              "start:{}, end:{}, flag:{}".format(response['vbucket'],
                                                 response['streamId'],
                                                 response['snap_start_seqno'],
                                                 response['snap_end_seqno'],
                                                 response['flag'])
        return int(response['snap_start_seqno']), int(response['snap_end_seqno'])

    def checkSnapshot(vb, se, current, stream):
        if se == current:
            print "Snapshot for vb:{} has completed, end:{}, " \
                  "stream.mutation_count:{}".format(vb, se, stream.mutation_count)

    def process_dcp_traffic(streams, args):
        active_streams = len(streams)

        while active_streams > 0:

            for vb in streams:
                stream = vb['stream']
                if not vb['complete']:
                    if stream.has_response():
                        response = stream.next_response()
                        if response == None:
                            print "No response from vbucket", vb['id']
                            vb['complete'] = True
                            active_streams -= 1
                            dcp_log_data.push_sequence_no(vb['id'])
                            continue

                        opcode = response['opcode']
                        if (opcode == CMD_MUTATION or
                                opcode == CMD_DELETION or
                                opcode == CMD_EXPIRATION):
                            handleMutation(response)
                            if args.failover_logging:
                                dcp_log_data.upsert_sequence_no(response['vbucket'],
                                                                response['by_seqno'])

                            vb['timed-out'] = args.retry_limit

                            checkSnapshot(response['vbucket'],
                                          vb['snap_end'],
                                          response['by_seqno'],
                                          stream)
                        elif opcode == CMD_SNAPSHOT_MARKER:
                            vb['snap_start'], vb['snap_end'] = handleMarker(response)
                        elif opcode == CMD_SYSTEM_EVENT:
                            vb['manifest'] = handleSystemEvent(response,
                                                               vb['manifest'])
                            checkSnapshot(response['vbucket'],
                                          vb['snap_end'],
                                          response['by_seqno'],
                                          stream)
                        elif opcode == CMD_STREAM_END:
                            print "Received stream end. Stream complete with " \
                                  "reason {}.".format(response['flags'])
                            vb['complete'] = True
                            active_streams -= 1
                            dcp_log_data.push_sequence_no(response['vbucket'])
                        else:
                            print "Unexpected and unhandled opcode:{}".format(opcode)
                    else:
                        print 'No response'

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
                            select_dcp_client(vb['id']).s.sendall(header)
                            vb['stream_open'] = False
                            if args.stream_req_info:
                                print 'Stream to vbucket(s)', str(vb['id']), 'closed'

        # Dump each VB manifest if collections were enabled
        if args.collections:
            for vb in streams:
                print "vb:{} The following manifest state was created from the " \
                      "system events".format(vb['id'])
                print json.dumps(vb['manifest'], sort_keys=True, indent=2)

    def initiate_connection(args):
        node = args.node
        bucket = args.bucket
        stream_xattrs = args.xattrs
        include_delete_times = args.delete_times
        stream_collections = args.collections
        use_compression = (args.compression > 0)
        force_compression = (args.compression > 1)
        host, port = args.node.split(":")
        host = check_valid_host(host, 'User Input')
        timeout = args.timeout

        dcp_client = DcpClient(host, int(port), timeout=timeout, do_auth=False)
        print 'Connected to:', node

        try:
            response = dcp_client.sasl_auth_plain(args.user, args.password)
        except MemcachedError as err:
            print 'ERROR:', err
            sys.exit(1)

        check_for_features(dcp_client, xattrs=stream_xattrs, collections=stream_collections, \
                           compression=use_compression)

        dcp_client.bucket_select(bucket)
        print "Successfully AUTHed to", bucket

        name = "simple_dcp_client " + str(uuid.uuid4())
        response = dcp_client.open_producer(name,
                                            xattr=stream_xattrs,
                                            delete_times=include_delete_times,
                                            collections=stream_collections)
        assert response['status'] == SUCCESS
        print "Opened DCP consumer connection"

        response = dcp_client.general_control("enable_noop", "true")
        assert response['status'] == SUCCESS
        print "Enabled NOOP"

        if args.noop_interval:
            noop_interval = str(args.noop_interval)
            response2 = dcp_client.general_control("set_noop_interval", noop_interval)
            assert response2['status'] == SUCCESS
            print "NOOP interval set to", noop_interval

        if args.opcode_dump:
            dcp_client.opcode_dump_control(True)

        if force_compression:
            response = dcp_client.general_control("force_value_compression", "true")
            assert response['status'] == SUCCESS
            print "Forcing compression on connection"

        if args.enable_expiry:
            response = dcp_client.general_control("enable_expiry_opcode", "true")
            assert response['status'] == SUCCESS
            print "Enabled Expiry Output"

        if args.enable_stream_id:
            response = dcp_client.general_control("enable_stream_id", "true")
            assert response['status'] == SUCCESS
            print "Enabled Stream-ID"

        return dcp_client

    def add_streams(args):
        vb_list = args.vbuckets
        start_seq_no_list = args.start
        end_seq_no = args.end
        vb_uuid_list = args.uuid
        vb_retry = args.retry_limit
        filter_file = args.filter
        filter_json = []
        stream_collections = args.collections
        streams = []

        # Filter is a file containing JSON, it can either be a single DCP
        # stream-request value, or an array of many values. Use of many values
        # is intended to be used in conjunction with enable_stream_id and sid
        if stream_collections and filter_file != None:
            filter_file = open(args.filter, "r")
            jsonData = filter_file.read()
            parsed = json.loads(jsonData)

            # Is this an array or singular filter?
            if 'streams' in parsed:
                for f in parsed['streams']:
                    filter_json.append(json.dumps(f))
            else:
                # Assume entire document is the filter
                filter_json.append(jsonData)
            print "DCP Open filter: {}".format(filter_json)
        else:
            filter_json.append('')

        for f in filter_json:
            for index in xrange(0, len(vb_list)):
                if args.stream_req_info:
                    print 'Stream to vbucket', vb_list[index], 'on node', get_node_of_dcp_client_connection(
                        vb_list[index]), \
                        'with seq no', start_seq_no_list[index], 'and uuid', vb_uuid_list[index]
                vb = vb_list[index]
                stream = select_dcp_client(vb).stream_req(vbucket=vb,
                                                          takeover=0,
                                                          start_seqno=int(start_seq_no_list[index]),
                                                          end_seqno=end_seq_no,
                                                          vb_uuid=int(vb_uuid_list[index]),
                                                          json=f)
                handle_response = handle_stream_create_response(stream, args)
                if handle_response is None:
                    vb_stream = {"id": vb_list[index],
                                 "complete": False,
                                 "keys_recvd": 0,
                                 "timed-out": vb_retry,  # Counts the amount of times that vb_stream gets timed out
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

    def handle_rollback(dcpStream, args):
        updated_dcpStreams = []
        vb = dcpStream.vbucket
        requested_rollback_no = dcpStream.rollback_seqno

        # If argument to use JSON log files
        if args.failover_logging:
            log_fetch = dcp_log_data.get_failover_logs([vb])
            if log_fetch.get(str(vb), None) is not None:  # If the failover log is not empty, use it
                data = log_fetch[str(vb)]
                failover_values = sorted(data, key=lambda x: x[1], reverse=True)
            else:
                failover_fetch = DcpClient.get_failover_log(select_dcp_client(vb), vb)
                failover_values = failover_fetch.get('value')

        # Otherwise get failover log from server
        else:
            failover_fetch = DcpClient.get_failover_log(select_dcp_client(vb), vb)
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

        print 'Retrying stream add on vb', vb, 'with seqs', new_seq_no, 'and uuids', new_uuid
        # Input new Args for rollback stream request (separate to, but extending original args)
        temp_args = copy.deepcopy(args)
        temp_args.start = new_seq_no
        temp_args.uuid = new_uuid
        temp_args.vbuckets = [vb]

        # NOTE: This can cause continuous rollbacks making client side recursive dependent on failover logs.
        return add_streams(temp_args)

    def initialise_cluster_connections(args):
        init_dcp_client = initiate_connection(args)

        config_json = json.loads(DcpClient.get_config(init_dcp_client)[2])
        global vb_map
        vb_map = config_json['vBucketServerMap']['vBucketMap']

        global dcp_client_dict
        dcp_client_dict = {}

        global dcp_log_data
        if args.log_path:
            args.log_path = os.path.normpath(args.log_path)
        dcp_log_data = LogData(args.log_path, args.vbuckets, args.keep_logs)

        # TODO: Remove globals and restructure (possibly into a class) to allow for multiple
        #       instances of the client (allowing multiple bucket connections)
        node_list = []
        for index, server in enumerate(config_json['vBucketServerMap']['serverList']):
            temp_args = copy.deepcopy(args)
            host = check_valid_host(server.split(':')[0], 'Server Config Cluster Map')
            node_list.append(host)
            port = config_json['nodesExt'][index]['services'].get('kv')

            if port is not None:
                temp_args.node = '{0}:{1}'.format(host, port)
                if 'thisNode' in config_json['nodesExt'][index]:
                    dcp_client_dict[index] = {'stream': init_dcp_client,
                                              'node': temp_args.node}
                else:
                    dcp_client_dict[index] = {'stream': initiate_connection(temp_args),
                                              'node': temp_args.node}

    def select_dcp_client(vb):
        main_node_id = vb_map[vb][0]

        # TODO: Adding support if not my vbucket received to use replica node.
        # if len(vb_map[vb]) > 1:
        #     replica1_node_id = vb_map[vb][1]
        # if len(vb_map[vb]) > 2:
        #     replica2_node_id = vb_map[vb][2]
        # if len(vb_map[vb]) > 3:
        #     replica3_node_id = vb_map[vb][3]

        return dcp_client_dict[main_node_id]['stream']

    def get_node_of_dcp_client_connection(vb):
        node_id = vb_map[vb][0]
        return dcp_client_dict[node_id]['node']

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
        if self.bucket_type == 'ephemeral': return
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
                if  mode is not None:
                    if mode == '0' or mode == 'false':
                        break
            except Exception as ex:
                pass
            self.sleep(1)

