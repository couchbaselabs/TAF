import time
from random import shuffle

from cb_constants import constants
from cb_server_rest_util.buckets.buckets_api import BucketRestApi
from common_lib import sleep
from global_vars import logger
import global_vars
from custom_exceptions.exception import \
    ServerAlreadyJoinedException, RebalanceFailedException, \
    InvalidArgumentException, ServerSelfJoinException, \
    AddNodeException
from membase.api.rest_client import RestConnection
from memcached.helper.data_helper import MemcachedClientHelper, \
                                         VBucketAwareMemcached
from mc_bin_client import MemcachedClient


class RebalanceHelper(object):
    @staticmethod
    # bucket is a json object that contains name,port,password
    def wait_for_mc_stats_all_nodes(master, bucket, stat_key, stat_value,
                                    timeout_in_seconds=120, verbose=True):
        log = logger.get("infra")
        log.info("waiting for bucket {0} stat : {1} to match {2} on {3}"
                 .format(bucket, stat_key, stat_value, master.ip))
        time_to_timeout = 0
        previous_stat_value = -1
        curr_stat_value = -1
        verified = False
        all_stats = {}
        while not verified:
            rest = RestConnection(master)
            nodes = rest.node_statuses()
            for node in nodes:
                _server = {"ip": node.ip, "port": node.port,
                           "username": master.rest_username,
                           "password": master.rest_password}
                # Failed over node is part of node_statuses but since
                # its failed over memcached connections to this node will fail
                node_self = RestConnection(_server).get_nodes_self()
                if node_self.clusterMembership == 'active':
                    mc = MemcachedClientHelper.direct_client(_server, bucket)
                    n_stats = mc.stats("")
                    mc.close()
                    all_stats[node.id] = n_stats
            actual_stat_value = -1
            for k in all_stats:
                if all_stats[k] and stat_key in all_stats[k]:
                    if actual_stat_value == -1:
                        log.info(all_stats[k][stat_key])
                        actual_stat_value = int(all_stats[k][stat_key])
                    else:
                        actual_stat_value += int(all_stats[k][stat_key])
            if actual_stat_value == stat_value:
                log.info("{0} : {1}".format(stat_key, actual_stat_value))
                verified = True
                break
            else:
                if verbose:
                    log.info("{0} : {1}".format(stat_key, actual_stat_value))
                curr_stat_value = actual_stat_value

                # values are changing so clear any timeout
                if curr_stat_value != previous_stat_value:
                    time_to_timeout = 0
                else:
                    if time_to_timeout == 0:
                        time_to_timeout = time.time() + timeout_in_seconds
                    if time_to_timeout < time.time():
                        log.info("no change in {0} stat after {1} seconds (value = {2})".format(stat_key, timeout_in_seconds, curr_stat_value))
                        break

                previous_stat_value = curr_stat_value

                sleep_time = 2
                if not verbose:
                    sleep_time = 0.1
                sleep(sleep_time)
        return verified

    @staticmethod
    # bucket is a json object that contains name,port,password
    def wait_for_stats(master, bucket, stat_key, stat_value, timeout_in_seconds=120, verbose=True):
        log = logger.get("infra")
        log.info("waiting for bucket {0} stat : {1} to match {2} on {3}"
                 .format(bucket, stat_key, stat_value, master.ip))
        time_to_timeout = 0
        previous_stat_value = -1
        curr_stat_value = -1
        verified = False
        while not verified:
            rest = BucketRestApi(master)
            try:
                stats = rest.get_bucket_stats(bucket)
                if stats and stat_key in stats and stats[stat_key] == stat_value:
                    log.info("{0} : {1}".format(stat_key, stats[stat_key]))
                    verified = True
                    break
                else:
                    if stats and stat_key in stats:
                        if verbose:
                            log.info("{0} : {1}".format(stat_key, stats[stat_key]))
                        curr_stat_value = stats[stat_key]

                    # values are changing so clear any timeout
                    if curr_stat_value != previous_stat_value:
                        time_to_timeout = 0
                    else:
                        if time_to_timeout == 0:
                            time_to_timeout = time.time() + timeout_in_seconds
                        if time_to_timeout < time.time():
                            log.info("no change in {0} stat after {1} seconds (value = {2})".format(stat_key, timeout_in_seconds, curr_stat_value))
                            break

                    previous_stat_value = curr_stat_value

                    sleep_time = 2
                    if not verbose:
                        sleep_time = 0.1
                    sleep(sleep_time)
            except:
                log.info("unable to collect stats from server {0}".format(master))
                # TODO: throw ex and assume caller catches
                verified = True
                break
            sleep(5, "Wait before next stats check", log_type="infra")

        return verified

    @staticmethod
    def wait_for_stats_no_timeout(master, bucket, stat_key, stat_value,
                                  timeout_in_seconds=-1, verbose=True):
        log = logger.get("infra")
        log.info("Waiting for bucket {0} stat: {1} to match {2} on {3}"
                 .format(bucket, stat_key, stat_value, master.ip))
        rest = BucketRestApi(master)
        stats = rest.get_bucket_stats(bucket)

        while stats.get(stat_key, -1) != stat_value:
            stats = rest.get_bucket_stats(bucket)
            if verbose:
                log.info("{0} : {1}".format(stat_key, stats.get(stat_key, -1)))
            sleep(5, log_type="infra")
        return True

    @staticmethod
    # Bucket is a json object that contains name,port,password
    def wait_for_mc_stats(master, bucket, stat_key, stat_value,
                          timeout_in_seconds=120, verbose=True):
        log = logger.get("infra")
        log.info("Waiting for bucket {0} stat: {1} to match {2} on {3}"
                 .format(bucket, stat_key, stat_value, master.ip))
        start = time.time()
        verified = False
        while (time.time() - start) <= timeout_in_seconds:
            c = MemcachedClient(master.ip, constants.memcached_port)
            stats = c.stats()
            c.close()
            if stats and stat_key in stats \
                    and str(stats[stat_key]) == str(stat_value):
                log.info("{0} : {1}".format(stat_key, stats[stat_key]))
                verified = True
                break
            else:
                if stats and stat_key in stats:
                    if verbose:
                        log.info("{0} : {1}".format(stat_key, stats[stat_key]))
                sleep_time = 2
                if not verbose:
                    sleep_time = 0.1
                sleep(sleep_time, log_type="infra")
        return verified

    @staticmethod
    def wait_for_mc_stats_no_timeout(master, bucket, stat_key, stat_value,
                                     timeout_in_seconds=-1, verbose=True):
        log = logger.get("infra")
        log.info("Waiting for bucket {0} stat : {1} to match {2} on {3}"
                 .format(bucket, stat_key, stat_value, master.ip))
        # keep retrying until reaches the server
        stats = dict()
        while not stats:
            try:
                c = MemcachedClient(master.ip, constants.memcached_port)
                c.sasl_auth_plain(bucket, '')
                stats = c.stats()
            except Exception as e:
                stats = dict()
                sleep(2, "Exception: %s. Will retry.." % str(e),
                      log_type="infra")
            finally:
                c.close()

        while str(stats[stat_key]) != str(stat_value):
            c = MemcachedClient(master.ip, constants.memcached_port)
            c.sasl_auth_plain(bucket, '')
            stats = c.stats()
            c.close()
            if verbose:
                log.info("{0} : {1}".format(stat_key, stats[stat_key]))
            sleep(5, log_type="infra")
        return True

    @staticmethod
    # bucket is a json object that contains name,port,password
    def wait_for_stats_int_value(master, bucket, stat_key, stat_value,
                                 option="==", timeout_in_seconds=120,
                                 verbose=True):
        log = logger.get("infra")
        log.info("waiting for bucket {0} stat : {1} to {2} {3} on {4}"
                 .format(bucket, stat_key, option, stat_value, master.ip))
        start = time.time()
        verified = False
        while (time.time() - start) <= timeout_in_seconds:
            rest = BucketRestApi(master)
            stats = rest.get_bucket_stats(bucket)
            # some stats are in memcached
            if stats and stat_key in stats:
                actual = int(stats[stat_key])
                if option == "==":
                    verified = stat_value == actual
                elif option == ">":
                    verified = stat_value > actual
                elif option == "<":
                    verified = stat_value < actual
                elif option == ">=":
                    verified = stat_value >= actual
                elif option == "<=":
                    verified = stat_value <= actual
                if verified:
                    log.info("verified {0} : {1}".format(stat_key, actual))
                    break
                if verbose:
                    log.info("{0} : {1} isn't {2} {3}"
                             .format(stat_key, stat_value, option, actual))
            sleep(2, log_type="infra")
        return verified

    @staticmethod
    # bucket is a json object that contains name,port,password
    def wait_for_stats_on_all(master, bucket, stat_key, stat_value,
                              timeout_in_seconds=120, fn=None):
        log = logger.get("infra")
        fn = fn or RebalanceHelper.wait_for_stats
        rest = RestConnection(master)
        servers = rest.get_nodes()
        verified = False
        start_time = time.time()
        for server in servers:
            verified = fn(server, bucket, stat_key, stat_value, \
                          timeout_in_seconds=timeout_in_seconds)
            if not verified:
                log.info("bucket {0}: stat_key {1} for server {2} timed out in {3}".format(bucket, stat_key, \
                                                                                           server.ip, time.time() - start_time))
                break

        return verified

    @staticmethod
    def wait_for_persistence(master, bucket, bucket_type='memcache', timeout=120):
        if bucket_type == 'ephemeral':
            return True
        verified = True
        verified &= RebalanceHelper.wait_for_mc_stats_all_nodes(
            master, bucket, "ep_queue_size", 0,
            timeout_in_seconds=timeout)
        verified &= RebalanceHelper.wait_for_mc_stats_all_nodes(
            master, bucket, "ep_flusher_todo", 0,
            timeout_in_seconds=timeout)
        verified &= RebalanceHelper.wait_for_mc_stats_all_nodes(
            master, bucket, "ep_uncommitted_items", 0,
            timeout_in_seconds=timeout)
        return verified

    @staticmethod
    #TODO: add password and port
    def print_taps_from_all_nodes(rest, bucket='default'):
        #get the port number from rest ?

        log = logger.get("infra")
        nodes_for_stats = rest.get_nodes()
        for node_for_stat in nodes_for_stats:
            try:
                client = MemcachedClientHelper.direct_client(node_for_stat, bucket)
                log.info("getting tap stats... for {0}".format(node_for_stat.ip))
                tap_stats = client.stats('tap')
                if tap_stats:
                    RebalanceHelper.log_interesting_taps(node_for_stat, tap_stats, log)
                client.close()
            except Exception as ex:
                log.error("error {0} while getting stats...".format(ex))

    @staticmethod
    def log_interesting_taps(node, tap_stats, log):
        interesting_stats = ['ack_log_size', 'ack_seqno', 'ack_window_full',
                             'has_item', 'has_queued_item',
                             'idle', 'paused', 'backfill_completed',
                             'pending_backfill', 'pending_disk_backfill',
                             'recv_ack_seqno', 'ep_num_new_']
        for name in tap_stats:
            for interesting_stat in interesting_stats:
                if name.find(interesting_stat) != -1:
                    log.info("TAP {0}: {1} {2}"
                                .format(node.id, name, tap_stats[name]))
                    break

    @staticmethod
    def verify_maps(vbucket_map_before, vbucket_map_after):
        #for each bucket check the replicas
        log = logger.get("infra")
        for i in range(0, len(vbucket_map_before)):
            if not vbucket_map_before[i].master == vbucket_map_after[i].master:
                log.error(
                    'vbucket[{0}].master mismatch {1} vs {2}'.format(i, vbucket_map_before[i].master,
                                                                     vbucket_map_after[i].master))
                return False
            for j in range(0, len(vbucket_map_before[i].replica)):
                if not (vbucket_map_before[i].replica[j]) == (vbucket_map_after[i].replica[j]):
                    log.error('vbucket[{0}].replica[{1} mismatch {2} vs {3}'.format(i, j,
                                                                                    vbucket_map_before[i].replica[j],
                                                                                    vbucket_map_after[i].replica[j]))
                    return False
        return True

    #read the current nodes
    # if the node_ip already added then just
    #silently return
    #if its not added then let try to add this and then rebalance
    #we should alo try to get the bucket information from
    #rest api instead of passing it to the fucntions

    @staticmethod
    def rebalance_in(servers, how_many, do_shuffle=True, monitor=True, do_check=True, validate_bucket_ranking=True):
        log = logger.get("infra")
        servers_rebalanced = []
        rest = RestConnection(servers[0])
        nodes = rest.node_statuses()
        # are all ips the same
        nodes_on_same_ip = True
        firstIp = nodes[0].ip
        if len(nodes) == 1:
            nodes_on_same_ip = False
        else:
            for node in nodes:
                if node.ip != firstIp:
                    nodes_on_same_ip = False
                    break
        nodeIps = ["{0}:{1}".format(node.ip, node.port) for node in nodes]
        log.info("current nodes : {0}".format(nodeIps))
        toBeAdded = []
        master = servers[0]
        selection = servers[1:]
        if do_shuffle:
            shuffle(selection)
        for server in selection:
            if nodes_on_same_ip:
                if not "{0}:{1}".format(firstIp, server.port) in nodeIps:
                    toBeAdded.append(server)
                    servers_rebalanced.append(server)
                    log.info("choosing {0}:{1}".format(server.ip, server.port))
            elif not "{0}:{1}".format(server.ip, server.port) in nodeIps:
                toBeAdded.append(server)
                servers_rebalanced.append(server)
                log.info("choosing {0}:{1}".format(server.ip, server.port))
            if len(toBeAdded) == int(how_many):
                break

        if do_check and len(toBeAdded) < how_many:
            raise Exception("unable to find {0} nodes to rebalance_in".format(how_many))

        for server in toBeAdded:
            otpNode = rest.add_node(master.rest_username, master.rest_password,
                                    server.ip, server.port)
        otpNodes = [node.id for node in rest.node_statuses()]
        started, _ = rest.rebalance(otpNodes, [])
        msg = "rebalance operation started ? {0}"
        log.info(msg.format(started))
        if monitor is not True:
            return True, servers_rebalanced
        if started:
            try:
                result = rest.monitorRebalance()
            except RebalanceFailedException as e:
                log.error("rebalance failed: {0}".format(e))
                return False, servers_rebalanced
            if validate_bucket_ranking:
                # Validating bucket ranking post rebalance
                validate_ranking_res = global_vars.cluster_util.validate_bucket_ranking(None, servers[0])
                if not validate_ranking_res:
                    log.error("Vbucket movement during rebalance did not occur as per bucket ranking")
                    return False, servers_rebalanced

            msg = "successfully rebalanced in selected nodes from the cluster ? {0}"
            log.info(msg.format(result))
            return result, servers_rebalanced
        return False, servers_rebalanced

    @staticmethod
    def rebalance_out(servers, how_many, monitor=True, validate_bucket_ranking=True):
        log = logger.get("infra")
        rest = RestConnection(servers[0])
        cur_ips = map(lambda node: node.ip, rest.node_statuses())
        servers = filter(lambda server: server.ip in cur_ips, servers) or servers
        if len(cur_ips) <= how_many or how_many < 1:
            log.error("failed to rebalance %s servers out: not enough servers"
                      % how_many)
            return False, []

        ejections = servers[1:how_many + 1]

        log.info("rebalancing out %s" % ejections)
        RebalanceHelper.begin_rebalance_out(servers[0], ejections)

        if not monitor:
            return True, ejections
        try:
            result = rest.monitorRebalance()
            if not result:
                return result, ejections

            if validate_bucket_ranking:
                # Validating bucket ranking post rebalance
                validate_ranking_res = global_vars.cluster_util.validate_bucket_ranking(None, servers[0])
                result = result and validate_ranking_res
                if not validate_ranking_res:
                    log.error("Vbucket movement during rebalance did not occur as per bucket ranking")
            return result, ejections

        except RebalanceFailedException as e:
            log.error("failed to rebalance %s servers out: %s" % (how_many, e))
            return False, ejections

    @staticmethod
    def rebalance_swap(servers, how_many, monitor=True, validate_bucket_ranking=True):
        log = logger.get("infra")
        if how_many < 1:
            log.error("failed to swap rebalance %s servers - invalid count"
                      % how_many)
            return False, []

        rest = RestConnection(servers[0])
        cur_nodes = rest.node_statuses()
        cur_ips = map(lambda node: node.ip, cur_nodes)
        cur_ids = map(lambda node: node.id, cur_nodes)
        free_servers = filter(lambda server: server.ip not in cur_ips, servers)

        if len(cur_ids) <= how_many or len(free_servers) < how_many:
            log.error("failed to swap rebalance %s servers - not enough servers"
                      % how_many)
            return False, []

        ejections = cur_ids[-how_many:]
        additions = free_servers[:how_many]

        log.info("swap rebalance: cur: %s, eject: %s, add: %s"
                 % (cur_ids, ejections, additions))

        try:
            map(lambda server: rest.add_node(servers[0].rest_username,
                                             servers[0].rest_password,
                                             server.ip, server.port), additions)
        except (ServerAlreadyJoinedException,
                ServerSelfJoinException, AddNodeException) as e:
            log.error("failed to swap rebalance - addition failed %s: %s"
                      % (additions, e))
            return False, []

        cur_ids = map(lambda node: node.id, rest.node_statuses())
        try:
            rest.rebalance(otpNodes=cur_ids, ejectedNodes=ejections)
        except InvalidArgumentException as e:
            log.error("failed to swap rebalance - rebalance failed :%s" % e)
            return False, []

        if not monitor:
            return True, ejections + additions

        try:
            result = rest.monitorRebalance()
            if not result:
                return result, ejections + additions

            if validate_bucket_ranking:
                # Validating bucket ranking post rebalance
                validate_ranking_res = global_vars.cluster_util.validate_bucket_ranking(None, servers[0])
                result = result and validate_ranking_res
                if not validate_ranking_res:
                    log.error("Vbucket movement during rebalance did not occur as per bucket ranking")
            return result, ejections + additions

        except RebalanceFailedException as e:
            log.error("failed to swap rebalance %s servers: %s" % (how_many, e))
            return False, ejections + additions

    @staticmethod
    def begin_rebalance_in(master, servers, timeout=5):
        log = logger.get("infra")
        rest = RestConnection(master)
        otpNode = None

        for server in servers:
            if server == master:
                continue
            log.info("adding node {0}:{1} to cluster".format(server.ip, server.port))
            try:
                otpNode = rest.add_node(master.rest_username, master.rest_password, server.ip, server.port)
                msg = "unable to add node {0}:{1} to the cluster"
                assert otpNode, msg.format(server.ip, server.port)
            except ServerAlreadyJoinedException:
                log.info("server {0} already joined".format(server))
        log.info("beginning rebalance in")
        try:
            rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()], ejectedNodes=[])
        except:
            log.error("rebalance failed, trying again after {0} seconds".format(timeout))

    @staticmethod
    def begin_rebalance_out(master, servers, timeout=5):
        log = logger.get("infra")
        rest = RestConnection(master)
        master_node = rest.get_nodes_self()

        ejectedNodes = []
        nodes = rest.node_statuses()
        for server in servers:
            server_node = RestConnection(server).get_nodes_self()
            if server_node == master_node:
                continue
            log.info("removing node {0}:{1} from cluster".format(server_node.ip, server_node.port))
            for node in nodes:
                if "{0}:{1}".format(node.ip, node.port) == "{0}:{1}".format(server_node.ip, server_node.port):
                    ejectedNodes.append(node.id)
        log.info("beginning rebalance out")
        try:
            rest.rebalance(otpNodes=[node.id for node in nodes],
                           ejectedNodes=ejectedNodes)
        except:
            log.error("rebalance failed, trying again after {0} seconds".format(timeout))

    @staticmethod
    def end_rebalance(master, validate_bucket_ranking=True):
        log = logger.get("infra")
        rest = RestConnection(master)
        result = False
        try:
            result = rest.monitorRebalance()
        except RebalanceFailedException as e:
            log.error("rebalance failed: {0}".format(e))
        assert result, "rebalance operation failed after adding nodes"
        log.info("rebalance finished")

        if validate_bucket_ranking:
            # Validating bucket ranking post rebalance
            validate_ranking_res = global_vars.cluster_util.validate_bucket_ranking(None, master)
            assert validate_ranking_res, "Vbucket movement during rebalance did not occur as per bucket ranking"

    @staticmethod
    def getOtpNodeIds(master):
        rest = RestConnection(master)
        nodes = rest.node_statuses()
        otpNodeIds = [node.id for node in nodes]
        return otpNodeIds

    @staticmethod
    def verify_vBuckets_info(master, bucket="default"):
        '''
        verify vBuckets' state and items count(for active/replica) in them related to vBucketMap for all nodes in cluster
        '''
        log = logger.get("infra")
        awareness = VBucketAwareMemcached(RestConnection(master), bucket)
        vb_map = awareness.vBucketMap
        vb_mapReplica = awareness.vBucketMapReplica
        replica_num = len(vb_mapReplica[0])

        #get state and count items for all vbuckets for each node
        node_stats = RebalanceHelper.get_vBuckets_info(master)
        state = True
        #iterate throught all vbuckets by their numbers
        for num in vb_map:
            #verify that active vbucket in memcached  is also active in stats("hash)
            if(node_stats[vb_map[num]]["vb_" + str(num)][0] != "active"):
                log.info("vBucket {0} in {1} node has wrong state {3}".format("vb_" + str(num), vb_map[num], node_stats[vb_map[num]]["vb_" + str(num)]));
                state = False
            #number of active items for num vBucket
            vb = node_stats[vb_map[num]]["vb_" + str(num)][1]
            active_vb = vb_map[num]
            #list of nodes for wich num vBucket is replica
            replica_vbs = vb_mapReplica[key]
            sum_items_replica = 0
            #sum of replica items for all nodes for num vBucket
            for i in range(replica_num):
                if(node_stats[vb_mapReplica[num][i]]["vb_" + str(num)][0] != "replica"):
                    log.info("vBucket {0} in {1} node has wrong state {3}".format("vb_" + str(num), vb_mapReplica[num], node_stats[vb_mapReplica[num]]["vb_" + str(num)]));
                    state = False
                sum_items_replica += int(node_stats[replica_vbs[i]]["vb_" + str(num)][1])
            #print information about the discrepancy of the number of replica and active items for num vBucket
            if (int(vb) * len(vb_mapReplica[num]) != sum_items_replica):
                log.info("sum of active items doesn't correspond to replica's vBucets in {0} vBucket:".format("vb_" + str(num)))
                log.info("items in active vBucket {0}:{1}".format(vb_map[num], node_stats[vb_map[num]]["vb_" + str(num)]))
                for j in range(replica):
                    log.info("items in replica vBucket {0}: {1}".format(vb_mapReplica[num][j], node_stats[vb_mapReplica[num][j]]["vb_" + str(num)]))
                    log.info(node_stats[vb_mapReplica[num][0]])
                state = False

        if not state:
            log.error("Something is wrong, see log above. See details:")
            log.error("vBucetMap: {0}".format(vb_map))
            log.error("vBucetReplicaMap: {0}".format(vb_mapReplica))
            log.error("node_stats: {0}".format(node_stats))
        return state

    @staticmethod
    def get_vBuckets_info(master):
        """
        return state and count items for all vbuckets for each node
        format: dict: {u'1node_ip1': {'vb_79': ['replica', '0'], 'vb_78': ['active', '0']..}, u'1node_ip1':....}
        """
        log = logger.get("infra")
        rest = RestConnection(master)
        port = rest.get_nodes_self().memcached
        nodes = rest.node_statuses()
        _nodes_stats = {}
        for node in nodes:
            stat = {}
            buckets = []
            _server = {"ip": node.ip, "port": node.port,
                       "username": master.rest_username,
                       "password": master.rest_password}
            try:
                buckets = rest.get_buckets()
                mc = MemcachedClient(node.ip, port)
                stat_hash = mc.stats("hash")
            except Exception:
                if not buckets:
                    log.error("There are not any buckets in {0}:{1} node"
                              .format(node.ip, node.port))
                else:
                    log.error("Impossible to get vBucket's information for {0}:{1} node"
                              .format(node.ip, node.port))
                    _nodes_stats[node.ip + ":" + str(node.port)]
                continue
            mc.close()
            vb_names = [key[:key.index(":")] for key in stat_hash.keys()]

            for name in vb_names:
                stat[name] = [stat_hash[name + ":state"], stat_hash[name + ":counted"]]
            _nodes_stats[node.ip + ":" + str(port)] = stat
        log.info(_nodes_stats)
        return _nodes_stats
