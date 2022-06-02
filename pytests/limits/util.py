import copy
import time

from cb_tools.cbstats import Cbstats
from bucket_utils.bucket_ready_functions import BucketUtils
from remote.remote_util import RemoteMachineShellConnection


def retry_with_timeout(timeout, f):
    """ Retries until the condition is met and returns True, otherwise returns
    False. """
    endtime = time.time() + timeout

    # TODO make the usage of sleep better here
    while time.time() < endtime:
        if f():
            return True

        time.sleep(5)

    return False


def time_it(f, *args, **kwargs):
    """ Returns time taken for a function in addition it's return value"""
    s_time = time.time()
    result = f(*args, **kwargs)
    return time.time() - s_time, result


def copy_node_for_user(user, node):
    """ Copy the ServerInfo object and change it's rest credentials to
    those of the use object. """
    nodecopy = copy.deepcopy(node)
    nodecopy.rest_username = user.username
    nodecopy.rest_password = user.password
    return nodecopy


def vbuckets_on_node(node, bucket_name, vbucket_type='active'):
    """ Returns vbuckets for a specific node """
    vbuckets = set(Cbstats(node).vbucket_list(bucket_name, vbucket_type))
    return vbuckets


def key_for_node(node, bucket_name):
    vbuckets = vbuckets_on_node(node, bucket_name)
    for i in range(10000):
        key = "key{}".format(i)
        if BucketUtils.get_vbucket_num_for_key(key) in vbuckets:
            return key
    raise RuntimeError('A key that belongs to this node could not be found.')


def apply_rebalance(task, servers, cycles=3, strategy="rebalance"):
    """ Shuffles servers in and out via a swap-rebalance or failover.
    Requires a minimum of 3 servers. """
    # Remove last server
    task.rebalance(servers, [], servers[-1:])

    # Swap rebalance a single node for several cycles.
    for _ in range(cycles):
        # Add last server and remove second-to-last server
        to_add, to_remove = servers[-1:], servers[-2:-1]

        if strategy == "rebalance":
            # Perform a swap rebalance
            task.rebalance(servers, to_add, to_remove)

        if strategy == "graceful-failover" or strategy == "hard-failover":
            # Perform a graceful-failover followed
            task.failover(servers=servers, failover_nodes=to_remove,
                          graceful=strategy == "graceful-failover")
            task.rebalance(servers, to_add, [])

        # Swap last two elements
        servers[-1], servers[-2] = servers[-2], servers[-1]

    task.rebalance(servers, to_remove, [])
