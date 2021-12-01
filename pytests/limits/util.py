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
    shell = RemoteMachineShellConnection(node)
    vbuckets = set(Cbstats(shell).vbucket_list(bucket_name, vbucket_type))
    shell.disconnect()
    return vbuckets


def key_for_node(node, bucket_name):
    vbuckets = vbuckets_on_node(node, bucket_name)
    for i in range(10000):
        key = "key{}".format(i)
        if BucketUtils.get_vbucket_num_for_key(key) in vbuckets:
            return key
    raise RuntimeError('A key that belongs to this node could not be found.')
