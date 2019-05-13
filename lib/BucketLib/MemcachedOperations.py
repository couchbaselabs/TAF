'''
Created on Nov 3, 2017

@author: riteshagarwal
'''
import copy
import exceptions
import time
import zlib
import logging
import mc_bin_client
import crc32
import socket
import ctypes
from membase.api.rest_client import RestConnection, RestHelper
import memcacheConstants
from memcached.helper.data_helper import MemcachedClientHelper, VBucketAwareMemcached
from mc_bin_client import MemcachedClient
from threading import Thread
import Queue
from collections import defaultdict
from BucketLib.BucketOperations import BucketHelper

log = logging.getLogger()


class MemcachedHelper:
    @staticmethod
    def wait_for_vbuckets_ready_state(node, bucket, timeout_in_seconds=300, log_msg='', admin_user='cbadminbucket',
                                      admin_pass='password'):
        start_time = time.time()
        end_time = start_time + timeout_in_seconds
        ready_vbuckets = {}
        rest = RestConnection(node)
#         servers = rest.get_nodes()
        bucket_conn = BucketHelper(node)
        bucket_conn.vbucket_map_ready(bucket, 60)
        vbucket_count = len(bucket_conn.get_vbuckets(bucket))
        vbuckets = bucket_conn.get_vbuckets(bucket)
        obj = VBucketAwareMemcached(rest, bucket, info=node)
        memcacheds, vbucket_map, vbucket_map_replica = obj.request_map(rest, bucket)
        #Create dictionary with key:"ip:port" and value: a list of vbuckets
        server_dict = defaultdict(list)
        for everyID in range(0, vbucket_count):
            memcached_ip_port = str(vbucket_map[everyID])
            server_dict[memcached_ip_port].append(everyID)
        while time.time() < end_time and len(ready_vbuckets) < vbucket_count:
            for every_ip_port in server_dict:
                #Retrieve memcached ip and port
                ip, port = every_ip_port.split(":")
                client = MemcachedClient(ip, int(port), timeout=30)
                client.vbucket_count = len(vbuckets)
                bucket_info = bucket_conn.get_bucket(bucket)
                versions = rest.get_nodes_versions(logging=False)
                pre_spock = False
                for version in versions:
                    if "5" > version:
                        pre_spock = True
                if pre_spock:
                    log.info("Atleast 1 of the server is on pre-spock "
                             "version. Using the old ssl auth to connect to "
                             "bucket.")
                    client.sasl_auth_plain(
                    bucket_info.name.encode('ascii'),
                    bucket_info.saslPassword.encode('ascii'))
                else:
                    client.sasl_auth_plain(admin_user, admin_pass)
                    bucket = bucket.encode('ascii')
                    client.bucket_select(bucket)
                for i in server_dict[every_ip_port]:
                    try:
                        (a, b, c) = client.get_vbucket_state(i)
                    except mc_bin_client.MemcachedError as e:
                        ex_msg = str(e)
                        if "Not my vbucket" in log_msg:
                            log_msg = log_msg[:log_msg.find("vBucketMap") + 12] + "..."
                        if e.status == memcacheConstants.ERR_NOT_MY_VBUCKET:
                            # May receive this while waiting for vbuckets, continue and retry...S
                            continue
                        log.error("%s: %s" % (log_msg, ex_msg))
                        continue
                    except exceptions.EOFError:
                        # The client was disconnected for some reason. This can
                        # happen just after the bucket REST API is returned (before
                        # the buckets are created in each of the memcached processes.)
                        # See here for some details: http://review.couchbase.org/#/c/49781/
                        # Longer term when we don't disconnect clients in this state we
                        # should probably remove this code.
                        log.error("got disconnected from the server, reconnecting")
                        client.reconnect()
                        client.sasl_auth_plain(bucket_info.name.encode('ascii'),
                                               bucket_info.saslPassword.encode('ascii'))
                        continue

                    if c.find("\x01") > 0 or c.find("\x02") > 0:
                        ready_vbuckets[i] = True
                    elif i in ready_vbuckets:
                        log.warning("vbucket state changed from active to {0}".format(c))
                        del ready_vbuckets[i]
                client.close()
        return len(ready_vbuckets) == vbucket_count

    # try to insert key in all vbuckets before returning from this function
    # bucket { 'name' : 90,'password':,'port':1211'}
    @staticmethod
    def wait_for_memcached(node, bucket, timeout_in_seconds=300, log_msg=''):
#         time.sleep(10)
#         return True
        msg = "waiting for memcached bucket : {0} in {1} to accept set ops"
        log.info(msg.format(bucket, node.ip))
        all_vbuckets_ready = MemcachedHelper.wait_for_vbuckets_ready_state(node,
                                                                                 bucket, timeout_in_seconds, log_msg)
        # return (counter == vbucket_count) and all_vbuckets_ready
        return all_vbuckets_ready

    @staticmethod
    def verify_data(server, keys, value_equal_to_key, verify_flags, test, debug=False, bucket="default"):
        log_error_count = 0
        # verify all the keys
        client = MemcachedClientHelper.direct_client(server, bucket)
        vbucket_count = len(BucketHelper(server).get_vbuckets(bucket))
        # populate key
        index = 0
        all_verified = True
        keys_failed = []
        for key in keys:
            try:
                index += 1
                vbucketId = crc32.crc32_hash(key) & (vbucket_count - 1)
                client.vbucketId = vbucketId
                flag, keyx, value = client.get(key=key)
                if value_equal_to_key:
                    test.assertEquals(value, key, msg='values dont match')
                if verify_flags:
                    actual_flag = socket.ntohl(flag)
                    expected_flag = ctypes.c_uint32(zlib.adler32(value)).value
                    test.assertEquals(actual_flag, expected_flag, msg='flags dont match')
                if debug:
                    log.info("verified key #{0} : {1}".format(index, key))
            except mc_bin_client.MemcachedError as error:
                if debug:
                    log_error_count += 1
                    if log_error_count < 100:
                        log.error(error)
                        log.error(
                            "memcachedError : {0} - unable to get a pre-inserted key : {0}".format(error.status, key))
                keys_failed.append(key)
                all_verified = False
        client.close()
        if len(keys_failed) > 0:
            log.error('unable to verify #{0} keys'.format(len(keys_failed)))
        return all_verified

    @staticmethod
    def keys_dont_exist(server, keys, bucket):
        #verify all the keys
        client = MemcachedClientHelper.direct_client(server, bucket)
        vbucket_count = len(BucketHelper(server).get_vbuckets(bucket))
        #populate key
        for key in keys:
            try:
                vbucketId = crc32.crc32_hash(key) & (vbucket_count - 1)
                client.vbucketId = vbucketId
                client.get(key=key)
                client.close()
                log.error('key {0} should not exist in the bucket'.format(key))
                return False
            except mc_bin_client.MemcachedError as error:
                log.error(error)
                log.error("expected memcachedError : {0} - unable to get a pre-inserted key : {1}".format(error.status, key))
        client.close()
        return True

    @staticmethod
    def chunks(l, n):
        keys_chunks = {}
        index = 0
        for i in range(0, len(l), n):
            keys_chunks[index] = l[i:i + n]
            index += 1
        return keys_chunks

    @staticmethod
    def keys_exist_or_assert_in_parallel(keys, server, bucket_name, test, concurrency=2):
        verification_threads = []
        queue = Queue.Queue()
        for i in range(concurrency):
            keys_chunk = MemcachedHelper.chunks(keys, len(keys) / concurrency)
            t = Thread(target=MemcachedHelper.keys_exist_or_assert,
                       name="verification-thread-{0}".format(i),
                       args=(keys_chunk.get(i), server, bucket_name, test, queue))
            verification_threads.append(t)
        for t in verification_threads:
            t.start()
        for t in verification_threads:
            log.info("thread {0} finished".format(t.name))
            t.join()
        while not queue.empty():
            item = queue.get()
            if item is False:
                return False
        return True

    @staticmethod
    def keys_exist_or_assert(keys, server, bucket_name, test, queue=None):
        # we should try out at least three times
        # verify all the keys
        client = MemcachedClientHelper.proxy_client(server, bucket_name)
        # populate key
        retry = 1

        keys_left_to_verify = []
        keys_left_to_verify.extend(copy.deepcopy(keys))
        log_count = 0
        while retry < 6 and len(keys_left_to_verify) > 0:
            msg = "trying to verify {0} keys - attempt #{1} : {2} keys left to verify"
            log.info(msg.format(len(keys), retry, len(keys_left_to_verify)))
            keys_not_verified = []
            for key in keys_left_to_verify:
                try:
                    client.get(key=key)
                except mc_bin_client.MemcachedError as error:
                    keys_not_verified.append(key)
                    if log_count < 100:
                        log.error("key {0} does not exist because {1}".format(key, error))
                        log_count += 1
            retry += 1
            keys_left_to_verify = keys_not_verified
        if len(keys_left_to_verify) > 0:
            log_count = 0
            for key in keys_left_to_verify:
                log.error("key {0} not found".format(key))
                log_count += 1
                if log_count > 100:
                    break
            msg = "unable to verify {0} keys".format(len(keys_left_to_verify))
            log.error(msg)
            if test:
                queue.put(False)
                test.fail(msg=msg)
            if queue is None:
                return False
            else:
                queue.put(False)
        log.info("verified that {0} keys exist".format(len(keys)))
        if queue is None:
            return True
        else:
            queue.put(True)
