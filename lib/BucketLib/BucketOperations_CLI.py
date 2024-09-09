"""
Created on Sep 27, 2017

@author: riteshagarwal
"""
from cb_tools.cb_cli import CbCli
from BucketOperations_Rest import BucketHelper as BucketHelperRest
from bucket import Bucket
from shell_util.remote_connection import RemoteMachineShellConnection


class BucketHelper(BucketHelperRest):
    def __init__(self, server, username="Administrator", password="password"):
        super(BucketHelper, self).__init__(server)
        self.server = server
        self.username = username
        self.password = password
        self.cb_cli = None

    def __use_shell(self, cli_function):
        def wrapper():
            shell = RemoteMachineShellConnection(self.server)
            self.cb_cli = CbCli(shell, self.username, self.password)
            try:
                output = cli_function()
            except Exception as e:
                shell.disconnect()
                raise e
            shell.disconnect()
            return output
        return wrapper

    @__use_shell
    def create_bucket(self, bucket_params={}):
        return self.cb_cli.create_bucket(bucket_params)

    @__use_shell
    def delete_bucket(self, bucket_name="default"):
        return self.cb_cli.delete_bucket(bucket_name)

    @__use_shell
    def flush_bucket(self, bucket_name="default"):
        return self.cb_cli.flush_bucket(bucket_name, force=True)

    @__use_shell
    def change_bucket_props(self, bucket, ramQuotaMB=None,
                            replicaNumber=None, proxyPort=None,
                            replicaIndex=None, flushEnabled=None,
                            bucket_rank=None,
                            timeSynchronization=None, maxTTL=None,
                            compressionMode=None, bucket_durability=None,
                            bucketWidth=None, bucketWeight=None):
        bucket_params = dict()
        bucket_params["name"] = "%s" % bucket
        if ramQuotaMB:
            bucket_params["ramQuotaMB"] = ramQuotaMB
        if replicaNumber is not None:
            bucket_params["replicaNumber"] = replicaNumber
        if bucket_rank is not None:
            bucket_params[Bucket.rank] = bucket_rank
        if replicaIndex:
            bucket_params["replicaIndex"] = replicaIndex
        if flushEnabled:
            bucket_params["flushEnabled"] = flushEnabled
        if timeSynchronization:
            bucket_params["timeSynchronization"] = timeSynchronization
        if maxTTL is not None:
            bucket_params["maxTTL"] = maxTTL
        if compressionMode:
            bucket_params["compressionMode"] = compressionMode
        if bucket_durability:
            bucket_params["durabilityMinLevel"] = bucket_durability
        if bucketWidth:
            bucket_params["width"] = bucketWidth
        if bucketWeight:
            bucket_params["weight"] = bucketWeight


        return self.cb_cli.edit_bucket(bucket_params)

    @__use_shell
    def enable_n2n_encryption(self):
        return self.cb_cli.enable_n2n_encryption()

    @__use_shell
    def set_n2n_encryption_level(self, level="all"):
        return self.cb_cli.set_n2n_encryption_level(level=level)
