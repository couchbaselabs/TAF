"""
Created on Sep 27, 2017

@author: riteshagarwal
"""
from cb_tools.cb_cli import CbCli
from remote.remote_util import RemoteMachineShellConnection
from BucketOperations_Rest import BucketHelper as BucketHelperRest


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
    def change_bucket_props(self, bucket, ramQuotaMB=None, authType=None,
                            saslPassword=None, replicaNumber=None,
                            proxyPort=None, replicaIndex=None,
                            flushEnabled=None, timeSynchronization=None,
                            maxTTL=None, compressionMode=None,
                            bucket_durability=None):
        bucket_params = dict()
        bucket_params["name"] = "%s" % bucket
        if ramQuotaMB:
            bucket_params["ramQuotaMB"] = ramQuotaMB
        if authType:
            bucket_params["authType"] = authType
        if saslPassword:
            bucket_params["authType"] = "sasl"
            bucket_params["saslPassword"] = saslPassword
        if replicaNumber is not None:
            bucket_params["replicaNumber"] = replicaNumber
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

        return self.cb_cli.edit_bucket(bucket_params)
