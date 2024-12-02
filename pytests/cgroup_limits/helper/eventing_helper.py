import json
import os
import time
from cb_server_rest_util.eventing.eventing_api import EventingRestAPI


class CGroupEventingHelper:

    def __init__(self, cluster, bucket_util, logger):
        self.cluster = cluster
        self.bucket_util = bucket_util
        self.log = logger
        self.eventing_node = self.cluster.eventing_nodes[0]
        self.log.info("Eventing node: {}".format(self.eventing_node.__dict__))

    def create_eventing_functions(self):

        self.eventing_funcs = 0
        self.eventing_func_dict = dict()
        deploy = True
        for bucket in self.cluster.buckets:
            func_name = "eventingfunc_" + bucket.name
            self.create_deploy_eventing_functions(self.cluster, bucket,
                                                func_name, deploy=deploy)
            self.eventing_func_dict[func_name] = deploy
            self.eventing_funcs += 1
        self.log.info("Wait after creating/deploying eventing functions")
        time.sleep(30)
        self.log.info("Eventing functions = {}".format(self.eventing_func_dict))
        for func_name in self.eventing_func_dict:
            status, content = EventingRestAPI(self.eventing_node).\
                                        get_function_details(func_name)
            self.log.info(f"Status = {status}, Content = {content}")

    def create_deploy_eventing_functions(self, cluster, bucket, appname,
                                         meta_coll_name="eventing_metadata",
                                         deploy=True):

        eventing_helper = EventingRestAPI(self.eventing_node)
        self.log.info("Creating collection: {} for eventing".format(meta_coll_name))
        self.bucket_util.create_collection(
            cluster.master, bucket,
            "_default", {"name": meta_coll_name})
        time.sleep(2)

        eventing_functions_dir = os.path.join(os.getcwd(),
                                "pytests/eventing/exported_functions/volume_test")
        file = "bucket-op.json"
        fh = open(os.path.join(eventing_functions_dir, file), "r")
        body = fh.read()
        json_obj = json.loads(body)
        fh.close()

        json_obj["depcfg"].pop("buckets")
        json_obj["appcode"] = "function OnUpdate(doc, meta, xattrs) {\n    " \
                    "log(\"Doc created/updated\", meta.id);\n} \n\nfunction " \
                    "OnDelete(meta, options) {\n    log(\"Doc deleted/expired\", " \
                    "meta.id);\n}"
        json_obj["depcfg"]["source_bucket"] = bucket.name
        json_obj["depcfg"]["source_scope"] = "_default"
        json_obj["depcfg"]["source_collection"] = "_default"
        json_obj["depcfg"]["metadata_bucket"] = bucket.name
        json_obj["depcfg"]["metadata_scope"] = "_default"
        json_obj["depcfg"]["metadata_collection"] = meta_coll_name
        json_obj["appname"] = appname
        self.log.info("Creating Eventing function - {}".format(json_obj["appname"]))
        status, content = eventing_helper.create_function(appname, json_obj)
        self.log.info(f"Status = {status}, Content = {content}")
        if deploy:
            time.sleep(5)
            status, content = eventing_helper.deploy_eventing_function(appname)
            self.log.info(f"Status = {status}, Content = {content}")