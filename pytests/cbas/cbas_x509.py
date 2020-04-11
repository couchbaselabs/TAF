from basetestcase import BaseTestCase
from security_utils.x509main import x509main
from membase.api.rest_client import RestConnection, RestHelper
import commands
import json
from remote.remote_util import RemoteMachineShellConnection
import subprocess
import copy
from lib.BucketLib.BucketOperations import BucketHelper
from rbac_utils.Rbac_ready_functions import rbac_utils

class x509tests(BaseTestCase):

    def setUp(self):
        super(x509tests, self).setUp()
        self._reset_original()
        self.ip_address = self.getLocalIPAddress()
        self.ip_address = '172.16.1.174'
        self.root_ca_path = x509main.CACERTFILEPATH + x509main.CACERTFILE
        self.client_cert_pem = x509main.CACERTFILEPATH + self.ip_address + ".pem"
        self.client_cert_key = x509main.CACERTFILEPATH + self.ip_address + ".key"
        SSLtype = "openssl"
        encryption_type = self.input.param('encryption_type',"")
        key_length=self.input.param("key_length",1024)
        #Input parameters for state, path, delimeters and prefixes
        self.client_cert_state = self.input.param("client_cert_state","disable")
        self.paths = self.input.param('paths',"subject.cn:san.dnsname:san.uri").split(":")
        self.prefixs = self.input.param('prefixs','www.cb-:us.:www.').split(":")
        self.delimeters = self.input.param('delimeter','.:.:.') .split(":")
        self.setup_once = self.input.param("setup_once",False)
        
        self.dns = self.input.param('dns',None)
        self.uri = self.input.param('uri',None)
        
        copy_servers = copy.deepcopy(self.servers)
        
        self.rbac_user = self.input.param('rbac_user',None)
        if self.rbac_user:
            self.rbac_util = rbac_utils(self.master)
            self.rbac_util._create_user_and_grant_role("ro_admin", "ro_admin")
        
        #Generate cert and pass on the client ip for cert generation
        if (self.dns is not None) or (self.uri is not None):
            x509main(self.master)._generate_cert(copy_servers,type=SSLtype,encryption=encryption_type,key_length=key_length,client_ip=self.ip_address,alt_names='non_default',dns=self.dns,uri=self.uri)
        else:
            x509main(self.master)._generate_cert(copy_servers,type=SSLtype,encryption=encryption_type,key_length=key_length,client_ip=self.ip_address)
        self.log.info(" Path is {0} - Prefixs - {1} -- Delimeters - {2}".format(self.paths, self.prefixs, self.delimeters))
        
        if (self.setup_once):
            x509main(self.master).setup_master(self.client_cert_state, self.paths, self.prefixs, self.delimeters)
            x509main().setup_cluster_nodes_ssl(self.servers)
        

        #reset the severs to ipv6 if there were ipv6
        '''
        for server in self.servers:
            if server.ip.count(':') > 0:
                    # raw ipv6? enclose in square brackets
                    server.ip = '[' + server.ip + ']'
        '''
        
        self.log.info (" list of server {0}".format(self.servers))
        self.log.info (" list of server {0}".format(copy_servers))
        
    def tearDown(self):
        print "Into Teardown"
        self._reset_original()
        shell = RemoteMachineShellConnection(x509main.SLAVE_HOST)
        shell.execute_command("rm " + x509main.CACERTFILEPATH)
        super(x509tests, self).tearDown()


    def _reset_original(self):
        self.log.info ("Reverting to original state - regenerating certificate and removing inbox folder")
        tmp_path = "/tmp/abcd.pem"
        for servers in self.servers:
            cli_command = "ssl-manage"
            remote_client = RemoteMachineShellConnection(servers)
            options = "--regenerate-cert={0}".format(tmp_path)
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, options=options,
                                                                cluster_host=servers.ip, user="Administrator",
                                                                password="password")
            x509main(servers)._delete_inbox_folder()

    def getLocalIPAddress(self):
        '''
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('couchbase.com', 0))
        return s.getsockname()[0]
        '''
        status, ipAddress = commands.getstatusoutput("ifconfig en0 | grep 'inet addr:' | cut -d: -f2 |awk '{print $1}'")
        if '1' not in ipAddress:
            status, ipAddress = commands.getstatusoutput("ifconfig eth0 | grep  -Eo 'inet (addr:)?([0-9]*\.){3}[0-9]*' | awk '{print $2}'")
        return ipAddress
    
    def check_rebalance_complete(self,rest):
        progress = None
        count = 0
        while (progress == 'running' or count < 10):
            progress = rest._rebalance_progress_status()
            self.sleep(10)
            count = count + 1
        if progress == 'none':
            return True
        else:
            return False

    def test_limited_access_user(self):
        servs_inout = self.servers[1:4]
        rest = RestConnection(self.master)
        services_in = []
        self.log.info ("list of services to be added {0}".format(self.services_in))
        
        for service in self.services_in.split("-"):
            services_in.append(service.split(":")[0])
        self.log.info ("list of services to be added after formatting {0}".format(services_in))
        
        #add nodes to the cluster
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], servs_inout, [],
                                                     services=services_in)
        self.task_manager.get_task_result(rebalance)
        
        self.sleep(20)
        
        #check for analytics services, for Vulcan check on http port
        cbas_node = self.get_nodes_from_services_map(service_type='cbas')
        if cbas_node is not None:
            helper = BucketHelper(cbas_node)
            if not helper.bucket_exists('default'):
                helper.create_bucket(bucket='default', ramQuotaMB=100)
            
            query = "'statement=create dataset default_ds on default'"
            if self.client_cert_state == 'enable':
                output = x509main()._execute_command_clientcert(cbas_node.ip,url='/analytics/service',port=18095,headers=' --data pretty=true --data-urlencode '+query,client_cert=True,curl=True,verb='POST')
            else:
                output = x509main()._execute_command_clientcert(cbas_node.ip,url='/analytics/service',port=18095,headers=' --data pretty=true --data-urlencode '+query+' -u Administrator:password ',client_cert=False,curl=True,verb='POST')
            
            self.assertEqual(json.loads(output)['status'],"fatal","Create Index Failed")
            self.assertEqual(json.loads(output)['errors'][0]['msg'],'User must have permission (cluster.analytics!select)',"Incorrect error message.")
            self.assertEqual(json.loads(output)['errors'][0]['code'],20001,"Incorrect code.")
    
    def test_incorrect_user(self):
        host = self.master
        rest = BucketHelper(self.master)
        rest.create_bucket(bucket='default', ramQuotaMB=100)
        query = "'statement=create dataset default_ds on default'"
        servs_inout = self.servers[1:4]
        
        self.log.info ("list of services to be added {0}".format(self.services_in))
        services_in = []
        for service in self.services_in.split("-"):
            services_in.append(service.split(":")[0])
        self.log.info ("list of services to be added after formatting {0}".format(services_in))
        
        #add nodes to the cluster
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], servs_inout, [],
                                                     services=services_in)
        self.task_manager.get_task_result(rebalance)
        
        self.sleep(20)
        
        cbas_node = self.get_nodes_from_services_map(service_type='cbas')
        
        output = x509main()._execute_command_clientcert(cbas_node.ip,url='/analytics/service',port=18095,headers=' --data pretty=true --data-urlencode '+query,client_cert=True,curl=True,verb='POST')
        self.assertEqual(json.loads(output)['errors'][0]['msg'],"Unauthorized user.","Incorrect user logged in successfully.")
        
    #Common test case for testing services and other parameter
    def test_add_node_with_cert_diff_services(self):
        servs_inout = self.servers[1:4]
        rest = RestConnection(self.master)
        services_in = []
        self.log.info ("list of services to be added {0}".format(self.services_in))
        
        for service in self.services_in.split("-"):
            services_in.append(service.split(":")[0])
        self.log.info ("list of services to be added after formatting {0}".format(services_in))
        
        #add nodes to the cluster
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], servs_inout, [],
                                                     services=services_in)
        self.task_manager.get_task_result(rebalance)
        
        self.sleep(20)
        
        #check for analytics services, for Vulcan check on http port
        cbas_node = self.get_nodes_from_services_map(service_type='cbas')
        if cbas_node is not None:
            self.check_analytics_service(cbas_node)
            self.check_analytics_cluster(cbas_node)
            self.check_analytics_cluster_diagnostics(cbas_node)
            self.check_analytics_node_config(cbas_node)
            self.check_analytics_node_diagnostics(cbas_node)
            self.check_analytics_cluster_cc(cbas_node)
            self.check_analytics_node_restart(cbas_node)
            self.sleep(60)
            self.check_analytics_cluster_restart(cbas_node)
            self.sleep(60)
        #check for kv service, test for /pools/default
        kv_node = self.get_nodes_from_services_map(service_type='kv')
        if kv_node is not None:
            self.check_ns_server_rest_api(kv_node)
        
    def check_ns_server_rest_api(self, host):
        helper = BucketHelper(host)
        if not helper.bucket_exists('default'):
            helper.create_bucket(bucket='default', ramQuotaMB=100)
            self.sleep(10)
            
        if self.client_cert_state == 'enable':
            output = x509main()._execute_command_clientcert(host.ip, url='/pools/default', port=18091, headers="", client_cert=True, curl=True)
        else:
            output = x509main()._execute_command_clientcert(host.ip, url='/pools/default', port=18091, headers=' -u Administrator:password ', client_cert=False, curl=True)

        output = json.loads(output)
        self.log.info ("Print output of command is {0}".format(output))
        self.assertEqual(output['rebalanceStatus'], 'none', " The Web request has failed on port 18091 ")            
        
        if self.client_cert_state == 'enable':
            output = x509main()._execute_command_clientcert(host.ip, url='/pools/default', port=18091, headers=None, client_cert=True, curl=True, verb='POST', data='memoryQuota=400')
        else:
            output = x509main()._execute_command_clientcert(host.ip, url='/pools/default', port=18091, headers=' -u Administrator:password ', client_cert=False, curl=True, verb='POST', data='memoryQuota=400')
        
        if output == "":
            self.assertTrue(True, "Issue with post on /pools/default")
        
        output = x509main()._execute_command_clientcert(host.ip, url='/pools/default', port=8091, headers=" -u Administrator:password ", client_cert=False, curl=True, verb='GET', plain_curl=True)
        self.assertEqual(json.loads(output)['rebalanceStatus'], 'none', " The Web request has failed on port 8091 ")
        
        output = x509main()._execute_command_clientcert(host.ip, url='/pools/default', port=8091, headers=" -u Administrator:password ", client_cert=True, curl=True, verb='POST', plain_curl=True, data='memoryQuota=400')
        if output == "":
            self.assertTrue(True, "Issue with post on /pools/default")


    #Check for analytics service api, right now SSL is not supported for Analytics, hence http port
    def check_analytics_service(self,host):
        helper = BucketHelper(host)
        if not helper.bucket_exists('default'):
            helper.create_bucket(bucket='default', ramQuotaMB=100)
        
        query = "'statement=create dataset default_ds on default'"
        if self.client_cert_state == 'enable':
            output = x509main()._execute_command_clientcert(host.ip,url='/analytics/service',port=18095,headers=' --data pretty=true --data-urlencode '+query,client_cert=True,curl=True,verb='POST')
        else:
            output = x509main()._execute_command_clientcert(host.ip,url='/analytics/service',port=18095,headers=' --data pretty=true --data-urlencode '+query+' -u Administrator:password ',client_cert=False,curl=True,verb='POST')
        
        self.assertEqual(json.loads(output)['status'],"success","Create Index Failed")

        query = "'statement=create dataset default_ds1 on default'"
        if self.client_cert_state == 'enable':
            output = x509main()._execute_command_clientcert(host.ip,url='/analytics/service',port=8095,headers=' --data pretty=true --data-urlencode '+query+' -u Administrator:password ',client_cert=True,curl=True,verb='POST',plain_curl=True)
        else:
            output = x509main()._execute_command_clientcert(host.ip,url='/analytics/service',port=8095,headers=' --data pretty=true --data-urlencode '+query+' -u Administrator:password ',client_cert=False,curl=True,verb='POST',plain_curl=True)
        
        self.assertEqual(json.loads(output)['status'],"success","Create Index Failed")
        
    def check_analytics_cluster(self,host):
        helper = BucketHelper(host)
        if not helper.bucket_exists('default'):
            helper.create_bucket(bucket='default', ramQuotaMB=100)

        if self.client_cert_state == 'enable':
            output = x509main()._execute_command_clientcert(host.ip,url='/analytics/cluster',port=18095,headers='',client_cert=True,curl=True)
        else:
            output = x509main()._execute_command_clientcert(host.ip,url='/analytics/cluster',port=18095,headers=' -u Administrator:password ',client_cert=False,curl=True)
        
        self.assertEqual(json.loads(output)['state'],"ACTIVE","Create Index Failed")
        
    def check_analytics_cluster_diagnostics(self,host):
        helper = BucketHelper(host)
        if not helper.bucket_exists('default'):
            helper.create_bucket(bucket='default', ramQuotaMB=100)
        
        if self.client_cert_state == 'enable':
            output = x509main()._execute_command_clientcert(host.ip,url='/analytics/cluster/diagnostics',port=18095,headers='',client_cert=True,curl=True)
        else:
            output = x509main()._execute_command_clientcert(host.ip,url='/analytics/cluster/diagnostics',port=18095,headers=' -u Administrator:password ',client_cert=False,curl=True)
                        
        self.assertTrue(json.loads(output)['nodes'],"Cannot execute command on port 18095")

    def check_analytics_node_config(self,host):
        helper = BucketHelper(host)
        if not helper.bucket_exists('default'):
            helper.create_bucket(bucket='default', ramQuotaMB=100)

        if self.client_cert_state == 'enable':
            output = x509main()._execute_command_clientcert(host.ip,url='/analytics/node/config',port=18095,headers='',client_cert=True,curl=True)
        else:
            output = x509main()._execute_command_clientcert(host.ip,url='/analytics/node/config',port=18095,headers=' -u Administrator:password ',client_cert=False,curl=True)
                                    
        self.assertTrue(json.loads(output)['analyticsCcHttpPort'],"Cannot execute command on port 18095")
                
    def check_analytics_node_diagnostics(self,host):
        helper = BucketHelper(host)
        if not helper.bucket_exists('default'):
            helper.create_bucket(bucket='default', ramQuotaMB=100)

        if self.client_cert_state == 'enable':
            output = x509main()._execute_command_clientcert(host.ip,url='/analytics/node/diagnostics',port=18095,headers='',client_cert=True,curl=True)
        else:
            output = x509main()._execute_command_clientcert(host.ip,url='/analytics/node/diagnostics',port=18095,headers=' -u Administrator:password ',client_cert=False,curl=True)

        self.assertTrue(json.loads(output)['date'],"Cannot execute command on port 18095")
    
    def check_analytics_node_restart(self,host):
        helper = BucketHelper(host)
        if not helper.bucket_exists('default'):
            helper.create_bucket(bucket='default', ramQuotaMB=100)
                
        if self.client_cert_state == 'enable':
            output = x509main()._execute_command_clientcert(host.ip,url='/analytics/node/restart',port=18095,headers='',client_cert=True,curl=True,verb='POST')
        else:
            output = x509main()._execute_command_clientcert(host.ip,url='/analytics/node/restart',port=18095,headers=' -u Administrator:password ',client_cert=False,curl=True,verb='POST')

        self.assertEqual(json.loads(output)['status'],"restarting node","/analytics/node/restart API failed")
        
    def check_analytics_cluster_restart(self,host):
        helper = BucketHelper(host)
        if not helper.bucket_exists('default'):
            helper.create_bucket(bucket='default', ramQuotaMB=100)

        if self.client_cert_state == 'enable':
            output = x509main()._execute_command_clientcert(host.ip,url='/analytics/cluster/restart',port=18095,headers='',client_cert=True,curl=True,verb='POST')
        else:
            output = x509main()._execute_command_clientcert(host.ip,url='/analytics/cluster/restart',port=18095,headers=' -u Administrator:password ',client_cert=False,curl=True,verb='POST')

        self.assertEqual(json.loads(output)['status'],"SHUTTING_DOWN","/analytics/cluster/restart API failed")
            
    def check_analytics_cluster_cc(self,host):
        helper = BucketHelper(host)
        if not helper.bucket_exists('default'):
            helper.create_bucket(bucket='default', ramQuotaMB=100)

        if self.client_cert_state == 'enable':
            output = x509main()._execute_command_clientcert(host.ip,url='/analytics/cluster/cc',port=18095,headers='',client_cert=True,curl=True)
        else:
            output = x509main()._execute_command_clientcert(host.ip,url='/analytics/cluster/cc',port=18095,headers=' -u Administrator:password ',client_cert=False,curl=True)

        self.assertTrue(json.loads(output)['configUri'],"Cannot execute command on port 18095")

        if self.client_cert_state == 'enable':
            output = x509main()._execute_command_clientcert(host.ip,url='/analytics/cluster/cc/config',port=18095,headers='',client_cert=True,curl=True)
        else:
            output = x509main()._execute_command_clientcert(host.ip,url='/analytics/cluster/cc/config',port=18095,headers=' -u Administrator:password ',client_cert=False,curl=True)

        self.assertTrue(json.loads(output)['os_name'],"Cannot execute command on port 18095")

        if self.client_cert_state == 'enable':
            output = x509main()._execute_command_clientcert(host.ip,url='/analytics/cluster/cc/stats',port=18095,headers='',client_cert=True,curl=True)
        else:
            output = x509main()._execute_command_clientcert(host.ip,url='/analytics/cluster/cc/stats',port=18095,headers=' -u Administrator:password ',client_cert=False,curl=True)

        self.assertTrue(json.loads(output)['thread_count'],"Cannot execute command on port 18095")
        
        if self.client_cert_state == 'enable':
            output = x509main()._execute_command_clientcert(host.ip,url='/analytics/cluster/cc/threaddump',port=18095,headers='',client_cert=True,curl=True)
        else:
            output = x509main()._execute_command_clientcert(host.ip,url='/analytics/cluster/cc/threaddump',port=18095,headers=' -u Administrator:password ',client_cert=False,curl=True)

        self.assertTrue(json.loads(output)['date'],"Cannot execute command on port 18095")
