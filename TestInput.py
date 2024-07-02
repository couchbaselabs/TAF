import getopt
import re
import logging
import os
from configparser import ConfigParser

from builds.build_query import BuildQuery
from py_constants import constants


class TestInputSingleton:
    input = None

    def __init__(self):
        pass


class TestInput(object):
    """
    class to parse the inputs either from command line or from a ini file
    command line supports a subset of configuration which tests
    ideally should accept a regular expression
    """
    def __init__(self):
        self.servers = list()
        self.clusters = dict()
        self.membase_settings = None
        self.test_params = dict()
        self.tuq_client = dict()
        self.elastic = list()
        self.capella = dict()
        self.datasources = dict()
        # servers, each server can have u_name, passwd, port, directory

    def param(self, name, *args):
        """Returns the paramater or a default value

        The first parameter is the name of property, the second
        parameter is the default value. If not default value is given,
        an exception will be raised.
        """
        if name in self.test_params:
            return TestInput._parse_param(self.test_params[name])
        elif len(args) == 1:
            return args[0]
        else:
            raise Exception("Parameter `{}` must be set "
                            "in the test configuration".format(name))

    @staticmethod
    def _parse_param(value):
        try:
            return int(value)
        except ValueError:
            pass

        try:
            return float(value)
        except ValueError:
            pass

        if value.lower() == "false":
            return False

        if value.lower() == "true":
            return True

        return value


class TestInputServer(object):
    def __init__(self):
        self.ip = ''
        self.id = ''
        self.hostname = ''
        self.ssh_username = ''
        self.ssh_password = ''
        self.ssh_key = ''
        self.rest_username = ''
        self.rest_password = ''
        self.services = ''
        self.port = ''
        self.memcached_port = constants.memcached_port
        self.cli_path = ''
        self.data_path = ''
        self.index_path = ''
        self.cbas_path = ''
        self.eventing_path = ''
        self.n1ql_port = ''
        self.index_port = ''
        self.fts_port = ''
        self.es_username = ''
        self.es_password = ''
        self.upgraded = False
        self.remote_info = None
        self.use_sudo = False
        self.type = "default"

    def __str__(self):
        ip_str = "ip:{0} port:{1}".format(self.ip, self.port)
        ssh_username_str = "ssh_username:{0}".format(self.ssh_username)
        return "{0} {1}".format(ip_str, ssh_username_str)

    def __repr__(self):
        ip_str = "ip:{0} port:{1}".format(self.ip, self.port)
        ssh_username_str = "ssh_username:{0}".format(self.ssh_username)
        return "{0} {1}".format(ip_str, ssh_username_str)


class TestInputMembaseSetting(object):
    def __init__(self):
        self.rest_username = ''
        self.rest_password = ''


class TestInputBuild(object):
    def __init__(self):
        self.version = ''
        self.url = ''


# we parse this and then pass it on to all the test case
class TestInputParser:
    def __init__(self):
        pass

    @staticmethod
    def get_test_input(options):
        params = dict()
        if options.params:
            """
            Takes in a string of the form "p1=v1,v2,p2=v3,p3=v4,v5,v6"
            and converts to a dictionary of the form,
            {"p1":"v1,v2", "p2":"v3", "p3":"v4,v5,v6"}
            """
            argument_split = [a.strip() for a in re.split("[,]?([^,=]+)=",
                                                          options.params)[1:]]
            pairs = dict(zip(argument_split[::2], argument_split[1::2]))
            for pair in pairs.items():
                argument_list = [a.strip() for a in pair[1].split(",")]
                if len(argument_list) > 1:
                    # if the parameter had multiple entries separated
                    # by comma then store as a list
                    # ex. {'vbuckets':[1,2,3,4,100]}
                    params[pair[0]] = argument_list
                else:
                    # if parameter only had one entry then store
                    # as a string. ex. {'product':'cb'}
                    params[pair[0]] = argument_list[0]

        t_input = TestInputParser.parse_from_file(options.ini)
        # Now let's get the test specific parameters
        t_input.test_params = params

        # Do not override the command line value
        if "num_clients" not in t_input.test_params.keys() and t_input.clients:
            t_input.test_params["num_clients"] = len(t_input.clients)
        if "num_nodes" not in t_input.test_params.keys() and t_input.servers:
            t_input.test_params["num_nodes"] = len(t_input.servers)

        return t_input

    @staticmethod
    def parse_from_file(input_file):
        servers = []
        ips = []
        t_input = TestInput()
        config = ConfigParser()
        config.read(input_file)
        sections = config.sections()
        global_properties = {}
        count = 0
        start = 0
        end = 0
        cluster_ips = []
        clusters = {}
        t_input.tuq_client = {}
        client_ips = []
        t_input.dashboard = []
        t_input.ui_conf = {}
        for section in sections:
            result = re.search('^cluster', section)
            if section == "datasources":
                t_input.datasources = TestInputParser.get_datasources_config(config, section)
            elif section == "capella":
                t_input.capella = TestInputParser.get_capella_config(config, section)
            elif section == 'servers':
                ips = TestInputParser.get_server_ips(config, section)
            elif section == 'clients':
                client_ips = TestInputParser.get_server_ips(config, section)
            elif section == 'membase':
                t_input.membase_settings = \
                    TestInputParser.get_membase_settings(config, section)
            elif section == 'global':
                # Get global stuff and override for those unset
                for option in config.options(section):
                    global_properties[option] = config.get(section, option)
            elif section == 'dashboard':
                t_input.dashboard = TestInputParser.get_server_ips(config,
                                                                   section)
            elif section == 'uiconf':
                t_input.ui_conf = TestInputParser.get_ui_tests_config(config,
                                                                      section)
            elif section == 'tuq_client':
                t_input.tuq_client = TestInputParser.get_tuq_config(config,
                                                                    section)
            elif section == 'elastic':
                t_input.elastic = TestInputParser.get_elastic_config(config,
                                                                     section)
            elif result is not None:
                cluster_list = TestInputParser.get_server_ips(config, section)
                cluster_ips.extend(cluster_list)
                clusters[count] = len(cluster_list)
                count += 1

        # Setup 'cluster#' tag as dict
        # input.clusters -> {0: [ip:10.1.6.210 ssh_username:root,
        #                        ip:10.1.6.211 ssh_username:root]}
        for cluster_ip in cluster_ips:
            servers.append(TestInputParser.get_server(cluster_ip, config))
        servers = TestInputParser.get_server_options(servers,
                                                     t_input.membase_settings,
                                                     global_properties)
        if 'client' in t_input.tuq_client and t_input.tuq_client['client']:
            t_input.tuq_client['client'] = TestInputParser.get_server_options(
                [t_input.tuq_client['client']],
                t_input.membase_settings,
                global_properties)[0]
        for key, value in clusters.items():
            end += value
            t_input.clusters[key] = servers[start:end]
            start += value

        # Setting up 'servers' tag
        servers = []
        for ip in ips:
            servers.append(TestInputParser.get_server(ip, config))
        t_input.servers = TestInputParser.get_server_options(
            servers,
            t_input.membase_settings,
            global_properties)

        # Setting up 'clients' tag
        t_input.clients = client_ips
        return t_input

    @staticmethod
    def get_capella_config(config, section):
        capella = dict()
        for option in config.options(section):
            capella[option] = config.get(section, option)
        return capella

    @staticmethod
    def get_datasources_config(config, section):
        datasources = dict()
        for option in config.options(section):
            datasources[option] = config.get(section, option)
        return datasources

    @staticmethod
    def get_server_options(servers, membase_settings, global_properties):
        for server in servers:
            if server.ssh_username == '' and 'username' in global_properties:
                server.ssh_username = global_properties['username']
            if server.ssh_password == '' and 'password' in global_properties:
                server.ssh_password = global_properties['password']
            if server.ssh_key == '' and 'ssh_key' in global_properties:
                server.ssh_key = os.path.expanduser(
                    global_properties['ssh_key'])
            if not server.port and 'port' in global_properties:
                server.port = global_properties['port']
            if server.cli_path == '' and 'cli' in global_properties:
                server.cli_path = global_properties['cli']
            if server.rest_username == '' \
                    and membase_settings.rest_username != '':
                server.rest_username = membase_settings.rest_username
            if server.rest_password == '' \
                    and membase_settings.rest_password != '':
                server.rest_password = membase_settings.rest_password
            if server.data_path == '' and 'data_path' in global_properties:
                server.data_path = global_properties['data_path']
            if server.index_path == '' and 'index_path' in global_properties:
                server.index_path = global_properties['index_path']
            if server.cbas_path == '' and 'cbas_path' in global_properties:
                server.cbas_path = global_properties['cbas_path']
            if server.eventing_path == '' and 'eventing_path' in global_properties:
                server.eventing_path = global_properties['eventing_path']
            if server.services == '' and 'services' in global_properties:
                server.services = global_properties['services']
            if server.n1ql_port == '' and 'n1ql_port' in global_properties:
                server.n1ql_port = global_properties['n1ql_port']
            if server.index_port == '' and 'index_port' in global_properties:
                server.index_port = global_properties['index_port']
            if server.es_username == '' and 'es_username' in global_properties:
                server.es_username = global_properties['es_username']
            if server.es_password == '' and 'es_password' in global_properties:
                server.es_password = global_properties['es_password']
        return servers

    @staticmethod
    def get_server_ips(config, section):
        ips = []
        options = config.options(section)
        for option in options:
            ips.append(config.get(section, option))
        return ips

    @staticmethod
    def get_ui_tests_config(config, section):
        conf = {}
        server = TestInputServer()
        options = config.options(section)
        for option in options:
            if option == 'selenium_ip':
                server.ip = config.get(section, option)
            elif option == 'selenium_port':
                server.port = config.get(section, option)
            elif option == 'selenium_user':
                server.ssh_username = config.get(section, option)
            elif option == 'selenium_password':
                server.ssh_password = config.get(section, option)
            else:
                conf[option] = config.get(section, option)
        conf['server'] = server
        return conf

    @staticmethod
    def get_tuq_config(config, section):
        conf = {}
        options = config.options(section)
        for option in options:
            if option == 'ip':
                ip = config.get(section, option)
                conf['client'] = TestInputParser.get_server(ip, config)
            else:
                conf[option] = config.get(section, option)
            conf[option] = config.get(section, option)
        return conf

    @staticmethod
    def get_elastic_config(config, section):
        server = TestInputServer()
        options = config.options(section)
        for option in options:
            if option == 'ip':
                server.ip = config.get(section, option)
            if option == 'port':
                server.port = config.get(section, option)
            if option == 'es_username':
                server.es_username = config.get(section, option)
            if option == 'es_password':
                server.es_password = config.get(section, option)
        return server

    @staticmethod
    def get_server(ip, config):
        server = TestInputServer()
        server.ip = ip
        for section in config.sections():
            if section == ip:
                options = config.options(section)
                for option in options:
                    if option == 'username':
                        server.ssh_username = config.get(section, option)
                    if option == 'password':
                        server.ssh_password = config.get(section, option)
                    if option == 'cli':
                        server.cli_path = config.get(section, option)
                    if option == 'ssh_key':
                        server.ssh_key = config.get(section, option)
                    if option == 'port':
                        server.port = config.get(section, option)
                    if option == 'ip':
                        server.ip = config.get(section, option)
                    if option == 'services':
                        server.services = config.get(section, option)
                    if option == 'memcached_port':
                        server.memcached_port = config.get(section, option)
                    if option == 'n1ql_port':
                        server.n1ql_port = config.get(section, option)
                    if option == 'index_port':
                        server.index_port = config.get(section, option)
                    if option == 'fts_port':
                        server.fts_port = config.get(section, option)
                break
        return server

    @staticmethod
    def get_membase_build(config, section):
        membase_build = TestInputBuild()
        for option in config.options(section):
            if option == 'version':
                pass
            if option == 'url':
                pass
        return membase_build

    @staticmethod
    def get_membase_settings(config, section):
        membase_settings = TestInputMembaseSetting()
        for option in config.options(section):
            if option == 'rest_username':
                membase_settings.rest_username = config.get(section, option)
            if option == 'rest_password':
                membase_settings.rest_password = config.get(section, option)
        return membase_settings

    @staticmethod
    def handle_command_line_u_or_v(option, argument):
        input_build = TestInputBuild()
        if option == "-u":
            # let's check whether this url exists or not
            # let's extract version from this url
            pass
        if option == "-v":
            allbuilds = BuildQuery().get_all_builds()
            for build in allbuilds:
                if build.product_version == argument:
                    input_build.url = build.url
                    input_build.version = argument
                    break
        return input_build

    # Returns list of server objects
    @staticmethod
    def handle_command_line_s(argument):
        ips = argument.split(",")
        servers = []

        for ip in ips:
            server = TestInputServer()
            if ip.find(":") == -1:
                pass
            else:
                """
                info format: [ip, port, username, password, cli_path]
                """
                info = ip.split(":")
                server.ip = info[0]
                server.port = info[1]
                server.ssh_username = info[2]
                server.ssh_password = info[3]
                server.cli_path = info[4]
                servers.append(server)

        return servers
