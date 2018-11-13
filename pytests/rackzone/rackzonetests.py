from membase.api.rest_client import RestConnection
from rackzone.rackzonebasetest import RackzoneBaseTest


class RackZoneTests(RackzoneBaseTest):
    def setUp(self):
        super(RackZoneTests, self).setUp()
        self.command = self.input.param("command", "")
        self.zone = self.input.param("zone", 1)
        self.replica = self.input.param("replica", 1)
        self.command_options = self.input.param("command_options", '')
        self.set_get_ratio = self.input.param("set_get_ratio", 0.9)
        self.item_size = self.input.param("item_size", 128)
        self.shutdown_zone = self.input.param("shutdown_zone", 1)
        self.do_verify = self.input.param("do-verify", True)
        self.num_node = self.input.param("num_node", 4)
        self.timeout = 6000

    def tearDown(self):
        super(RackZoneTests, self).tearDown()

    def _verify_zone(self, name):
        serverInfo = self.cluster.servers[0]
        rest = RestConnection(serverInfo)
        if rest.is_zone_exist(name.strip()):
            self.log.info("verified! zone '{0}' is existed".format(name.strip()))
        else:
            raise Exception("There is not zone with name: %s in cluster" % name)

    def test_check_default_zone_create_by_default(self):
        zone_name = "Group 1"
        self._verify_zone(zone_name)

    def test_create_zone_with_upper_lower_number_and_space_name(self):
        zone_name = " AAAB BBCCC aakkk kmmm3 456 72 "
        serverInfo = self.cluster.servers[0]
        rest = RestConnection(serverInfo)
        try:
            self.log.info("create zone {0}".format(zone_name))
            rest.add_zone(zone_name)
        except Exception,e :
            print e
        self._verify_zone(zone_name)
