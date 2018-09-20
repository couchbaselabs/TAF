from remote.remote_util import RemoteMachineShellConnection
from cbas.cbas_base import CBASBaseTest
import testconstants
from membase.api.rest_client import RestConnection



class JreLessTest(CBASBaseTest):
    def setUp(self):
        super(JreLessTest, self).setUp()

        shell = RemoteMachineShellConnection(self.master)
        type = shell.extract_remote_info().distribution_type
        if type.lower() == 'windows':
            self.path = testconstants.WIN_COUCHBASE_BIN_PATH
            self.curl_path = "%scurl" % self.path
            self.n1ql_certs_path = "/cygdrive/c/Program\ Files/Couchbase/server/var/lib/couchbase/n1qlcerts"
        self.jre=self.input.param("jre",None)


    def test_default_jre_path(self):
        self.rest = RestConnection(self.cbas_node)
        res=self.rest.get_jre_path()
        self.assertEquals(res["storage"]["hdd"][0]["java_home"],'')
        diag_res=self.cbas_util.get_analytics_diagnostics(self.cbas_node)
        self.assertEquals(diag_res["runtime"]["systemProperties"]["java.home"],'/opt/couchbase/lib/cbas/runtime')

    def test_empty_jre_path(self):
        self.rest = RestConnection(self.cbas_node)
        self.rest.set_jre_path('')
        res = self.rest.get_jre_path()
        self.assertEquals(res["storage"]["hdd"][0]["java_home"], '')
        diag_res = self.cbas_util.get_analytics_diagnostics(self.cbas_node)
        self.assertEquals(diag_res["runtime"]["systemProperties"]["java.home"], '/opt/couchbase/lib/cbas/runtime')

    def test_set_supported_jre_path(self):
        self.rest = RestConnection(self.cbas_node)
        self.rest.set_jre_path(jre_path=self.jre)
        self.sleep(60)
        res = self.rest.get_jre_path()
        self.assertEquals(res["storage"]["hdd"][0]["java_home"], self.jre)
        diag_res = self.cbas_util.get_analytics_diagnostics(self.cbas_node)
        self.assertEquals(diag_res["runtime"]["systemProperties"]["java.home"], self.jre)

    def test_unsupported_jre_path(self):
        self.rest=RestConnection(self.cbas_node)
        content=self.rest.set_jre_path(jre_path=self.jre,set=False)
        print(content)
        self.assertTrue("has incorrect version of java or is not a java home directory" in content)
        res = self.rest.get_jre_path()
        self.assertEquals(res["storage"]["hdd"][0]["java_home"], '')
        diag_res = self.cbas_util.get_analytics_diagnostics(self.cbas_node)
        self.assertEquals(diag_res["runtime"]["systemProperties"]["java.home"], '/opt/couchbase/lib/cbas/runtime')