from cb_server_rest_util.connection import CBRestConnection


class DiagEvalAPI(CBRestConnection):
    def __init__(self):
        super(DiagEvalAPI, self).__init__()

    def diag_eval(self, code):

        api = self.base_url + '/diag/eval'
        status, content, _ = self.request(api, CBRestConnection.POST, code)
        return status, content

    def ns_bucket_update_bucket_props(self, bucket, bucket_prop_value_config):
        code = "ns_bucket:update_bucket_props(\"" + bucket + "\", [{extra_config_string, \"" + bucket_prop_value_config + "\"}])"
        status, content = self.diag_eval(code)
        return status, content

    def ns_config_set(self, key, value):
        code = "ns_config:set(" + key + ", " + str(value) + ")"
        status, content = self.diag_eval(code)
        return status, content

    def testconditions_set(self, test_condition):
        code = 'testconditions:set({0})'.format(test_condition)
        status, content = self.diag_eval(code)
        return status, content

    def testconditions_delete(self, test_condition):
        code = 'testconditions:delete({0})'.format(test_condition)
        status, content = self.diag_eval(code)
        return status, content

    def cluster_compat_mode_get_version(self):
        status, content = self.diag_eval('cluster_compat_mode:get_compat_version().')
        return status, content

    def get_admin_credentials(self):
        code = 'ns_config:search_node_prop(node(), ' \
               'ns_config:latest(), memcached, %s)'
        status, id = self.diag_eval(code.format("admin_user"))
        status, password = self.diag_eval(code.format("admin_pass"))
        return id.strip('"'), password.strip('"')

    def change_memcached_t_option(self, value):
        cmd = '[ns_config:update_key({node, N, memcached}, fun (PList)' + \
              ' -> lists:keystore(verbosity, 1, PList, {verbosity, \'-t ' + str(value) + '\'}) end)' + \
              ' || N <- ns_node_disco:nodes_wanted()].'
        return self.diag_eval(cmd)