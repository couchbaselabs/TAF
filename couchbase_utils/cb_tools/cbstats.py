from couchbase_utils.cb_tools.cbstats_cmdline import Cbstats as Cbstats_cmdline
from couchbase_utils.cb_tools.cbstats_memcached import Cbstats as Cbstats_mc


class Cbstats(object):

    def __new__(cls, *args, **kwargs):
        server = args[0]
        username = kwargs.get("username", "Administrator")
        password = kwargs.get("password", "password")

        if server.type == "default":
            cbstat_obj = Cbstats_cmdline(server, username, password)
        else:
            username = kwargs.get("username", None)
            password = kwargs.get("password", None)
            cbstat_obj = Cbstats_mc(server, username, password)
        return cbstat_obj
