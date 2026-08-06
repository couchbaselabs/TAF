from cb_server_rest_util.connection import CBRestConnection


class Auditing(CBRestConnection):
    def __init__(self):
        super(Auditing, self).__init__()
