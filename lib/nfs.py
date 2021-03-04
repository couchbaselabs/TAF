from remote.remote_util import RemoteMachineShellConnection


class NfsConnection:
    def __init__(self, server, clients, directory_to_share, directory_to_mount):
        """ Shares a folder across multiple clients using NFS.

        Shares `directory_to_share` on `server` across `clients` mounting the
        shared folder at `directory_to_mount`.

        """
        self.server, self.clients = NfsServer(server), [NfsClient(server, client) for client in clients]
        self.directory_to_share = directory_to_share
        self.directory_to_mount = directory_to_mount

    def share(self, privileges={}):
        """ Shares the folder

        Causes `self.directory_to_share` to be shared across all clients at
        `self.directory_to_mount`.
        """
        self.server.share(self.directory_to_share, privileges=privileges)

        for client in self.clients:
            client.mount(self.directory_to_share, self.directory_to_mount)

    def clean(self):
        """ Unshares the folder.

        Unshares `self.directory_to_share` and unmounts `self.directory_to_mount`
        """
        self.server.clean()

        for client in self.clients:
            client.clean(self.directory_to_mount)

    def close(self):
        """ Closes any resources held (e.g. RemoteShellConnections). """
        self.server.close()

        for client in self.clients:
            client.close()


class NfsServer:
    exports_directory = "/etc/exports"

    def __init__(self, server):
        self.remote_shell = RemoteMachineShellConnection(server)
        self.provision()

    def fetch_id(self, username, id_type='u'):
        """ Fetch a id dynamically

        Args:
            id_type (str): If 'u' fetches the uid. If 'g' fetches the gid.
        """
        accepted_id_types = ['u', 'g']

        if id_type not in accepted_id_types:
            raise ValueError("The id_type:{} is not in {}".format(id_type, accepted_id_types))

        output, error = self.remote_shell.execute_command("id -{} {}".format(id_type, username))
        return int(output[0])

    def provision(self):
        self.remote_shell.execute_command("yum -y install nfs-utils")
        self.remote_shell.execute_command("systemctl start nfs-server.service")

    def share(self, directory_to_share, privileges={}):
        """ Shares `directory_to_share`

        params:
            directory_to_share (str): The directory that will be shared, it will be created/emptied.
            privileges (dict): A dict of hosts to host specific privileges where privilegs are comma
            separated e.g. {'127.0.0.1': 'ro'}.
        """
        self.remote_shell.execute_command("exportfs -ua")
        self.remote_shell.execute_command("rm -rf {}".format(directory_to_share))
        self.remote_shell.execute_command("mkdir -p {}".format(directory_to_share))
        self.remote_shell.execute_command("chmod -R 777 {}".format(directory_to_share))
        self.remote_shell.execute_command("chown -R couchbase:couchbase {}".format(directory_to_share))
        self.remote_shell.execute_command("echo -n > {}".format(NfsServer.exports_directory))

        # If there are no privileges every host gets read-write permissions
        if not privileges:
            privileges['*'] = 'rw'

        # Fetch uid, gid dynamically
        uid, gid = self.fetch_id('couchbase', id_type='u'), self.fetch_id('couchbase', id_type='g')

        for host, privilege in privileges.items():
            self.remote_shell.execute_command("echo '{} {}({},sync,all_squash,anonuid={},anongid={},fsid=1)' >> {} && exportfs -a".format(
                directory_to_share, host, privilege, uid, gid, NfsServer.exports_directory))

    def clean(self):
        self.remote_shell.execute_command("echo -n > {} && exportfs -a".format(NfsServer.exports_directory))

    def close(self):
        self.remote_shell.disconnect()


class NfsClient:

    def __init__(self, server, client):
        self.server, self.client, self.remote_shell = server, client, RemoteMachineShellConnection(client)
        self.provision()

    def provision(self):
        self.remote_shell.execute_command("yum -y install nfs-utils")
        self.remote_shell.execute_command("systemctl start nfs-server.service")

    def mount(self, directory_to_share, directory_to_mount):
        self.remote_shell.execute_command("umount -f -l {}".format(directory_to_mount))
        self.remote_shell.execute_command("rm -rf {}".format(directory_to_mount))
        self.remote_shell.execute_command("mkdir -p {}".format(directory_to_mount))
        self.remote_shell.execute_command("mount {}:{} {}".format(
            self.server.ip, directory_to_share, directory_to_mount))

    def clean(self, directory_to_mount):
        self.remote_shell.execute_command("umount -f -l {}".format(directory_to_mount))

    def close(self):
        self.remote_shell.disconnect()
