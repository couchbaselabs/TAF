import errno
import base64
import socket

from global_vars import logger
from java.util.concurrent.atomic import AtomicInteger
from limits.abstract_resource_tasks import UserResourceTask
from limits.non_blocking_connection import NonBlockingConnection
from membase.api.rest_client import RestConnection
from threading import Thread


log = logger.get("test")


def get_http_request(host, path, username, password):
    """ Returns a http GET request """
    token = base64.encodestring('%s:%s' % (username, password)).strip()

    lines = ['GET %s HTTP/1.1' % path,
             'Host: %s' % host,
             'Authorization: Basic %s' % token,
             'Connection: close']  # (The other side will send an EOF)

    return '\r\n'.join(lines) + '\r\n\r\n'


class NsServerNumConcurrentRequests(UserResourceTask):

    def __init__(self, user, node, streaminguri):
        super(NsServerNumConcurrentRequests, self).__init__(user, node)
        self.nconns = []  # Belongs exclusively to poll method
        self.thread = None
        self.httprq = get_http_request(
            self.node.ip,
            streaminguri,
            self.node.rest_username,
            self.node.rest_password)
        self.no_of_connections = AtomicInteger(0)
        self.no_of_open_connections = AtomicInteger(0)
        self.no_of_throughput_updates = AtomicInteger(0)
        self.rest = RestConnection(self.node)

    def on_throughput_increase(self, throughput):
        log.debug(
            "Increasing throughput by {}".format(
                throughput -
                self.throughput))

        # Record the last throughput update
        last_throughput_update = self.no_of_throughput_updates.get()

        # Update the throughput
        self.no_of_connections.set(throughput)

        # Launch the thread
        if self.thread is None:
            self.thread = Thread(target=self.poll)
            self.thread.start()

        # Block until the update has gone (TODO add a timeout)
        while self.no_of_throughput_updates.get() <= last_throughput_update:
            continue

    def on_throughput_decrease(self, throughput):
        log.debug(
            "Decreasing throughput by {}".format(
                self.throughput -
                throughput))

        # Record the last throughput update
        last_throughput_update = self.no_of_throughput_updates.get()

        # Update the throughput
        self.no_of_connections.set(throughput)

        if self.thread and throughput == 0:
            self.thread.join()
            self.thread = None

        # Block until the update has gone (TODO add a timeout)
        while self.no_of_throughput_updates.get() <= last_throughput_update:
            continue

    def get_throughput_success(self):
        return self.no_of_open_connections.get()

    def poll(self):
        """ Repeatedly poll each connection and attempt to keep them alive """
        no_of_conns = self.no_of_connections.get()

        while no_of_conns > 0 or len(self.nconns) > 0:
            update_nconns = no_of_conns != len(self.nconns)

            if update_nconns:
                # Add any new connections
                for i in range(no_of_conns - len(self.nconns)):
                    self.nconns.append(
                        NonBlockingConnection(
                            self.node.ip, 8091, self.httprq))
                # Disconnect the connections that need to be closed
                for conn in self.nconns[no_of_conns:]:
                    conn.disconnect()
                # Delete the disconnected connections
                del self.nconns[no_of_conns:]

            # Poll and count open connections
            open_count = 0
            for conn in self.nconns:
                if conn.poll():
                    open_count += 1

            # Update the number of open connections
            self.no_of_open_connections.set(open_count)

            # Notify the main thread that the connections have been updated
            if update_nconns:
                self.no_of_throughput_updates.incrementAndGet()

            no_of_conns = self.no_of_connections.get()

    def error(self):
        return self.rest._http_request(self.rest.baseUrl + "/pools/default")[1]

    def expected_error(self):
        return 'Limit(s) exceeded [num_concurrent_requests]'
