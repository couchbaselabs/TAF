import errno
import socket


class NonBlockingConnection:

    def __init__(self, host, port, mesg):
        self.host = host
        self.port = port
        self.mesg = mesg
        self.open = False
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # Note that calling the constructor also establishes the connection. As
        # connections are killed in order of oldest to newest.
        self.reconnect()

    def reconnect(self):
        """ Restablish connection and send the message """
        try:
            self.sock.setblocking(True)
            self.sock.connect((self.host, self.port))
            self.sock.sendall(self.mesg)
            self.sock.setblocking(False)
            self.open = True
        except socket.error as e:
            self.open = False

    def disconnect(self):
        """ Close socket """
        self.sock.close()
        self.open = False

    def poll(self):
        """ Poll the socket while messages are available """
        try:
            # Restablish connection
            if not self.open:
                self.reconnect()

            # Consume as many messages as possible
            while True:
                msg = self.sock.recv(2048)
                # Update if the connection was disconnected by an EOF
                if len(msg) == 0:
                    self.open = False
                    self.sock.close()
                    break
        except socket.error as e:
            # Update the connection status if it's a blocking related error
            if e.args[0] != errno.EAGAIN and e.args[0] != errno.EWOULDBLOCK:
                self.open = False

        return self.open
