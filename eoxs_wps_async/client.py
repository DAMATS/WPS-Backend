#-------------------------------------------------------------------------------
#
# Asynchronous WPS back-end - client proxy
#
# Authors: Martin Paces <martin.paces@eox.at>
#-------------------------------------------------------------------------------
# Copyright (C) 2016 EOX IT Services GmbH
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies of this Software or works derived from this Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#-------------------------------------------------------------------------------

from time import time
from .util.ipc import get_client


class ClientError(IOError):
    """ Client error. """


class ClientConnectionError(ClientError):
    """ Failed to connect to the processing daemon. """


class ClientConnectionTimeout(ClientError):
    """ Client connection time-out. """


class ClientConnectionClosed(ClientError):
    """ Client connection closed. """


class Client():
    """ Client class.

    Parameters:
        socket_filename - file-name of the IPC socket.
        connection_timeout - time in seconds after which an inactive client
            gets disconnected (10s by default)
    """

    def __init__(self, socket_filename, connection_timeout=10):
        self._conn = None
        self._conn_timeout = connection_timeout
        self.socket_address = socket_filename
        self.socket_family = 'AF_UNIX'
        self.socket_kwargs = {}

    def __del__(self):
        if hasattr(self, '_conn'):
            self.close()

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, type, value, traceback):
        # pylint: disable=redefined-builtin
        self.close()

    def connect(self):
        """ Open connection. """
        if not self._conn:
            try:
                self._conn = get_client(
                    self.socket_address, self.socket_family, **self.socket_kwargs
                )
            except OSError:
                raise ClientConnectionError(
                    "Failed to connect to the daemon socket!"
                )

    def close(self):
        """ Close connection. """
        if self._conn:
            self._conn.close()
            self._conn = None

    def send(self, obj):
        """ Send an object. """
        if not self._conn:
            raise ClientConnectionError("No connection to the daemon socket!")
        self._conn.send(obj)

    def recv(self):
        """ Receive an object. """
        if not self._conn:
            raise ClientConnectionError("No connection to the daemon socket!")
        start_time = time()
        try:
            while True:
                timeout = max(0, self._conn_timeout + start_time - time())
                if self._conn.poll(timeout):
                    return self._conn.recv()
                if (time() - start_time) > self._conn_timeout:
                    raise ClientConnectionTimeout("Connection timed out!")
        except EOFError:
            ClientConnectionClosed(
                "Connection closed before receiving any response!"
            )
