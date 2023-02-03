#-------------------------------------------------------------------------------
#
# Asynchronous WPS back-end - connection listener object
#
# Authors: Martin Paces <martin.paces@eox.at>
#-------------------------------------------------------------------------------
# Copyright (C) 2016-2023 EOX IT Services GmbH
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
# pylint: disable=too-many-instance-attributes, too-many-arguments

from logging import getLogger
from errno import EBADF
from threading import Event, Semaphore
from eoxs_wps_async.util.thread import ThreadSet
from .settings import DEFAULT_CONNECTION_TIMEOUT, DEFAULT_POLL_TIMEOUT
from .connection_handler import ConnectionHandlerThread


class ConnectionListener:
    """ Object listening to the socket and handling new connections.

    Parameters:
        max_connections - maximum number of simultaneous connections (>= 1)
        connection_timeout - optional time in seconds after which an inactive
            client gets disconnected (set to 10s by default)
        poll_timeout - time in seconds after which the blocking poll() method
            returns control to the connection handler (1s by default)
        logger - optional logger

        handler - handler implementing the server protocol. Required methods:
            handler.handle_request(request)
            handler.busy_response()
            handler.timeout_response()
        connection - the actual handled multiprocessing.connection.Connection
            object
        semaphore - threading.Semaphore object limiting the number of active
            connections
        stop_event - optional stop event
        thread_group - optional set-like object registering the active
            connection handlers threads.
    """
    ConnectionHandlerThread = ConnectionHandlerThread

    def __init__(self, max_connections,
                 connection_timeout=DEFAULT_CONNECTION_TIMEOUT,
                 poll_timeout=DEFAULT_POLL_TIMEOUT,
                 logger=None):
        self.logger = logger or getLogger(__name__)
        self.connection_timeout = connection_timeout
        self.poll_timeout = poll_timeout

        self._stop_event = Event()
        self._poll_timeout = poll_timeout
        self._conn_timeout = connection_timeout
        self._semaphore = Semaphore(max_connections)
        self._thread_pool = ThreadSet()

    def listen(self, listener, protocol):
        """ Listen to the socket and process new incoming connections
        using the given protocol.

        Parameters:
            listener - socket listener object
            protocol - handler implementing the server protocol.
                Required methods:
                    handler.handle_request(request)
                    handler.busy_response()
                    handler.timeout_response()
        """
        self.logger.info("Daemon is listening to new connections ...")

        while not self._stop_event.is_set():
            self.logger.debug("CH: LISTEN")
            try:
                connection = listener.accept()
            except OSError as error:
                if error.errno == EBADF:
                    break
                raise
            self.handle_connection(connection, protocol)

        self.logger.debug("CH: STOP")

    def handle_connection(self, connection, protocol):
        """ Handle new connection.

        Parameters:
            connection - socket connection object
            protocol - handler implementing the server protocol.
                Required methods:
                    handler.handle_request(request)
                    handler.busy_response()
                    handler.timeout_response()
        """

        if self._stop_event.is_set():
            connection.close()
            return

        self.ConnectionHandlerThread(
            handler=protocol,
            connection=connection,
            semaphore=self._semaphore,
            thread_group=self._thread_pool,
            stop_event=self._stop_event,
            connection_timeout=self.connection_timeout,
            poll_timeout=self.poll_timeout,
            logger=self.logger,
        ).start()

    def stop(self):
        """ Tell the threads to stop. """
        self.logger.debug("CH: SHUTDOWN")
        self._stop_event.set()

    def join(self):
        """ Joint the remaining handler threads. """
        for item in list(self._thread_pool):
            item.join()
        self.logger.debug("CH: HANDLERS JOINED")
