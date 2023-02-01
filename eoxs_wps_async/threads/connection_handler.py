#-------------------------------------------------------------------------------
#
# Asynchronous WPS back-end - Connection Handler thread object
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

from threading import Thread, Event
from logging import getLogger
from eoxs_wps_async.util import format_exception, Timer

LOGGER_NAME = "eoxs_wps_async.daemon"


class ConnectionHandlerThread(Thread):
    """ Thread class dedicated to handling of the incoming connections.

    Parameters:
        handler - handler implementing the serve protocol. Required methods:
            handler.handle_request(request)
            handler.busy_response()
            handler.timeout_response()
        connection - the actual handled multiprocessing.connection.Connection
            object
        semaphore - threading.Semaphore object limiting the number of active
            connections
        thread_group - optional set-like object registering the active
            connection handlers threads.
        connection_timeout - optional time in seconds after which an inactive
            client gets disconnected (set to 10s by default)
        poll_timeout - time in seconds after which the blocking poll() method
            returns control to the connection handler (1s by default)
    """

    def __init__(self, request_handler, connection,
                 semaphore, thread_group=None, connection_timeout=10.0,
                 poll_timeout=1.0):
        Thread.__init__(self)
        self._request_handler = request_handler
        self._poll_timeout = poll_timeout
        self._conn_timeout = connection_timeout
        self._stop_event = Event()
        self._connection = connection
        self._semaphore = semaphore
        self._thread_group = thread_group
        if self._thread_group:
            self._thread_group.add(self)
        self.logger = None

    def stop(self):
        """ Tell the thread to stop. """
        self._stop_event.set()

    def run(self):
        """ Thread connection handler. """
        self.logger = getLogger(LOGGER_NAME)
        try:
            if self._semaphore.acquire(False):
                self.logger.debug("New connection accepted.")
                try:
                    self._handle_requests()
                finally:
                    self._semaphore.release()
            else:
                self.logger.warning("Too busy. New connection rejected.")
                self._connection.send(self._request_handler.busy_response())
        except EOFError:
            self.logger.debug("EOF")
        except OSError as error:
            self.logger.error("%s", format_exception(error))
        except Exception as error:
            self.logger.error(
                "Connection failed! %s",
                format_exception(error), exc_info=True
            )
        finally:
            self._connection.close()
            self.logger.debug("Connection closed.")
            if self._thread_group:
                self._thread_group.remove(self)

    def _handle_requests(self):
        """ Handle stream of client requests. """

        def _receive_request(elapsed_time):
            self.logger.debug("RECV %s %s", elapsed_time, self._conn_timeout)
            request = self._connection.recv()
            self.logger.debug("REQUEST: %s", request)
            return request

        def _send_response(response):
            self.logger.debug("RESPONSE: %s", response)
            self._connection.send(response)

        def _get_timeout(elapsed_time):
            timeout = max(0, min(
                self._conn_timeout - elapsed_time,
                self._poll_timeout
            ))
            self.logger.debug("POLL %s", timeout)
            return timeout

        def _is_connection_timed_out(elapsed_time):
            if elapsed_time > self._conn_timeout:
                self.logger.debug(
                    "TIMEOUT %s %s", elapsed_time, self._conn_timeout
                )
                return True
            return False

        timer = Timer()

        while not self._stop_event.is_set():
            if self._connection.poll(_get_timeout(timer.elapsed_time)):
                request = _receive_request(timer.elapsed_time)
                response = self._request_handler.handle_request(request)
                _send_response(response)
                timer.reset()

            elif _is_connection_timed_out(timer.elapsed_time):
                self.logger.warning("Connection timed out.")
                _send_response(self._request_handler.timeout_response())
                return
