#-------------------------------------------------------------------------------
#
# Processing daemon.
#
# Project: asynchronous WPS back-end
# Authors: Martin Paces <martin.paces@eox.at>
#
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
# pylint: disable=too-many-instance-attributes, too-many-arguments, no-self-use

import sys
from sys import stderr
from os import environ
from os.path import basename
from logging import getLogger, DEBUG, Formatter, StreamHandler
from time import time
from datetime import datetime

from traceback import format_exc
from socket import error as SocketError
from signal import SIGINT, SIGTERM, signal, SIG_IGN
from threading import Thread, Semaphore, Event
import multiprocessing.util as mp_util
from multiprocessing import Semaphore as ProcessSemaphore, Pool as ProcessPool

import django
from eoxserver.core import initialize as eoxs_initialize

from eoxs_wps_async.config import get_wps_config
from eoxs_wps_async.util.thread import ThreadSet, Queue
from eoxs_wps_async.util.ipc import get_listener
from eoxs_wps_async.handler import accept_job, execute_job, purge_job

LOGGER_NAME = "eoxs_wps_async.daemon"

def error(message, *args):
    """ Print error message. """
    print >>stderr, "ERROR: %s" % (message % args)


def info(message, *args):
    """ Print error message. """
    print >>stderr, "INFO: %s" % (message % args)


def usage(exename):
    """ Print simple usage. """
    print >>stderr, (
        "USAGE: %s <eoxs-settings-module> [<eoxs-instance-path>]" % exename
    )


def set_stream_handler(logger, level=DEBUG):
    """ Set stream handler to the logger. """
    formatter = Formatter('%(levelname)s: %(module)s: %(message)s')
    handler = StreamHandler()
    handler.setLevel(level)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(level)


def init_worker():
    """ Process pool initialization. """
    # ignore SIGINT in the worker processes
    signal(SIGINT, SIG_IGN)


class WorkerPoolManager(Thread):
    """ Thread managing the worker pool

    Parameters:
        worker_pool - worker pool
        semaphore - worker semaphore
        job_queue - job queue
        timeout - semaphore time-out in seconds
    """

    def __init__(self, worker_pool, semaphore, job_queue, timeout=1.0):
        Thread.__init__(self)
        self._pool = worker_pool
        self._semaphore = semaphore
        self._job_queue = job_queue
        self._stop_event = Event()
        self.timeout = timeout

    def stop(self):
        """ Tell the thread to stop. """
        self._stop_event.set()

    def run(self):
        """ Thread connection handler. """
        logger = getLogger(LOGGER_NAME)
        logger.debug("WPM: START")

        def callback(exception):
            """ Worker callback. """
            self._semaphore.release()
            logger.debug("WPM: SEMAPHORE RELEASED")
            if exception:
                logger.error("%s", exception)
                for line in format_exc().split("\n"):
                    logger.debug(line)

        while not self._stop_event.is_set():
            if self._semaphore.acquire(True, self.timeout):
                logger.debug("WPM: SEMAPHORE ACQUIRED")
                while not self._stop_event.is_set():
                    try:
                        _, job = self._job_queue.get()
                    except self._job_queue.Empty:
                        continue
                    self._pool.apply_async(
                        #execute_job, job, callback=worker_callback
                        execute_job, job, callback=callback
                    )
                    logger.debug("WPM: APPLIED JOB")
                    break

        logger.debug("WPM: STOP")


class ConnectionHandler(Thread):
    # pylint: disable=too-many-nested-blocks, too-many-branches
    """ Thread class dedicated to handling of the incoming connections.

    Parameters:
        handler - handler of the successful connection. This should accept
            the request and return the response:
               <response> = handler(<request>)
        busy_handler - handler of the refused connection (optional, set to None
            if not used). This handler is used in case of maximum number of
            incoming connections reached.
        timeout_handler - handler of the refused connection (optional, set to
            None if not used). This handler is used in case of connection
            time-out reached.
        connection_timeout - time in seconds after which an inactive client
            gets disconnected (10s by default)
        poll_timeout - time in seconds after which the blocking poll() method
            returns control to the connection handler (1s by default)
    """

    def __init__(self, handler, busy_handler, timeout_handler, connection,
                 semaphore, thread_group=None, connection_timeout=10.0,
                 poll_timeout=1.0):
        Thread.__init__(self)
        self._handler = handler
        self._busy_handler = busy_handler
        self._timeout_handler = timeout_handler
        self._poll_timeout = poll_timeout
        self._conn_timeout = connection_timeout
        self._stop_event = Event()
        self._connection = connection
        self._semaphore = semaphore
        self._thread_group = thread_group
        if self._thread_group:
            self._thread_group.add(self)

    def stop(self):
        """ Tell the thread to stop. """
        self._stop_event.set()

    def run(self):
        """ Thread connection handler. """
        logger = getLogger(LOGGER_NAME)
        try:
            if self._semaphore.acquire(False):
                logger.info("New connection accepted.")
                try:
                    start_time = time()
                    while not self._stop_event.is_set():
                        timeout = max(0, min(
                            self._conn_timeout + start_time - time(),
                            self._poll_timeout
                        ))
                        logger.debug("POLL %s", timeout)
                        if self._connection.poll(timeout):
                            logger.debug(
                                "RECV %s %s", time() - start_time,
                                self._conn_timeout
                            )
                            request = self._connection.recv()
                            logger.debug("REQUEST: %s", (request,))
                            response = self._handler(request)
                            logger.debug("RESPONSE: %s", (response,))
                            self._connection.send(response)
                            start_time = time()
                        elif (time() - start_time) > self._conn_timeout:
                            logger.debug(
                                "TIMEOUT %s %s", time() - start_time,
                                self._conn_timeout
                            )
                            logger.warning("Connection timed out.")
                            if self._timeout_handler:
                                response = self._timeout_handler()
                                self._connection.send(response)
                            break
                finally:
                    self._semaphore.release()
            else:
                logger.warning("Too busy. New connection rejected.")
                if self._busy_handler:
                    response = self._busy_handler()
                    self._connection.send(response)
        except EOFError as exc:
            logger.debug("EOF")
        except SocketError as exc:
            logger.error("SocketError: %s", exc)
            for line in format_exc().split("\n"):
                logger.debug(line)
        except Exception as exc: #pylint: disable=broad-except
            logger.error("Exception: %s", exc)
            for line in format_exc().split("\n"):
                logger.error(line)
        finally:
            logger.debug("CLOSE")
            self._connection.close()
            if self._thread_group:
                self._thread_group.remove(self)


class Daemon(object):
    """ Task scheduling daemon.

    Parameters:
        socket_filename - file-name of the IPC socket.
        max_connections - maximum allowed number of concurrent connections
            (64 by default).
        connection_timeout - time in seconds after which an inactive client
            gets disconnected (10s by default)
        poll_timeout - time in seconds after which the blocking poll() method
            returns control to the connection handler (1s by default)
    """

    def __init__(self, socket_filename, max_connections=64,
                 connection_timeout=10, poll_timeout=1, num_workers=1,
                 max_queued_jobs=8, logger=None):
        self.socket_address = socket_filename
        self.socket_family = 'AF_UNIX'
        self.socket_args = ()
        self.socket_kwargs = {}
        self.socket_listener = None
        self.connection_semaphore = Semaphore(max_connections)
        self.connection_handlers = ThreadSet()
        self.connection_timeout = connection_timeout
        self.connection_poll_timeout = poll_timeout
        self.job_queue = Queue(max_queued_jobs)

        self.logger = logger or getLogger(LOGGER_NAME)

        # set verbose multi-processes debugging
        set_stream_handler(mp_util.get_logger())
        mp_util.get_logger().setLevel(mp_util.SUBWARNING)
        worker_semaphore = ProcessSemaphore(num_workers)

        self.worker_pool = ProcessPool(num_workers, init_worker)
        self.worker_pool_manager = WorkerPoolManager(
            self.worker_pool, worker_semaphore, self.job_queue
        )

    def run(self):
        """ Start the daemon. """
        if self.socket_listener is not None:
            raise RuntimeError(
                "An attempt to start again an already running daemon instance!"
            )
        self.logger.info("Starting daemon ...")
        self.worker_pool_manager.start()
        try:
            # set signal handlers
            signal(SIGINT, self.terminate)
            signal(SIGTERM, self.terminate)
            # create the listener for incoming connections
            self.logger.info("Listening at %s ...", self.socket_address)
            self.socket_listener = get_listener(
                self.socket_address, self.socket_family,
                *self.socket_args, **self.socket_kwargs
            )
            # start handling new connections
            info("Daemon is listening to new connections ...")
            while True:
                ConnectionHandler(
                    self.request_handler,
                    self.busy_response, self.timeout_response,
                    self.socket_listener.accept(), self.connection_semaphore,
                    self.connection_handlers, self.connection_timeout,
                    self.connection_poll_timeout,
                ).start()
        except SocketError as exc:
            self.logger.error("SocketError: %s", exc)
            for line in format_exc().split("\n"):
                self.logger.debug(line)
        finally:
            self.cleanup()

    def cleanup(self):
        """ Perform the final clean-up. """
        # avoid repeated clean-up
        if getattr(self, '_cleanedup', False):
            return
        self._cleanedup = True

        message = "Stopping daemon ..."
        self.logger.info(message)
        info(message)

        # terminate threads and processes
        if self.socket_listener:
            self.socket_listener.close()
        self.worker_pool_manager.stop()
        self.worker_pool.terminate()
        for item in list(self.connection_handlers):
            item.stop()

        # join the pending threads
        self.worker_pool_manager.join()
        self.worker_pool.join()
        for item in list(self.connection_handlers):
            item.join()

        self.socket_listener = None
        self.worker_pool_manager = None
        self.worker_pool = None

        message = "Daemon stopped."
        self.logger.info(message)
        info(message)


    def terminate(self, signum=None, frame=None):
        # pylint: disable=unused-argument
        """ Signal handler performing graceful daemon shut-down and clean-up.
        """
        self.logger.info("Termination signal received ...")
        self.cleanup()

    def busy_response(self):
        """ Busy response. """
        return "BUSY",

    def timeout_response(self):
        """ Timeout response. """
        return "TIMEOUT",

    def request_handler(self, request):
        """ Request handler. """
        try:
            if request[0] == "EXECUTE":
                timestamp = datetime.utcnow()
                job = request[1:]
                accept_job(*job)
                try:
                    self.job_queue.put((timestamp, job))
                except self.job_queue.Full:
                    purge_job(job[0])
                    return "BUSY",
            else:
                ValueError("Unknown request! REQ=%r" % request[0])
        except Exception as exc: #pylint: disable=broad-except
            self.logger.error("Handler error: %s", exc)
            for line in format_exc().split("\n"):
                self.logger.error(line)
            return "ERROR", str(exc)
        else:
            return "OK",

def main(argv):
    """ Main subroutine. """
    # configure Django settings module
    try:
        environ["DJANGO_SETTINGS_MODULE"] = argv[1]
    except IndexError:
        error("Not enough input arguments!")
        usage(basename(argv[0]))
        return 1

    # add Django instance search path
    for path in argv[2:]:
        if path not in sys.path:
            sys.path.append(path)

    # initialize the EOxServer component system.
    # ... set temporary stderr stream log handler to see the components' imports
    set_stream_handler(getLogger())
    eoxs_initialize()

    # initialize Django
    django.setup()

    # kick in logger
    # initialize Django
    logger = getLogger(LOGGER_NAME)
    logger.info("Daemon is initialized.")
    info("Daemon is initialized.")

    # load configuration
    conf = get_wps_config()

    try:
        Daemon(
            conf.socket_file,
            conf.socket_max_connections,
            conf.socket_connection_timeout,
            conf.socket_poll_timeout,
            conf.num_workers,
            conf.max_queued_jobs,
            logger=logger,
        ).run()
    except Exception as exc:
        logger.error("Daemon crash: %s", exc)
        for line in format_exc().split("\n"):
            logger.error(line)
        raise


if __name__ == "__main__":
    sys.exit(main(sys.argv))
