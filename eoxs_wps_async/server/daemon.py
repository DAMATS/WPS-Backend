#-------------------------------------------------------------------------------
#
# Asynchronous WPS back-end - daemon class
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
# pylint: disable=too-many-instance-attributes, too-many-arguments, no-self-use

from logging import getLogger
from signal import SIGINT, SIGTERM, signal
from eoxs_wps_async.util.thread import Queue
from eoxs_wps_async.util.ipc import get_listener
from eoxs_wps_async.protocol import ServerProtocol
from .connection_listener import ConnectionListener
from .work_pool_manager import WorkerPoolManagerThread


class Daemon:
    """ Task scheduling daemon.

    Parameters:
        socket_family - string socket type (AF_UNIX|AF_INET).
        socket_address - file-name (AF_UNIX) of the IPv4 (host, port) tuple (AF_INET).
        max_connections - maximum allowed number of concurrent connections
            (64 by default).
        connection_timeout - time in seconds after which an inactive client
            gets disconnected (10s by default)
        poll_timeout - time in seconds after which the blocking poll() method
            returns control to the connection handler (1s by default)
        num_workers - maximum number of active workers (>= 1)
        num_workers_processed - maximum number of worker processes (>= num_workers)
        max_processed_jobs - maximum processed job by one worker pool process
            before being restarted (None means never restarted)
        max_queued_jobs - maximum queued jobs waiting for processing (>= 1)
        logger - optional logger
    """
    ServerProtocol = ServerProtocol
    WorkerPoolManagerThread = WorkerPoolManagerThread
    ConnectionListener = ConnectionListener

    def __init__(self, socket_family, socket_address, max_connections=64,
                 connection_timeout=10, poll_timeout=1, num_workers=1,
                 num_worker_processes=2, max_processed_jobs=1,
                 max_queued_jobs=8, logger=None):

        self.logger = logger or getLogger(__name__)

        self.socket_address = socket_address
        self.socket_family = socket_family
        self.socket_kwargs = {}
        self.socket_listener = None

        self.max_connections = max_connections
        self.connection_timeout = connection_timeout
        self.connection_poll_timeout = poll_timeout

        self.max_queued_jobs = max_queued_jobs
        self.num_workers = num_workers
        self.num_worker_processes = num_worker_processes
        self.max_processed_jobs = max_processed_jobs

        self.connection_handler = None
        self.worker_pool_manager = None

        self.server_protocol = None

    def run(self):
        """ Start the daemon. """
        if self.socket_listener is not None:
            raise RuntimeError(
                "An attempt to start again an already running daemon instance!"
            )

        self.logger.info("Starting daemon ...")
        self.server_protocol = self.ServerProtocol(
            job_queue=Queue(self.max_queued_jobs),
            logger=self.logger
        )
        self.connection_handler = ConnectionListener(
            max_connections=self.max_connections,
            connection_timeout=self.connection_timeout,
            poll_timeout=self.connection_poll_timeout,
            logger=self.logger,
        )
        self.worker_pool_manager = self.WorkerPoolManagerThread(
            handler=self.server_protocol,
            num_workers=self.num_workers,
            num_worker_processes=self.num_worker_processes,
            max_processed_jobs=self.max_processed_jobs,
            logger=self.logger,
        )
        self.worker_pool_manager.start()

        try:
            # set signal handlers
            signal(SIGINT, self.terminate)
            signal(SIGTERM, self.terminate)

            self.logger.info("Listening at %s ...",
                    self.socket_address
                    if not isinstance(self.socket_address, tuple) else
                    "%s:%d" % self.socket_address
            )
            self.socket_listener = get_listener(
                self.socket_address, self.socket_family, **self.socket_kwargs
            )

            # load pickled jobs left from the previous run
            self.reload_jobs()

            # start the server
            self.connection_handler.listen(
                self.socket_listener, self.server_protocol
            )

        finally:
            self.cleanup()

    def cleanup(self):
        """ Perform the final clean-up. """

        if self.worker_pool_manager is None:
            # avoid repeated clean-up
            return

        self.logger.info("Stopping daemon ...")

        if self.socket_listener is not None:
            # stop socket listener
            self.socket_listener.close()
            self.socket_listener = None

        # terminate threads and processes
        self.connection_handler.stop()
        self.worker_pool_manager.stop()

        # join the pending threads
        self.worker_pool_manager.join()
        self.connection_handler.join()

        self.worker_pool_manager = None
        self.connection_handler = None

        self.logger.info("Daemon is stopped.")

    def terminate(self, signum=None, frame=None):
        """ Signal handler performing graceful daemon shut-down and clean-up.
        """
        del signum, frame
        self.logger.info("Termination signal received ...")
        self.cleanup()

    def reload_jobs(self):
        """ Load tasks after server restart. """
        self.logger.info("Loading stored jobs ...")
        count = self.server_protocol.reload_jobs()
        self.logger.info(
            "Reloaded %d stored job%s.",
            count, "" if count == 1 else "s"
        )
