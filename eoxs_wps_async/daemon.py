#-------------------------------------------------------------------------------
#
# Asynchronous WPS back-end - processing daemon
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

import sys
from sys import stderr
from os import environ
from os.path import basename
#from glob import iglob
from logging import getLogger, DEBUG, INFO, Formatter, StreamHandler
from errno import EBADF
from signal import SIGINT, SIGTERM, signal, SIG_IGN
from threading import Semaphore
import multiprocessing.util as mp_util
from multiprocessing import Semaphore as ProcessSemaphore, Pool as ProcessPool
import django
from eoxserver.core import initialize as eoxs_initialize
from eoxs_wps_async.config import get_wps_config, inet_address
from eoxs_wps_async.util.thread import ThreadSet, Queue
from eoxs_wps_async.util.ipc import get_listener
from eoxs_wps_async.threads import (
    WorkerPoolManagerThread, ConnectionHandlerThread,
)
from eoxs_wps_async.protocol import ServerProtocol

LOGGER_NAME = "eoxs_wps_async.daemon"


def set_stream_handler(logger, level=DEBUG):
    """ Set stream handler to the logger. """
    formatter = Formatter('%(levelname)s: %(module)s: %(message)s')
    handler = StreamHandler()
    handler.setLevel(level)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(min(level, logger.level))


def init_worker():
    """ Process pool initialization. """
    # ignore SIGINT and SIGTERM in the worker processes to prevent dead-locks
    signal(SIGINT, SIG_IGN)
    signal(SIGTERM, SIG_IGN)


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
    """

    def __init__(self, socket_family, socket_address, max_connections=64,
                 connection_timeout=10, poll_timeout=1, num_workers=1,
                 num_worker_processes=2, max_processed_jobs=1,
                 max_queued_jobs=8, logger=None):
        self.socket_address = socket_address
        self.socket_family = socket_family
        self.socket_kwargs = {}
        self.socket_listener = None
        self.connection_semaphore = Semaphore(max_connections)
        self.connection_handlers = ThreadSet()
        self.connection_timeout = connection_timeout
        self.connection_poll_timeout = poll_timeout

        self.logger = logger or getLogger(LOGGER_NAME)

        # set verbose multi-processes debugging
        set_stream_handler(mp_util.get_logger())
        mp_util.get_logger().setLevel(mp_util.SUBWARNING)

        self.max_queued_jobs = max_queued_jobs
        self.num_workers = num_workers
        self.num_worker_processes = num_worker_processes
        self.max_processed_jobs = max_processed_jobs

        self.job_queue = None
        self.worker_semaphore = None
        self.worker_pool = None
        self.worker_pool_manager = None

        self.request_handler = None

    def run(self):
        """ Start the daemon. """
        if self.socket_listener is not None:
            raise RuntimeError(
                "An attempt to start again an already running daemon instance!"
            )

        self.logger.info("Starting daemon ...")

        self.job_queue = Queue(self.max_queued_jobs)
        self.worker_semaphore = ProcessSemaphore(self.num_workers)
        self.worker_pool = ProcessPool(
            max(self.num_workers, self.num_worker_processes), init_worker,
            maxtasksperchild=self.max_processed_jobs
        )
        self.worker_pool_manager = WorkerPoolManagerThread(
            self.worker_pool, self.worker_semaphore, self.job_queue
        )
        self.worker_pool_manager.start()

        try:
            # set signal handlers
            signal(SIGINT, self.terminate)
            signal(SIGTERM, self.terminate)
            # create the listener for incoming connections
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
            # start handling new connections
            self.logger.info("Daemon is listening to new connections ...")
            while True:
                try:
                    connection = self.socket_listener.accept()
                except OSError as error:
                    if error.errno == EBADF and self.socket_listener is None:
                        break
                    raise
                ConnectionHandlerThread(
                    ServerProtocol(self.job_queue, self.logger),
                    connection,
                    self.connection_semaphore,
                    self.connection_handlers,
                    self.connection_timeout,
                    self.connection_poll_timeout,
                ).start()
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
            socket_listener = self.socket_listener
            self.socket_listener = None
            socket_listener.close()

        # terminate threads and processes
        self.worker_pool_manager.stop()
        self.worker_pool.terminate()
        for item in list(self.connection_handlers):
            item.stop()

        # join the pending threads
        self.worker_pool_manager.join()
        self.worker_pool.join()
        for item in list(self.connection_handlers):
            item.join()

        self.worker_pool_manager = None
        self.worker_pool = None
        self.worker_semaphore = None
        self.job_queue = None

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
        count = ServerProtocol(self.job_queue, self.logger).reload_jobs()
        self.logger.info(
            "Reloaded %d stored job%s.",
            count, "" if count == 1 else "s"
        )


def main(argv):
    """ Main subroutine. """
    try:
        prm = parse_argv(argv)
    except ArgvParserError as exc:
        print_error(str(exc))
        print_usage(basename(argv[0]))
        return 1

    # configure Django settings module
    environ["DJANGO_SETTINGS_MODULE"] = prm["django_settings_module"]

    # add Django instance search path(s)
    for path in prm["search_path"]:
        if path not in sys.path:
            sys.path.append(path)

    # handling log-messages
    set_stream_handler(getLogger(), INFO)

    # initialize Django
    django.setup()

    # initialize the EOxServer component system.
    eoxs_initialize()

    # setup daemon logger
    logger = getLogger(LOGGER_NAME)
    set_stream_handler(logger, INFO)
    logger.info("Daemon is initialized.")

    # load configuration
    conf = get_wps_config()

    # socket address
    if "address" in prm:
        family, address = prm["address"]
    elif conf.socket_address:
        family, address = "AF_INET", conf.socket_address
    elif conf.socket_file:
        family, address = "AF_UNIX", conf.socket_file
    else:
        print_error("Neither address nor socket file configured!")
        return 1

    try:
        Daemon(
            socket_family=family,
            socket_address=address,
            max_connections=conf.socket_max_connections,
            connection_timeout=conf.socket_connection_timeout,
            poll_timeout=conf.socket_poll_timeout,
            num_workers=conf.num_workers,
            num_worker_processes=conf.num_worker_processes,
            max_processed_jobs=conf.max_processed_jobs,
            max_queued_jobs=conf.max_queued_jobs,
            logger=logger,
        ).run()
    except Exception as exc:
        logger.error("Daemon failed! %s", exc, exc_info=True)
        raise

    return 0


def print_error(message, *args):
    """ Print error message. """
    print(f"ERROR: {message % args}", file=stderr)


def print_usage(exename):
    """ Print simple usage. """
    print(
        f"USAGE: {exename} [-a|--address <ip4addr>:<port>]"
        " <eoxs-settings-module> [<eoxs-instance-path>]", file=stderr
    )


class ArgvParserError(Exception):
    """ Argumens' Parser exception. """


def parse_argv(argv):
    """ Parse command-line arguments. """
    data = {
        'search_path': [],
    }
    it_argv = iter(argv[1:])

    try:
        for arg in it_argv:
            if arg in ('-a', '--address'):
                data['address'] = ('AF_INET', inet_address(next(it_argv)))
            else:
                if 'django_settings_module' in data:
                    data['search_path'].append(arg)
                else:
                    data['django_settings_module'] = arg

        if 'django_settings_module' not in data:
            raise StopIteration

    except ValueError as error:
        raise ArgvParserError(str(error)) from None
    except StopIteration:
        raise ArgvParserError('Not enough input arguments') from None

    return data


if __name__ == "__main__":
    sys.exit(main(sys.argv))
