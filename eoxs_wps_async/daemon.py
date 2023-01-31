#-------------------------------------------------------------------------------
#
# Asynchronous WPS back-end - processing daemon
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
# pylint: disable=too-many-instance-attributes, too-many-arguments, no-self-use

import sys
from sys import stderr
from os import environ
from os.path import basename, join, getctime, isfile
from glob import iglob
from logging import getLogger, DEBUG, INFO, Formatter, StreamHandler
from time import time
from datetime import datetime
from errno import EBADF
from signal import SIGINT, SIGTERM, signal, SIG_IGN
from threading import Thread, Semaphore, Event
import pickle
import multiprocessing.util as mp_util
from multiprocessing import Semaphore as ProcessSemaphore, Pool as ProcessPool
import django
from eoxserver.core import initialize as eoxs_initialize
from eoxs_wps_async.config import get_wps_config, inet_address
from eoxs_wps_async.util import format_exception
from eoxs_wps_async.util.thread import ThreadSet, Queue
from eoxs_wps_async.util.ipc import get_listener
from eoxs_wps_async.handler import (
    check_job_id, get_task_path, accept_job, execute_job, purge_job, reset_job,
    list_jobs, is_valid_job_id, JobInitializationError, OWS10Exception,
)

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
                logger.error(
                    "Job execution failed! Reason: %s",
                    format_exception(exception),
                )

        while not self._stop_event.is_set():
            if self._semaphore.acquire(True, self.timeout):
                logger.debug("WPM: SEMAPHORE ACQUIRED")
                while not self._stop_event.is_set():
                    try:
                        _, job_id = self._job_queue.get()
                    except self._job_queue.Empty:
                        continue
                    # load the pickled job
                    try:
                        with open(get_task_path(job_id), "rb") as fobj:
                            _, job = pickle.load(fobj)
                    except: # pylint: disable=bare-except
                        logger.error(
                            "Failed to unpickle job %s! The job is ignored!"
                        )
                    self._pool.apply_async(
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
                logger.debug("New connection accepted.")
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
        except EOFError:
            logger.debug("EOFError")
        except OSError as exc:
            logger.error("OSError: %s", exc)
        except Exception as exc:
            logger.error("Connection failed! %s", exc, exc_info=True)
        finally:
            self._connection.close()
            logger.debug("Connection closed.")
            if self._thread_group:
                self._thread_group.remove(self)


class Daemon():
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
        self.worker_pool_manager = WorkerPoolManager(
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
            self.logger.info("Loading stored jobs ...")
            self.load_tasks()
            # start handling new connections
            self.logger.info("Daemon is listening to new connections ...")
            while True:
                try:
                    connection = self.socket_listener.accept()
                except OSError as error:
                    if error.errno == EBADF and self.socket_listener is None:
                        break
                    raise
                ConnectionHandler(
                    self.request_handler,
                    self.busy_response,
                    self.timeout_response,
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
        self.logger.info("Termination signal received ...")
        self.cleanup()

    def busy_response(self):
        """ Busy response. """
        return ("BUSY",)

    def timeout_response(self):
        """ Timeout response. """
        return ("TIMEOUT",)

    def request_handler(self, request):
        """ Request handler. """
        try:
            action, payload = request[0], request[1:]
            if action == "EXECUTE":
                # enqueue a new job for asynchronous execution
                timestamp = datetime.utcnow()
                # check that job id can be used in file-system paths (security)
                job_id = check_job_id(payload[0])
                process_id = payload[1]
                try:
                    # save the pickle the job (persistence)
                    with open(get_task_path(job_id), "wb") as fobj:
                        pickle.dump((timestamp, payload), fobj, 2)
                    # initialize the context and the stored response
                    accept_job(*payload)
                    # enqueue the job for execution
                    self.job_queue.put((timestamp, job_id))
                except self.job_queue.Full:
                    purge_job(job_id, process_id)
                    return ("BUSY",)
                except:
                    purge_job(job_id, process_id)
                    raise
            elif action == "PURGE":
                # wipe out job and all resources it uses
                # check that job id can be used in paths (security)
                job_id = check_job_id(payload[0])
                # if the optional process is provided then the discard() callback
                # of the process is executed
                try:
                    process_id = payload[1]
                except IndexError:
                    process_id = None
                # remove enqueued job
                self.job_queue.remove(lambda v: v[1] == job_id)
                # remove files and directories
                purge_job(job_id, process_id)
            elif action == "LIST":
                # list all jobs and their status
                job_ids = payload[0] if payload else None
                return ('OK', list_jobs(job_ids=job_ids))
            #elif action == "STATUS":
                # get status of a job
            else:
                raise ValueError("Unknown request! REQ=%r" % request[0])
        except OWS10Exception as exc:
            return ("OWSEXC", exc)
        except JobInitializationError as exc:
            # error in the user-defined process initialization
            return ("ERROR", str(exc))
        except Exception as exc:
            # error in the handler code
            error_message = "%s: %s" % (type(exc).__name__, exc)
            self.logger.error(error_message, exc_info=True)
            return ("ERROR", error_message)
        else:
            return ("OK",)

    def load_tasks(self):
        """ Load tasks after server restart. """
        task_path = get_wps_config().path_task
        # list pickled jobs (base-name is a valid job id) and sort them by ctime
        jobs = sorted(
            (ctime, job_id) for ctime, job_id in (
                (getctime(fname), basename(fname))
                for fname in iglob(join(task_path, '*')) if isfile(fname)
            ) if is_valid_job_id(job_id)
        )
        # enqueue job by passing the queue size limit
        for ctime, job_id in jobs:
            reset_job(job_id)
            self.job_queue.put((datetime.utcfromtimestamp(ctime), job_id))
            self.logger.debug("Enqueued job %s.", job_id)
        self.logger.info("Loaded %d stored jobs.", len(jobs))


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
