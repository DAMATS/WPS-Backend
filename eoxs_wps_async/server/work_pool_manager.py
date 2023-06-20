#-------------------------------------------------------------------------------
#
# Asynchronous WPS back-end - worker pool manager thread object
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

from logging import getLogger
from signal import SIGINT, SIGTERM, signal, SIG_IGN
from multiprocessing import Semaphore as ProcessSemaphore, Pool as ProcessPool
from threading import Thread, Event
from eoxs_wps_async.util import format_exception
from eoxs_wps_async.util.thread import Counter


def init_worker():
    """ Process pool initialization. """
    # ignore SIGINT and SIGTERM in the worker processes to prevent dead-locks
    signal(SIGINT, SIG_IGN)
    signal(SIGTERM, SIG_IGN)


class WorkerPoolManagerThread(Thread):
    """ Thread managing the worker pool.

    The purpose of this thread is to pull jobs from the job queue
    and pass the job to be executed in the worker pool.

    The job queue handling is done by the server protocol handler.

    The provided semaphore constraints the number of the simultaneously
    executed jobs.

    Parameters:
        handler - handler implementing the server protocol. Required items:
            handler.dequeue_job() method
            handler.Break exception
        num_workers - maximum number of active workers (>= 1)
        num_workers_processed - maximum number of worker processes (>= num_workers)
        max_processed_jobs - maximum processed job by one worker pool process
            before being restarted (None means never restarted)
        timeout - optional semaphore time-out in seconds (set to 1s by default)
        logger - optional logger
    """

    def __init__(self, handler, num_workers, num_worker_processes=None,
                 max_processed_jobs=None, timeout=1.0, logger=None):
        num_workers = max(1, num_workers)
        num_worker_processes = max(num_workers, num_worker_processes or 0)
        Thread.__init__(self)
        self.logger = logger or getLogger(__name__)
        self.timeout = timeout
        self.counter_applied = Counter()
        self.counter_completed = Counter()
        self._handler = handler
        self._stop_event = Event()
        self._semaphore = ProcessSemaphore(num_workers)
        self._pool = ProcessPool(
            processes=num_worker_processes,
            initializer=init_worker,
            maxtasksperchild=max_processed_jobs,
        )
        self.logger.debug("WPM: POOL CREATED")


    def stop(self):
        """ Tell the thread to stop. """
        self.logger.debug("WPM: SHUTDOWN")
        self._stop_event.set()
        self._pool.terminate()

    def join(self, timeout=None):
        self._pool.join()
        self.logger.debug("WPM: POOL JOINED")
        super().join(timeout)

    def run(self):
        """ Run worker pool manager. """
        try:
            self._process_jobs()
        except Exception as error:
            self.logger.error(
                "WPM: Thread failed! %s",
                format_exception(error), exc_info=True
            )

    def _process_jobs(self):
        self.logger.debug("WPM: START")

        has_semaphore = False

        while not self._stop_event.is_set():

            if not has_semaphore:
                has_semaphore = self._semaphore.acquire(True, self.timeout)
                if not has_semaphore:
                    # timeout has been reached without acquiring of the semaphore
                    continue
                self.logger.debug("WPM: SEMAPHORE ACQUIRED")

            try:
                job = self._handler.dequeue_job()
                self.logger.debug("WPM: JOB DEQUEUED")

                self._apply_job(*job)
                self.logger.debug("WPM: JOB APPLIED")

                # the successfully applied job bears now the semaphore
                has_semaphore = False

            except self._handler.Break:
                continue

        if has_semaphore:
            self._semaphore.release()
            self.logger.debug("WPM: SEMAPHORE RELEASED")

        self.logger.debug("WPM: STOP")

    def _apply_job(self, *args):
        """ Apply job to the worker pool. """
        value = self.counter_applied.increment()
        try:
            self._pool.apply_async(
                *args,
                callback=self._exit_callback,
                error_callback=self._exit_callback,
            )
        except Exception as error:
            self.counter_applied.decrement()
            self.logger.error(
                "WPM: Failed to apply asynchronous job! Job skipped! %s",
                format_exception(error), exc_info=True
            )
            raise self._handler.Break from None
        self.logger.info("%d jobs running.", value)

    def _exit_callback(self, exception):
        """ Worker callback called on exit. """

        value = self.counter_applied.decrement()
        self.logger.info("%d jobs running.", value)

        value = self.counter_completed.increment()
        self.logger.info("%d jobs executed.", value)

        if exception:
            self.logger.error(
                "WPM: Job execution failed! %s",
                format_exception(exception)
            )

        self.logger.debug("WPM: JOB COMPLETED")

        self._semaphore.release()
        self.logger.debug("WPM: SEMAPHORE RELEASED")
