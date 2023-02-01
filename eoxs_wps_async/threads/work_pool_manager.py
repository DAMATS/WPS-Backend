#-------------------------------------------------------------------------------
#
# Asynchronous WPS back-end - Worker Pool Manager thread object
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
from threading import Thread, Event
from eoxs_wps_async.util import format_exception
from eoxs_wps_async.handler import load_job, execute_job


LOGGER_NAME = "eoxs_wps_async.daemon"


class WorkerPoolManagerThread(Thread):
    """ Thread managing the worker pool.

    The purpose of this thread is to pull jobs from the job queue
    and pass the job to be executed in the worker pool.

    The provided semaphore constraints the number of the simultaneously
    executed jobs.

    Parameters:
        worker_pool - worker pool (multiprocessing.Pool instance)
        semaphore - worker semaphore (multiprocessing.Semaphore instance)
        job_queue - job queue (eoxs_wps_async.util.thread.Queue instance)
        timeout - optional semaphore time-out in seconds (set to 1s by default)
    """

    class _Break(Exception):
        """ Break exception. """

    def __init__(self, worker_pool, semaphore, job_queue, timeout=1.0):
        Thread.__init__(self)
        self._pool = worker_pool
        self._semaphore = semaphore
        self._job_queue = job_queue
        self._stop_event = Event()
        self.timeout = timeout
        self.logger = None

    def stop(self):
        """ Tell the thread to stop. """
        self._stop_event.set()

    def run(self):
        """ Thread connection handler. """
        self.logger = getLogger(LOGGER_NAME)

        try:
            self._process_jobs()
        except Exception as error:
            self.logger.error(
                "WPM: Thread failed! %s",
                format_exception(error), exc_info=True
            )

    def _process_jobs(self):
        self.logger.debug("WPM: START")

        while not self._stop_event.is_set():

            if not self._semaphore.acquire(True, self.timeout):
                # timeout has been reached without acquiring of the semaphore
                continue

            self.logger.debug("WPM: SEMAPHORE ACQUIRED")

            try:
                job_id = self._pull_job_id()
                self.logger.debug("WPM: JOB %s PULLED", job_id)

                job = self._load_job(job_id)
                self.logger.debug("WPM: JOB %s UNPICKLED", job_id)

                self._apply_job(*job)
                self.logger.debug("WPM: JOB %s APPLIED", job_id)

            except self._Break:
                self._semaphore.release()
                self.logger.debug("WPM: SEMAPHORE RELEASED")
                continue

        self.logger.debug("WPM: STOP")

    def _pull_job_id(self):
        """ Pull job form the job queue. """
        while not self._stop_event.is_set():
            try:
                # the pull command blocks until the timeout is reached
                job_entry = self._job_queue.pull()
            except self._job_queue.Empty:
                # timeout has been reached while the queue remains empty
                continue
            return job_entry.job_id
        raise self._Break

    def _load_job(self, job_id):
        """ Load picked job data. """
        try:
            _, job = load_job(job_id)
        except Exception as error:
            self.logger.error(
                "WPM: Failed to unpickle job %s! Job skipped! %s",
                job_id, format_exception(error), exc_info=True
            )
            raise self._Break from None
        return job

    def _apply_job(self, job_id, *args):
        """ Apply job to the worker pool. """
        try:
            self._pool.apply_async(
                execute_job, (job_id, *args),
                callback=self._get_exit_callback(job_id)
            )
        except Exception as error:
            self.logger.error(
                "WPM: Failed to apply asynchronous job %s! Job skipped! %s",
                job_id, format_exception(error), exc_info=True
            )
            raise self._Break from None

    def _get_exit_callback(self, job_id):
        """ Worker callback called on exit. """

        def exit_callback(exception):
            """ Job exit callback. """
            self.logger.debug("WPM: JOB %s ENDED", job_id)

            self._semaphore.release()
            self.logger.debug("WPM: SEMAPHORE RELEASED")

            if exception:
                self.logger.error(
                    "WPM: Job %s execution failed! Reason: %s",
                    job_id, format_exception(exception)
                )

        return exit_callback
