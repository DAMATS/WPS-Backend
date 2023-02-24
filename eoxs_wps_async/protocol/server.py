#-------------------------------------------------------------------------------
#
# Asynchronous WPS back-end - server-side request/response handling
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

from datetime import datetime
from collections import namedtuple
from eoxs_wps_async.util import format_exception
from .handler import (
    JobInitializationError, OWS10Exception,
    check_job_id, accept_job, load_job, save_job, purge_job,
    list_jobs, reset_job, list_saved_jobs, execute_job,
)

JobEntry = namedtuple("JobEntry", ["timestamp", "process_id", "job_id"])


class ServerProtocol:
    """ Handling of incoming requests.

    Parameters:
        job_queue - job queue object
        logger - logger object
    """

    class Break(Exception):
        """ Custom break exception. """

    def __init__(self, job_queue, logger):
        self.job_queue = job_queue
        self.logger = logger
        self.handlers = {
            "EXECUTE": self.handle_execute_request,
            "PURGE": self.handle_purge_request,
            "LIST": self.handle_list_request,
            "LIST_QUEUE": self.handle_list_queue_request,
            "GET_QUEUE_SIZE": self.handle_get_queue_size_request,
        }

    def reload_jobs(self):
        """ Reload saved jobs and enqueue them for processing. """

        def _equeue_job(timestamp, job_id):
            """ Enqueue job ignoring the queue size limit. """
            reset_job(job_id)
            # try to retrieve job details from the saved file
            timestamp, (job_id, process_id, *_) = load_job(job_id)
            self.job_queue.push(
                JobEntry(timestamp, process_id, job_id),
                check_size=False # ingnore queue size limits
            )

        count = 0
        for timestamp, job_id in list_saved_jobs():
            try:
                _equeue_job(timestamp, job_id)
            except Exception as error:
                self.logger.error(
                    "Failed to reload job %s! %s",
                    job_id, format_exception(error), exc_info=True
                )
            else:
                count += 1
                self.logger.debug("Enqueued job %s.", job_id)

        return count

    def dequeue_job(self):
        """ Dequeue job and return a callable with its parameters.

        Raises ServerProtocol.Break exception if the job cannot be dequeued.
        """

        def _pull_job_id():
            """ Dequeue next job id. """
            try:
                # the pull command blocks until the timeout is reached
                job_entry = self.job_queue.pull()
            except self.job_queue.Empty:
                # timeout has been reached while the queue remains empty
                raise self.Break from None
            return job_entry.job_id

        def _load_job(job_id):
            """ Load picked job data. """
            try:
                _, job = load_job(job_id)
            except Exception as error:
                self.logger.error(
                    "Failed to unpickle job %s! Job skipped! %s",
                    job_id, format_exception(error), exc_info=True
                )
                raise self.Break from None
            return job

        job_id, *args = _load_job(_pull_job_id())

        return execute_job, (job_id, *args)

    def handle_request(self, request):
        """ Request handler. """

        try:
            action, *payload = request
        except (ValueError, TypeError):
            return self.error_response("Malformed request!")

        try:
            handler = self.handlers[action]
        except KeyError:
            return self.error_response(f"Unsupported request {action}!")

        try:
            response = handler(payload)

        except OWS10Exception as exception:
            response = self.ows_exception_response(exception)

        except JobInitializationError as error:
            # error in the user-defined process initialization
            response = self.error_response(str(error))

        except Exception as error:
            error_message = format_exception(error)
            self.logger.error(error_message, exc_info=True)
            response = self.error_response(error_message)

        return response


    def handle_execute_request(self, payload):
        """ Enqueue a new job for asynchronous execution. """
        try:
            job_id, process_id, *data = payload
        except ValueError:
            return self.error_response("Bad request!")

        check_job_id(job_id)
        timestamp = datetime.utcnow()
        try:
            # save the pickled job
            save_job(job_id, timestamp, (job_id, process_id, *data))
            # initialize the context and the stored response
            accept_job(job_id, process_id, *data)
            # enqueue the job for execution
            self.job_queue.push(JobEntry(timestamp, process_id, job_id))
        except self.job_queue.Full:
            purge_job(job_id, process_id)
            return self.busy_response()
        except:
            purge_job(job_id, process_id)
            raise
        return self.ok_response()

    def handle_purge_request(self, payload):
        """Wipe out job and all resources it uses.
        If the optional process is provided then the discard() callback
        of the process is executed.
        """
        try:
            job_id, process_id, *_ = *payload, None
        except ValueError:
            return self.error_response("Bad request!")

        check_job_id(job_id)
        # remove enqueued job
        for item in self.job_queue.remove(lambda item: item.job_id == job_id):
            # try to fill the blank process id from the queued item
            if process_id is None:
                process_id = item.process_id
        # remove files and directories
        purge_job(job_id, process_id)
        return self.ok_response()

    def handle_list_request(self, payload):
        """ List all jobs and their status. """
        try:
            job_ids, *_ = *payload, None
        except ValueError:
            return self.error_response("Bad request!")

        return self.ok_response(list_jobs(job_ids=job_ids))

    def handle_list_queue_request(self, payload):
        """ List queued jobs. """
        try:
            job_ids, *_ = *payload, None
        except ValueError:
            return self.error_response("Bad request!")

        items = self.job_queue.items

        if job_ids is not None:
            job_ids = set(job_ids)
            items = [item for item in items if item.job_id in job_ids]

        return self.ok_response([tuple(item) for item in items])

    def handle_get_queue_size_request(self, payload):
        """ Get number of queued jobs. """
        del payload
        return self.ok_response(len(self.job_queue))

    @classmethod
    def ok_response(cls, *args):
        """ OK response. """
        return cls._response("OK", *args)

    @classmethod
    def busy_response(cls):
        """ Busy response. """
        return cls._response("BUSY")

    @classmethod
    def timeout_response(cls):
        """ Timeout response. """
        return cls._response("TIMEOUT")

    @classmethod
    def error_response(cls, error):
        """ Error response. """
        return cls._response("ERROR", error)

    @classmethod
    def ows_exception_response(cls, error):
        """ Error response. """
        return cls._response("OWSEXC", error)

    @staticmethod
    def _response(type_, *payload):
        """ Base response. """
        return (type_, *payload)
