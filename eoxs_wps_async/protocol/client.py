#-------------------------------------------------------------------------------
#
# Asynchronous WPS back-end - client-side request/response handling
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
# pylint: disable=unused-argument, too-many-arguments, no-self-use

from uuid import uuid4
from eoxserver.services.ows.wps.exceptions import ServerBusy, NoApplicableCode
from eoxs_wps_async.handler import check_job_id, is_valid_job_id


class ClientProtocol:
    """ Client side request/response protocol. """

    def __init__(self, logger):
        self.logger = logger

    def handle_response(self, response, logger=None):
        """ Handle server response. """

        response_type, *payload = response

        if response_type == "OK":
            return payload
        if response_type == "BUSY":
            raise ServerBusy("The server is busy!")
        if response_type == "OWSEXC":
            raise payload[0]
        if response_type == "ERROR":
            raise NoApplicableCode(payload[0], "eoxs_wps_async.daemon")

        return self._handle_unknown_response(response_type, logger)

    def _handle_unknown_response(self, response_type, logger=None):
        logger = logger or self.logger
        message = f"Unknown response! RESP={response_type!r}"
        logger.error(message)
        raise ValueError(message)

    @classmethod
    def execute_request(cls, job_id, *payload):
        """ Get execute request. """
        job_id = check_job_id(job_id or str(uuid4()))
        return cls._request("EXECUTE", job_id, *payload)

    @classmethod
    def purge_request(cls, job_id, *payload):
        """ Get job purge request. """
        job_id = check_job_id(job_id)
        return cls._request("PURGE", job_id, *payload)

    @classmethod
    def list_request(cls, job_ids=None):
        """ Get list jobs request. """
        if job_ids is not None:
            # convert to a list and reject invalid job ids
            job_ids = [id_ for id_ in job_ids if is_valid_job_id(id_)]
        return cls._request("LIST", job_ids)

    @classmethod
    def list_queue_request(cls, job_ids=None):
        """ Get list queued jobs request. """
        if job_ids is not None:
            # convert to a list and reject invalid job ids
            job_ids = [id_ for id_ in job_ids if is_valid_job_id(id_)]
        return cls._request("LIST_QUEUE", job_ids)

    @classmethod
    def get_queue_size_request(cls):
        """ Get get queue size request. """
        return cls._request("GET_QUEUE_SIZE")

    @staticmethod
    def _request(*request):
        """ Request serialization. """
        return tuple(request)
