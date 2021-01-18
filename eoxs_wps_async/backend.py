#-------------------------------------------------------------------------------
#
# Asynchronous WPS back-end - implementation of the AsyncBackendInterface
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
# pylint: disable=unused-argument, too-many-arguments, no-self-use

from uuid import uuid4
from logging import getLogger
from eoxserver.services.ows.wps.exceptions import ServerBusy, NoApplicableCode
from eoxs_wps_async.util import cached_property
from eoxs_wps_async.config import get_wps_config
from eoxs_wps_async.client import Client, ClientError
from eoxs_wps_async.handler import (
    check_job_id, get_job_logger, get_response_url, get_response,
)

LOGGER_NAME = "eoxserver.services.ows.wps"


class WPSAsyncBackendBase():
    """ Simple testing WPS fake asynchronous back-end. """
    supported_versions = ("1.0.0",)

    def execute(self, process, raw_inputs, resp_form, extra_parts=None,
                job_id=None, version="1.0.0", **kwargs):
        """ Asynchronous process execution. """
        job_id = check_job_id(job_id or str(uuid4()))
        logger = get_job_logger(job_id, LOGGER_NAME)

        response, *payload = self._request(
            "EXECUTE",
            job_id,
            process.identifier,
            raw_inputs,
            resp_form,
            extra_parts
        )

        if response == "OK":
            return job_id
        if response == "BUSY":
            raise ServerBusy("The server is busy!")
        if response == "OWSEXC":
            raise payload[0]
        if response == "ERROR":
            raise NoApplicableCode(payload[0], "eoxs_wps_async.daemon")

        return self._handle_unknown_response(response, logger)

    def purge(self, job_id, process_id=None, **kwargs):
        """ Purge the job from the system by removing all the resources
        occupied by the job.
        If the optional process_id is provided then the discard() callback
        of the process is executed.
        """
        job_id = check_job_id(job_id)
        logger = get_job_logger(job_id, LOGGER_NAME)

        response, *payload = self._request("PURGE", job_id, process_id)

        if response == "OK":
            return
        if response == "ERROR":
            raise ClientError(payload[0])

        self._handle_unknown_response(response, logger)

    def list(self, job_ids=None, **kwargs):
        """ List current jobs. The list can be restricted by the given job_ids.
        """
        logger = getLogger(LOGGER_NAME)

        response, *payload = self._request("LIST", job_ids)

        if response == "OK":
            return payload[0]
        if response == "ERROR":
            raise ClientError(payload[0])

        return self._handle_unknown_response(response, logger)

    def get_response_url(self, job_id):
        """ Return response URL for the given job identifier. """
        return get_response_url(job_id, self._conf)

    def get_response(self, job_id):
        """ Get the asynchronous response document as an open Python file-like
        object.
        """
        return get_response(job_id, self._conf)

    @cached_property
    def _conf(self):
        """ Get configuration. """
        return get_wps_config()

    @property
    def _client(self):
        """ Get connection to the execution daemon. """

        if self._conf.socket_address:
            family, address = 'AF_INET', self._conf.socket_address
        elif self._conf.socket_file:
            family, address = 'AF_UNIX', self._conf.socket_file
        else:
            raise ClientError("Neither address nor socket file provided!")

        return Client(
            family, address,
            self._conf.socket_connection_timeout,
        )

    def _request(self, *request):
        with self._client as client:
            client.send(request)
            return client.recv()

    def _handle_unknown_response(self, response, logger):
        message = "Unknown response! RESP=%r" % response
        logger.error(message)
        raise ValueError(message)
