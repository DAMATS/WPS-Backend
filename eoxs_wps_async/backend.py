#-------------------------------------------------------------------------------
#
# The actual implementation of the AsyncBackendInterface EOxServer component.
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
# pylint: disable=unused-argument, too-many-arguments, no-self-use

from uuid import uuid4

from eoxserver.core import Component, implements
from eoxserver.services.ows.wps.interfaces import AsyncBackendInterface
from eoxserver.services.ows.wps.exceptions import ServerBusy, NoApplicableCode

from eoxs_wps_async.util import cached_property
from eoxs_wps_async.config import get_wps_config
from eoxs_wps_async.client import Client, ClientError
from eoxs_wps_async.handler import (
    check_job_id, get_job_logger, get_response_url, get_response,
)

LOGGER_NAME = "eoxserver.services.ows.wps"


class WPSAsyncBackendBase(Component):
    """ Simple testing WPS fake asynchronous back-end. """
    implements(AsyncBackendInterface)
    supported_versions = ("1.0.0",)

    @cached_property
    def conf(self):
        """ Get configuration. """
        return get_wps_config()

    @property
    def client(self):
        """ Get connection to the execution daemon. """
        return Client(
            self.conf.socket_file,
            self.conf.socket_connection_timeout,
        )

    def execute(self, process, raw_inputs, resp_form, extra_parts=None,
                job_id=None, version="1.0.0", **kwargs):
        """ Asynchronous process execution. """
        job_id = check_job_id(job_id or str(uuid4()))
        logger = get_job_logger(job_id, LOGGER_NAME)

        with self.client as client:
            client.send((
                "EXECUTE",
                job_id,
                process.identifier,
                raw_inputs,
                resp_form,
                extra_parts
            ))
            response = client.recv()

        if response[0] == "OK":
            return job_id
        elif response[0] == "BUSY":
            raise ServerBusy("The server is busy!")
        elif response[0] == "OWSEXC":
            raise response[1]
        elif response[0] == "ERROR":
            raise NoApplicableCode(response[1], "eoxs_wps_async.daemon")
        else:
            message = "Unknown response! RESP=%r" % response[0]
            logger.error(message)
            raise ValueError(message)

    def get_response_url(self, job_id):
        """ Return response URL for the given job identifier. """
        return get_response_url(job_id, self.conf)

    def get_response(self, job_id):
        """ Get the asynchronous response document as an open Python file-like
        object.
        """
        return get_response(job_id, self.conf)

    def purge(self, job_id, **kwargs):
        """ Purge the job from the system by removing all the resources
        occupied by the job.
        """
        job_id = check_job_id(job_id)
        logger = get_job_logger(job_id, LOGGER_NAME)

        with self.client as client:
            client.send(("PURGE", job_id))
            response = client.recv()

        if response[0] == "OK":
            return
        elif response[0] == "ERROR":
            raise ClientError(response[1])
        else:
            message = "Unknown response! RESP=%r" % response[0]
            logger.error(message)
            raise ValueError(message)
