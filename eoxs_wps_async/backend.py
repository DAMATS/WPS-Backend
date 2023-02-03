#-------------------------------------------------------------------------------
#
# Asynchronous WPS back-end - implementation of the AsyncBackendInterface
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

from logging import getLogger
from eoxs_wps_async.util import cached_property
from eoxs_wps_async.client import Client, ClientError
from eoxs_wps_async.protocol import ClientProtocol
from eoxs_wps_async.protocol.handler import get_response_url, get_response
from eoxs_wps_async.protocol.config import get_wps_config, NoOptionError

LOGGER_NAME = "eoxserver.services.ows.wps"


class WPSAsyncBackend:
    """ WPS asynchronous back-end. """
    supported_versions = ("1.0.0",)

    def execute(self, process, raw_inputs, resp_form, extra_parts=None,
                job_id=None, version="1.0.0", **kwargs):
        """ Asynchronous process execution. """
        self._send_request(self._protocol.execute_request(
            job_id, process.identifier, raw_inputs, resp_form, extra_parts,
        ))
        return job_id

    def purge(self, job_id, process_id=None, **kwargs):
        """ Purge the job from the system by removing all the resources
        occupied by the job.
        If the optional process_id is provided then the discard() callback
        of the process is executed.
        """
        self._send_request(self._protocol.purge_request(job_id, process_id))

    def list(self, job_ids=None, **kwargs):
        """ List current jobs. The list can be restricted by the given job_ids.
        """
        job_ids, *_ = self._send_request(self._protocol.list_request(job_ids))
        return job_ids

    def list_queue(self, job_ids=None, **kwargs):
        """ List queued jobs. The list can be restricted by the given job_ids.
        """
        jobs, *_ = self._send_request(self._protocol.list_queue_request(job_ids))
        return jobs

    def get_queue_size(self, **kwargs):
        """ Get number of queued jobs.
        """
        size, *_ = self._send_request(self._protocol.get_queue_size_request())
        return size

    def get_response_url(self, job_id):
        """ Return response URL for the given job identifier. """
        return get_response_url(job_id, self._conf)

    def get_response(self, job_id):
        """ Get the asynchronous response document as an open readable binary
        file-like object.
        """
        return get_response(job_id, self._conf)

    def _send_request(self, request, logger=None):
        """ Send request and handle response. """

        def _request(request):
            with self._client as client:
                client.send(request)
                return client.recv()

        return self._protocol.handle_response(
            _request(request), logger=logger
        )

    @cached_property
    def _conf(self):
        """ Get configuration. """
        return get_wps_config()

    @cached_property
    def _protocol(self):
        """ Get client protocol instance. """
        return ClientProtocol(getLogger(LOGGER_NAME))

    @property
    def _client(self):
        """ Get connection to the execution daemon. """
        try:
            family, address = self._conf.socket_family_and_address
        except NoOptionError:
            raise ClientError("Neither socket file or address configured!") from None

        return Client(family, address, self._conf.socket_connection_timeout)


# alias for backward compatibility
WPSAsyncBackendBase = WPSAsyncBackend
