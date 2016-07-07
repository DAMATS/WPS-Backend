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
# pylint: disable=unused-argument, too-many-arguments, too-many-locals
# pylint: disable=unused-variable, too-few-public-methods, no-self-use

import re
from uuid import uuid4
from logging import getLogger
from os.path import join, isdir
from shutil import rmtree
from urlparse import urljoin

from eoxserver.core import Component, implements
from eoxserver.services.ows.wps.interfaces import AsyncBackendInterface
from eoxserver.services.ows.wps.context import Context
from eoxserver.services.ows.wps.exceptions import ServerBusy, NoApplicableCode

from eoxs_wps_async.util import cached_property, fix_dir, JobLoggerAdapter
from eoxs_wps_async.config import get_wps_config
from eoxs_wps_async.client import Client

LOGGER_NAME = "eoxserver.services.ows.wps.client"
RESPONSE_FILE = "executeResponse.xml"
RE_JOB_ID = re.compile(r'^[A-Za-z0-9_.-]+$')


class WPSAsyncBackendBase(Component):
    """ Simple testing WPS fake asynchronous back-end. """
    implements(AsyncBackendInterface)
    supported_versions = ("1.0.0",)

    @cached_property
    def conf(self):
        """ Get configuration. """
        return get_wps_config()

    def get_logger(self, job_id):
        """ Custom logger. """
        return JobLoggerAdapter(getLogger(LOGGER_NAME), {'job_id': job_id})

    @staticmethod
    def check_job_id(job_id):
        """ Check job id """
        if not (isinstance(job_id, basestring) and RE_JOB_ID.match(job_id)):
            raise ValueError("Invalid job identifier %r!" % job_id)
        return job_id

    def get_context(self, job_id, path_perm_exists=False, logger=None):
        """ Get context for the given job_id. """
        return Context(
            join(self.conf.path_temp, job_id),
            join(self.conf.path_perm, job_id),
            fix_dir(urljoin(fix_dir(self.conf.url_base), job_id)),
            logger=(logger or self.get_logger(job_id)),
            path_perm_exists=path_perm_exists,
        )

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
        job_id = self.check_job_id(job_id or str(uuid4()))
        logger = self.get_logger(job_id)

        with self.client as client:
            client.send((
                "EXECUTE", job_id, (process, raw_inputs, resp_form, extra_parts)
            ))
            response = client.recv()

        if response[0] == "OK":
            return job_id
        elif response[0] == "BUSY":
            raise ServerBusy("The server is busy!")
        elif response[0] == "ERROR":
            raise NoApplicableCode(response[1], "eoxs_wps_async.daemon")
        else:
            message = "Unknown response! RESP=%r" % response[0]
            logger.error(message)
            raise ValueError(message)

    def get_response_url(self, job_id):
        """ Return response URL for the given job identifier. """
        return urljoin(
            urljoin(
                fix_dir(self.conf.url_base), fix_dir(self.check_job_id(job_id))
            ), RESPONSE_FILE
        )

    def purge(self, job_id, **kwargs):
        """ Purge the job from the system by removing all the resources
        occupied by the job.
        """
        # TODO: fix me
        self.check_job_id(job_id)
        logger = self.get_logger(job_id)
        path_temp = join(self.conf.path_temp, job_id)
        if isdir(path_temp):
            rmtree(path_temp)
            logger.debug("removed %s", path_temp)
        path_perm = join(self.conf.path_perm, job_id)
        if isdir(path_perm):
            rmtree(path_perm)
            logger.debug("removed %s", path_perm)
