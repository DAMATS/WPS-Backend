#-------------------------------------------------------------------------------
#
# The actual implementation if the EoxServer's AsyncBackendInterface component.
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
# pylint: disable=wrong-import-order

import re
from uuid import uuid4
from logging import getLogger, LoggerAdapter
from os.path import join
from urlparse import urljoin
from eoxserver.core import Component, implements
from eoxserver.core.config import get_eoxserver_config
from eoxserver.services.ows.wps.interfaces import AsyncBackendInterface
from eoxserver.services.ows.wps.context import Context
from eoxserver.services.ows.wps.config import WPSConfigReader
from eoxserver.services.ows.wps.v10.encoders import (
    WPS10ExecuteResponseXMLEncoder,
)
from eoxserver.services.ows.wps.util import (
    parse_params, InMemoryURLResolver,
    decode_raw_inputs, decode_output_requests, pack_outputs,
)

LOGGER_NAME = "eoxserver.services.ows.wps"
RESPONSE_FILE = "executeResponse.xml"
RE_JOB_ID = re.compile(r'^[A-Za-z0-9_.-]+$')


def fix_dir(path):
    "Add trailing slash to URL path."
    return path if path is None or path[-1] == '/' else path + '/'


class JobLoggerAdapter(LoggerAdapter):
    """ Logger adapter adding job_id to the log messages. """
    def process(self, msg, kwargs):
        """Add job id to the message."""
        return '%s: %s' % (self.extra['job_id'], msg), kwargs


class WPSAsyncBackend(Component):
    """ Simple testing WPS fake asynchronous back-end. """
    implements(AsyncBackendInterface)
    supported_versions = ("1.0.0",)
    encoder = WPS10ExecuteResponseXMLEncoder()

    @staticmethod
    def get_logger(job_id):
        """ Custom logger. """
        return JobLoggerAdapter(getLogger(LOGGER_NAME), {'job_id': job_id})

    def update_reponse(self, context, encoded_response, logger):
        """ Update the execute response. """
        with open(RESPONSE_FILE, 'wb') as fobj:
            fobj.write(
                self.encoder.serialize(encoded_response, encoding='utf-8')
            )
        path, url = context.publish(RESPONSE_FILE)
        logger.info("Updating response: %s -> %s ", path, url)

    def execute(self, process, raw_inputs, resp_form, extra_parts=None,
                job_id=None, version="1.0.0", **kwargs):
        """ Asynchronous process execution. """
        # load WPS path configuration
        conf = WPSConfigReader(get_eoxserver_config())

        # prepare job_id value
        job_id = str(job_id or uuid4())
        if not RE_JOB_ID.match(job_id):
            raise ValueError("Invalid job identifier %r!" % job_id)

        logger = self.get_logger(job_id)
        context = Context(
            join(conf.path_temp, job_id), join(conf.path_perm, job_id),
            fix_dir(urljoin(fix_dir(conf.url_base), job_id)), logger
        )

        with context:
            logger.debug("Execute process %s", process.identifier)

            self.update_reponse(context, self.encoder.encode_accepted(
                #TODO: Fix the lineage output.
                process, resp_form, {}, raw_inputs, self.get_response_url(job_id)
            ), logger)


            # convert process's input/output definitions to a common format
            input_defs = parse_params(process.inputs)
            output_defs = parse_params(process.outputs)

            # prepare inputs passed to the process execution subroutine
            inputs = {
                "context": context
            }
            inputs.update(decode_output_requests(resp_form, output_defs))
            inputs.update(decode_raw_inputs(
                raw_inputs, input_defs, InMemoryURLResolver(extra_parts, logger)
            ))

            # execute the process
            outputs = process.execute(**inputs)

            # pack the outputs
            packed_outputs = pack_outputs(outputs, resp_form, output_defs)

            self.update_reponse(context, self.encoder.encode_response(
                process, packed_outputs, resp_form, inputs, raw_inputs
            ), logger)

        return job_id

    def get_response_url(self, job_id):
        """ Return response URL for the given job identifier. """
        conf = WPSConfigReader(get_eoxserver_config())
        return urljoin(
            urljoin(fix_dir(conf.url_base), fix_dir(job_id)), RESPONSE_FILE
        )
