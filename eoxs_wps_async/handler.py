#-------------------------------------------------------------------------------
#
# The execution handlers
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

import re
from logging import getLogger
from os.path import join, isdir
from shutil import rmtree
from urlparse import urljoin

from eoxserver.services.ows.wps.context import Context
from eoxserver.services.ows.wps.v10.encoders import (
    WPS10ExecuteResponseXMLEncoder,
)
from eoxserver.services.ows.wps.util import (
    parse_params, InMemoryURLResolver,
    decode_raw_inputs, decode_output_requests, pack_outputs,
)

from eoxs_wps_async.util import cached_property, fix_dir, JobLoggerAdapter
from eoxs_wps_async.config import get_wps_config

LOGGER_NAME = "eoxserver.services.ows.wps"
RE_JOB_ID = re.compile(r'^[A-Za-z0-9_.-]+$')
RESPONSE_FILE = "executeResponse.xml"

class Handler(object):
    """ Simple testing WPS fake asynchronous back-end. """
    encoder = WPS10ExecuteResponseXMLEncoder()

    @cached_property
    def conf(self):
        # pylint: disable=no-self-use
        """ Get configuration. """
        return get_wps_config()

    @staticmethod
    def check_job_id(job_id):
        """ Check job id """
        if not (isinstance(job_id, basestring) and RE_JOB_ID.match(job_id)):
            raise ValueError("Invalid job identifier %r!" % job_id)
        return job_id

    @staticmethod
    def get_logger(job_id):
        """ Custom logger. """
        return JobLoggerAdapter(getLogger(LOGGER_NAME), {'job_id': job_id})

    def get_context(self, job_id, path_perm_exists=False, logger=None):
        """ Get context for the given job_id. """
        return Context(
            join(self.conf.path_temp, job_id),
            join(self.conf.path_perm, job_id),
            fix_dir(urljoin(fix_dir(self.conf.url_base), job_id)),
            logger=(logger or self.get_logger(job_id)),
            path_perm_exists=path_perm_exists,
        )

    def update_reponse(self, context, encoded_response, logger):
        """ Update the execute response. """
        with open(RESPONSE_FILE, 'wb') as fobj:
            fobj.write(
                self.encoder.serialize(encoded_response, encoding='utf-8')
            )
        path, url = context.publish(RESPONSE_FILE)
        logger.info("Response updated.")
        return path, url


    def execute(self, job_id, process, raw_inputs, resp_form, extra_parts):
        """ Asynchronous process execution. """
        self.check_job_id(job_id)
        logger = self.get_logger(job_id)

        with self.get_context(job_id, False, logger) as context:
            self.update_reponse(context, self.encoder.encode_accepted(
                #TODO: Fix the lineage output.
                process, resp_form, {}, raw_inputs, self.get_response_url(job_id)
            ), logger)

        # following core will be performed by the worker process
        with self.get_context(job_id, True, logger) as context:
            # convert process's input/output definitions to a common format
            input_defs = parse_params(process.inputs)
            output_defs = parse_params(process.outputs)

            # prepare inputs passed to the process execution subroutine
            inputs = {"context": context}
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
