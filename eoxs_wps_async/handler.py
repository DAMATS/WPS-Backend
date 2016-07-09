#-------------------------------------------------------------------------------
#
# The WPS back-end low level handlers and their utilities
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

from eoxserver.services.ows.wps.util import InMemoryURLResolver
from eoxserver.services.ows.wps.v10.encoders import (
    WPS10ExecuteResponseXMLEncoder
)
from eoxserver.services.ows.wps.v10.execute_util import (
    parse_params, decode_raw_inputs, decode_output_requests, pack_outputs,
)

from eoxs_wps_async.util import fix_dir, JobLoggerAdapter
from eoxs_wps_async.config import get_wps_config
from eoxs_wps_async.context import Context

LOGGER_NAME = "eoxserver.services.ows.wps"
RE_JOB_ID = re.compile(r'^[A-Za-z0-9_.-]+$')

def check_job_id(job_id):
    """ Check job id """
    if not (isinstance(job_id, basestring) and RE_JOB_ID.match(job_id)):
        raise ValueError("Invalid job identifier %r!" % job_id)
    return job_id


def get_job_logger(job_id, logger_name):
    """ Custom logger. """
    return JobLoggerAdapter(getLogger(logger_name), {'job_id': job_id})


def get_context_args(job_id, path_perm_exists=False, logger=None, conf=None):
    """ Get context for the given job_id. """
    conf = conf or get_wps_config()
    return {
        "path_temp": join(conf.path_temp, job_id),
        "path_perm": join(conf.path_perm, job_id),
        "url_base": fix_dir(urljoin(fix_dir(conf.url_base), job_id)),
        "path_perm_exists": path_perm_exists,
        "logger": logger or get_job_logger(job_id, LOGGER_NAME),
    }


def get_response_url(job_id, conf=None):
    """ Return response URL for the given job identifier. """
    conf = conf or get_wps_config()
    return urljoin(
        urljoin(
            fix_dir(conf.url_base), fix_dir(check_job_id(job_id))
        ), Context.RESPONSE_FILE
    )

def accept_job(job_id, process, raw_inputs, resp_form, extra_parts):
    """ Accept the received task. """
    check_job_id(job_id)
    conf = get_wps_config()
    logger = get_job_logger(job_id, LOGGER_NAME)
    encoder = WPS10ExecuteResponseXMLEncoder(process, resp_form, raw_inputs)
    context = Context(encoder, **get_context_args(job_id, False, logger, conf))
    with context:
        context.set_accepted()


def execute_job(job_id, process, raw_inputs, resp_form, extra_parts):
    """ Asynchronous process execution. """
    try:
        check_job_id(job_id)
        conf = get_wps_config()
        logger = get_job_logger(job_id, LOGGER_NAME)
        encoder = WPS10ExecuteResponseXMLEncoder(process, resp_form, raw_inputs)
        context = Context(
            encoder, **get_context_args(job_id, True, logger, conf)
        )
        with context:
            context.set_started()
            try:
                # convert process's input/output definitions to a common format
                input_defs = parse_params(process.inputs)
                output_defs = parse_params(process.outputs)

                # prepare inputs passed to the process execution subroutine
                inputs = {"context": context}
                inputs.update(decode_output_requests(resp_form, output_defs))
                inputs.update(decode_raw_inputs(
                    raw_inputs, input_defs,
                    InMemoryURLResolver(extra_parts, logger)
                ))

                # execute the process
                outputs = process.execute(**inputs)

                # pack the outputs
                context.set_succeeded(
                    pack_outputs(outputs, resp_form, output_defs)
                )
            except Exception as exception: # pylint: disable=broad-except
                context.set_failed(exception)

    except Exception as exception: # pylint: disable=broad-except
        return exception
    else:
        return None


def purge_job(job_id, logger=None):
    """ Purge the job from the system by removing all the resources
    occupied by the job.
    """
    check_job_id(job_id)
    conf = get_wps_config()
    logger = logger or get_job_logger(job_id, LOGGER_NAME)

    dirs = [
        join(conf.path_temp, job_id),
        join(conf.path_perm, job_id),
    ]

    for path in dirs:
        if isdir(path):
            rmtree(path)
            logger.debug("removed %s", path)