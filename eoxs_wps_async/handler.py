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
from traceback import format_exc
from logging import getLogger
from os import remove
from os.path import join, isdir, isfile
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
RE_JOB_ID = re.compile(r'^[A-Za-z0-9_][A-Za-z0-9_.-]*$')

def is_valid_job_id(job_id):
    """ Return true for a valid job id."""
    return bool((isinstance(job_id, basestring) and RE_JOB_ID.match(job_id)))

def check_job_id(job_id):
    """ Check job id. """
    if not is_valid_job_id(job_id):
        raise ValueError("Invalid job identifier %r!" % job_id)
    return job_id

def get_job_logger(job_id, logger_name):
    """ Custom logger. """
    return JobLoggerAdapter(getLogger(logger_name), {'job_id': job_id})


def get_task_path(job_id, conf=None):
    """ Get path to the stored task. """
    return join((conf or get_wps_config()).path_task, job_id)


def get_perm_path(job_id, conf=None):
    """ Get path of the permanent output directory. """
    return join((conf or get_wps_config()).path_perm, job_id)


def get_temp_path(job_id, conf=None):
    """ Get path of the permanent output directory. """
    return join((conf or get_wps_config()).path_temp, job_id)


def get_base_url(job_id, conf=None):
    """ Get URL of the permanent output directory."""
    conf = conf or get_wps_config()
    return urljoin(fix_dir(conf.url_base), fix_dir(job_id))


def get_response_url(job_id, conf=None):
    """ Return response URL for the given job identifier. """
    return urljoin(get_base_url(job_id, conf), Context.RESPONSE_FILE)


def get_context_args(job_id, path_perm_exists=False, logger=None, conf=None):
    """ Get context for the given job_id. """
    conf = conf or get_wps_config()
    return {
        "identifier": job_id,
        "path_temp": get_temp_path(job_id, conf),
        "path_perm": get_perm_path(job_id, conf),
        "url_base": get_base_url(job_id, conf),
        "path_perm_exists": path_perm_exists,
        "logger": logger or get_job_logger(job_id, LOGGER_NAME),
    }


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
    # A generic logger is needed to allow exception logging before
    # the context specific logger adapter is created.
    logger = getLogger(LOGGER_NAME)
    try:
        check_job_id(job_id)
        # Replace the generic logger with the context specific adapter.
        logger = get_job_logger(job_id, LOGGER_NAME) #pylint: disable=redefined-variable-type
        conf = get_wps_config()
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
                logger.debug("%s %s", type(exception).__name__, exception)
                for line in format_exc().split("\n"):
                    logger.debug(line)
                context.set_failed(exception)
            finally:
                # remove the pickled task
                task_file = get_task_path(job_id, conf)
                if isfile(task_file):
                    remove(task_file)
                    logger.debug("removed %s", task_file)

    except Exception as exception: # pylint: disable=broad-except
        logger.error("%s %s", type(exception).__name__, exception)
        for line in format_exc().split("\n"):
            logger.debug(line)
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

    paths = [
        get_task_path(job_id, conf),
        get_perm_path(job_id, conf),
        get_temp_path(job_id, conf),
    ]

    for path in paths:
        if isdir(path):
            rmtree(path)
            logger.debug("removed %s", path)
        elif isfile(path):
            remove(path)
            logger.debug("removed %s", path)
