#-------------------------------------------------------------------------------
#
# Asynchronous WPS back-end - low-level handlers and utilities
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
# pylint: disable=unused-argument, too-many-arguments, too-many-locals
# pylint: disable=broad-except

import re
import pickle
from datetime import datetime
from logging import getLogger
from os import remove, listdir, getpid
from os.path import join, isdir, isfile, basename, getctime
from glob import iglob
from shutil import rmtree
from urllib.parse import urljoin
from eoxserver.services.ows.wps.util import InMemoryURLResolver, get_processes
from eoxserver.services.ows.wps.exceptions import OWS10Exception
from eoxserver.services.ows.wps.v10.encoders import (
    WPS10ExecuteResponseXMLEncoder
)
from eoxserver.services.ows.wps.v10.execute_util import (
    parse_params, decode_raw_inputs, decode_output_requests, pack_outputs,
)

from eoxs_wps_async.util import format_exception, fix_dir, JobLoggerAdapter
from .config import get_wps_config
from .context import Context, BaseContext, MissingContextError
from .django_db import db_connection, reset_db_connections

LOGGER_NAME = "eoxserver.services.ows.wps"
RE_JOB_ID = re.compile(r'^[A-Za-z0-9_][A-Za-z0-9_.-]*$')


def is_valid_job_id(job_id):
    """ Return true for a valid job id."""
    return bool((isinstance(job_id, str) and RE_JOB_ID.match(job_id)))


def check_job_id(job_id):
    """ Check job id. """
    # check that job id can be used in file-system paths
    if not is_valid_job_id(job_id):
        raise ValueError(f"Invalid job identifier {job_id!r}!")
    return job_id


def get_job_logger(job_id, logger_name):
    """ Create custom job-specific log adapter for the given job id and logger
    name.
    """
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
    """ Return WPS response URL for the given job identifier. """
    return urljoin(get_base_url(job_id, conf), Context.RESPONSE_FILE)


def get_response(job_id, conf=None):
    """ Return binary file stream reading the job's WPS response. """
    return open(join(get_perm_path(job_id, conf), Context.RESPONSE_FILE), "rb")


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


class JobInitializationError(Exception):
    """ Job initialization error. """


def get_process(process_id):
    """ Get the WPS process for the given process identifier. """
    for process in get_processes():
        is_async = getattr(process, 'asynchronous', False)
        if is_async and process.identifier == process_id:
            return process
    raise ValueError("Invalid process identifier {process_id!r}!")


@db_connection
def accept_job(job_id, process_id, raw_inputs, resp_form, extra_parts):
    """ Accept the received task. """
    check_job_id(job_id)
    conf = get_wps_config()
    logger = get_job_logger(job_id, LOGGER_NAME)
    process = get_process(process_id)
    encoder = WPS10ExecuteResponseXMLEncoder(process, resp_form, raw_inputs)
    context = Context(
        encoder, process, **get_context_args(job_id, False, logger, conf)
    )
    with context:
        try:
            # optional process initialization
            if hasattr(process, 'initialize'):
                process.initialize(context, raw_inputs, resp_form, extra_parts)
            context.set_accepted()
        except OWS10Exception:
            raise
        except Exception as exception:
            error_message = format_exception(exception)
            logger.error("Job initialization failed! %s", error_message, exc_info=True)
            raise JobInitializationError(error_message) from None


@db_connection
def execute_job(job_id, process_id, raw_inputs, resp_form, extra_parts):
    """ Asynchronous process execution. """
    # A generic logger is needed to allow exception logging before
    # the context specific logger adapter is created.
    logger = getLogger(LOGGER_NAME)
    try:
        check_job_id(job_id)
        # Replace the generic logger with the context specific adapter.
        logger = get_job_logger(job_id, LOGGER_NAME)
        logger.info("job execution start (pid: %s)", getpid())
        reset_db_connections()
        logger.info("DB connections refreshed.")
        conf = get_wps_config()
        process = get_process(process_id)
        encoder = WPS10ExecuteResponseXMLEncoder(process, resp_form, raw_inputs)
        context = Context(
            encoder, process, **get_context_args(job_id, True, logger, conf)
        )
        with context:
            try:
                context.set_started()

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
            except MissingContextError:
                purge_job(job_id, process_id, logger)
            except Exception as exception:
                error_message = format_exception(exception)
                logger.info("Job failed! %s", error_message)
                logger.debug(error_message, exc_info=True)
                try:
                    context.set_failed(exception)
                except MissingContextError:
                    purge_job(job_id, process_id, logger)
                except:
                    # The job status cannot be updated to the FAILED state and
                    # there is no other option than to remove the job.
                    purge_job(job_id, process_id, logger)
                    raise
            finally:
                # remove the pickled task
                task_file = get_task_path(job_id, conf)
                if isfile(task_file):
                    remove(task_file)
                    logger.debug("removed %s", task_file)

    except Exception as exception:
        logger.error("%s", format_exception(exception), exc_info=True)
    finally:
        logger.info("job execution end (pid: %s)", getpid())


def purge_job(job_id, process_id=None, logger=None):
    """ Purge the job from the system by removing all the resources
    occupied by the job.
    """
    check_job_id(job_id)
    conf = get_wps_config()

    if not logger:
        logger = get_job_logger(job_id, LOGGER_NAME)

    _remove_paths([
        get_task_path(job_id, conf),
        get_perm_path(job_id, conf),
        get_temp_path(job_id, conf),
    ], logger)

    logger.info("Job resources purged.")

    if process_id:
        process = get_process(process_id)
        if hasattr(process, 'discard'):
            try:
                process.discard(BaseContext(job_id, logger))
            except Exception as exception:
                logger.error(
                    "discard() callback failed! %s",
                    format_exception(exception), exc_info=True
                )


def reset_job(job_id, logger=None):
    """ Reset the job to its initial state by removing any temporary leftover.
    """
    check_job_id(job_id)
    conf = get_wps_config()
    _remove_paths([
        get_temp_path(job_id, conf),
    ], logger or get_job_logger(job_id, LOGGER_NAME))


def _remove_paths(paths, logger):
    """ Remove paths. """
    for path in paths:
        if isdir(path):
            rmtree(path)
            logger.debug("removed %s", path)
        elif isfile(path):
            remove(path)
            logger.debug("removed %s", path)


def get_job_info(job_id, conf=None):
    """ Get information about the job. """
    if not is_valid_job_id(job_id):
        return None

    task_path = get_task_path(job_id, conf)
    perm_path = get_perm_path(job_id, conf)
    temp_path = get_temp_path(job_id, conf)

    task_path_exists = isdir(task_path)
    perm_path_exists = isdir(perm_path)
    temp_path_exists = isdir(temp_path)

    job_exits = task_path_exists or perm_path_exists or temp_path_exists

    if not job_exits:
        return None

    return {
        "response_exists": isfile(join(perm_path, Context.RESPONSE_FILE)),
        "is_finished": not task_path_exists,
        "is_active": temp_path_exists,
    }


def list_jobs(job_ids=None, conf=None):
    """ List current jobs and information about them.
    Optionally, the list can be restricted by the provided list of job ids.
    """
    conf = conf or get_wps_config()

    def _list_dir(path, predicate):
        yield from (name for name in listdir(path) if predicate(join(path, name)))

    def _list_ids():
        yield from _list_dir(conf.path_perm, isdir)
        yield from _list_dir(conf.path_task, isfile)
        yield from _list_dir(conf.path_temp, isdir)

    job_ids = set(_list_ids() if job_ids is None else job_ids)

    return [
        (job_id, job_info) for job_id, job_info in (
            (job_id, get_job_info(job_id)) for job_id in job_ids
        ) if job_info
    ]


def load_job(job_id):
    """ Load saved pickled job parameters. """
    with open(get_task_path(job_id), "rb") as fobj:
        return pickle.load(fobj)


def save_job(job_id, timestamp, job):
    """ Save pickled job parameters. """
    with open(get_task_path(job_id), "wb") as fobj:
        pickle.dump((timestamp, job), fobj, 2)


def list_saved_jobs():
    """ list pickled jobs (base-name is a valid job id) and
    sort them by ctime
    """
    task_path = get_wps_config().path_task
    return sorted(
        (datetime.utcfromtimestamp(ctime), job_id) for ctime, job_id
        in (
            (getctime(fname), basename(fname))
            for fname in iglob(join(task_path, '*')) if isfile(fname)
        ) if is_valid_job_id(job_id)
    )
