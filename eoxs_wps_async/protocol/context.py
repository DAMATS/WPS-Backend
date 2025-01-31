#-------------------------------------------------------------------------------
#
# Asynchronous WPS back-end - storage context class
#
# Authors: Martin Paces <martin.paces@eox.at>
#-------------------------------------------------------------------------------
# Copyright (C) 2016-2025 EOX IT Services GmbH
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

import errno
from logging import getLogger
from os import makedirs, chdir, rmdir, listdir
from os.path import (
    join, isfile, isdir, exists, abspath, relpath, dirname, commonpath,
)
from shutil import move, rmtree
from urllib.parse import urljoin
from eoxserver.services.ows.wps.context import (
    BaseContext as BaseWpsContext, ContextError,
)
from eoxs_wps_async.util import format_exception
from eoxs_wps_async.util.files import copy_with_temp_file

LOGGER_NAME = "eoxserver.services.ows.wps"


class MissingContextError(ContextError):
    """ Exception raised when the Job context went missing while the job
    execution was in progress, most likely due to the job removal.
    This exception should lead to an immediate job termination.
    """


class BaseContext(BaseWpsContext):
    """ Base context class providing the identifier and logger properties. """

    def __init__(self, identifier, logger=None):
        super().__init__()
        self._identifier = identifier
        self._logger = logger or getLogger(__name__)

    @property
    def identifier(self):
        """ Get the context job id."""
        return self._identifier

    @property
    def logger(self):
        """ Get the context specific logger."""
        return self._logger


class PathContext(BaseContext):
    """ Basic WPS context class
    The this class manages the storage needed by the WPS output references and
    asynchronous processes.

    The context reads the configuration and creates the process temporary
    working directory and permanent directory where the results are published.

    The temporary workspace exists only during the processes execution and
    it gets automatically removed when the execution ends.

    The permanent storage contains the processing outputs and it keeps existing
    after the process termination.

    WARNING: Use with caution with multi-threading. The current directory is
    set per process and there is no concept of per-thread working directory.
    """

    def __init__(self, identifier, path_temp, path_perm, url_base, logger=None,
                 path_perm_exists=False):
        # pylint: disable=too-many-arguments
        """ Inputs:
            identifier  - a unique identifier of the context (job id.)
            path_temp   - temporary storage path (aka workspace)
            path_perm   - permanent storage path (aka output bucket)
            url_base    - public URL of the output bucket
            path_perm_exists (optional)
                        - the context can be set several times by different
                          processes.  The first time the output bucket must not
                          exist but later it must exists. Whether the output
                          bucket exists or not is controlled by this process.
        """
        super().__init__(identifier, logger=logger)
        self._path_temp = abspath(path_temp)
        self._path_perm = abspath(path_perm)
        self._url_base = url_base if url_base[-1] == '/' else url_base + '/'
        self._path_perm_exists = path_perm_exists

    def __enter__(self):
        # initialize context
        self.logger.info("Context initialized.")
        # create the temporary directory
        try:
            makedirs(self._path_temp)
        except OSError as error:
            if error.errno == errno.EEXIST:
                raise ContextError(
                    f"Temporary path must not exist! PATH={self._path_temp}"
                ) from None
            raise
        self.logger.debug("created %s", self._path_perm)
        # create the permanent directory
        try:
            if self._path_perm_exists:
                if not isdir(self._path_perm):
                    # the path must be a directory
                    raise ContextError(
                        "Permanent path is not a directory! "
                        f"PATH={self._path_perm}"
                    )
            else:
                # the directory must not exist
                try:
                    makedirs(self._path_perm)
                except OSError as error:
                    if error.errno == errno.EEXIST:
                        raise ContextError(
                            "Permanent path must not exist! "
                            f"PATH={self._path_perm}"
                        ) from None
                    raise
                self.logger.debug("created %s", self._path_temp)
        except:
            # in case of failure remove the already existing temporary directory
            rmdir(self._path_temp)
            self.logger.debug("removed %s", self._path_perm)
            raise
        # assure we stay in the temporary directory
        # (not reliable with multiple-threads)
        chdir(self._path_temp)
        self.logger.debug("directory changed to  %s", self._path_temp)
        return self

    def __exit__(self, type, value, traceback):
        # remove workspace
        self.logger.info("Context released.")
        if isdir(self._path_temp):
            # wipe out temporary workspace
            rmtree(self._path_temp)
            self.logger.debug("removed %s", self._path_temp)
        if isdir(self._path_perm) and not listdir(self._path_perm):
            # remove permanent directory if empty
            rmdir(self._path_perm)
            self.logger.debug("removed %s", self._path_perm)

    @property
    def workspace_path(self):
        """ Get the workspace path. """
        return self._path_temp

    def publish(self, path):
        """ Publish file from the local workspace and return its path
        and public URL.
        The file path must be relative to the workspace path.
        """
        self.logger.info("Publishing %s", path)
        try:
            workspace_path = self.workspace_path
            if workspace_path != commonpath([workspace_path, abspath(path)]):
                self.logger.error("Attempt to publish a non-local file %s", path)
                raise ContextError(
                    f"Only local workspace files can be published! PATH={path}"
                )
            if not isfile(path):
                self.logger.error("Attempt to publish a non-file path %s", path)
                raise ContextError(f"Only files can be published! PATH={path}")
            # publish the file
            if not isdir(self._path_perm):
                raise MissingContextError(
                    f"Permanent directory does not exist! PATH={self._path_perm}"
                )
            targer_url = urljoin(self._url_base, relpath(path, workspace_path))
            target_path = join(self._path_perm, relpath(path, workspace_path))
            if not exists(dirname(target_path)):
                makedirs(dirname(target_path))
            move(path, target_path, copy_function=copy_with_temp_file)
        except Exception as error:
            self.logger.warning("Failed to publish %s! %s", path, error)
            raise
        self.logger.debug("moved %s -> %s", path, target_path)
        return target_path, targer_url


class Context(PathContext):
    """ Context class with execute response support.
    This class extends the base context class by methods helping to serialize
    and export the stored execute response XML document.
    """
    RESPONSE_FILE = "executeResponse.xml"

    def __init__(self, encoder, callbacks=None, **kwargs):
        """ Inputs:
            encoder     - execute response encoder
            path_temp   - temporary storage path (aka workspace)
            path_perm   - permanent storage path (aka output bucket)
            url_base    - public URL of the output bucket
            path_perm_exists (optional)
                        - the context can be set several times by different
                          processes.  The first time the output bucket must not
                          exist but later it must exists. Whether the output
                          bucket exists or not is controlled by this process.
        """
        super().__init__(**kwargs)
        encoder.status_location = urljoin(
            self._url_base, self.RESPONSE_FILE
        )
        self.encoder = encoder
        self.callbacks = callbacks
        self._progress = 0

    def _callback(self, name, *args, **kwargs):
        """ Execute user-defined callback. """
        callback = getattr(self.callbacks, name, None)
        if callback:
            self.logger.info("Executing %s() callback ...", name)
            try:
                callback(self, *args, **kwargs)
            except Exception as error:
                self.logger.error(
                    "%s() callback failed! %s",
                    name, format_exception(error), exc_info=True
                )
            self.logger.info("... %s() callback completed.", name)

    @property
    def status_location(self):
        """ Get the status location URL """
        return self.encoder.status_location

    def update_response(self, response):
        """ Update the stored execute response. """
        try:
            try:
                response_path = join(self.workspace_path, self.RESPONSE_FILE)
                with open(response_path, 'wb') as fobj:
                    fobj.write(self.encoder.serialize(response)[0])
                path, url = self.publish(response_path)
            except FileNotFoundError:
                raise MissingContextError("Context does not exist!") from None
        except Exception as error:
            self.logger.warning("Failed to update the WPS execute response! %s", error)
            raise
        self.logger.debug("Response updated.")
        return path, url

    def set_accepted(self):
        """ Set the StatusAccepted stored response.
        """
        self.logger.info("status: ACCEPTED")
        self._callback('on_accepted')
        return self.update_response(self.encoder.encode_accepted())

    def set_succeeded(self, outputs):
        """ Set the StatusSucceeded stored response.
        """
        self.logger.info("status: SUCCEEDED")
        self._callback('on_succeeded', outputs)
        return self.update_response(self.encoder.encode_response(outputs))

    def set_failed(self, exception):
        """ Set the StatusAccepted stored response.
        """
        self.logger.info("status: FAILED")
        self._callback('on_failed', exception)
        return self.update_response(self.encoder.encode_failed(exception))

    def set_started(self, progress=None, message=None):
        """ Set the StatusStarted response with the new progress
        expressed in percent.  The progress therefore must be a number
        between 0 and 99.

        NOTE: This method should be called only when the process gets called
        first time or resumed after a pause.  For the regular progress updates
        use update_progress() method.
        """
        if progress is not None:
            assert 0 <= progress < 100
            self._progress = int(progress)
        self.logger.info(
            "status: STARTED %d%% %s", self._progress, message or ""
        )
        self._callback('on_started', self._progress, message)
        return self.update_response(
            self.encoder.encode_started(self._progress, message)
        )

    def set_paused(self, progress=None):
        """ Update the StatusStarted response and set the new progress
        expressed in percent.  The progress therefore must be a number
        between 0 and 99.
        """
        if progress is not None:
            assert 0 <= progress < 100
            self._progress = int(progress)
        self.logger.info("status: PAUSED %d%%", self._progress)
        self._callback('on_paused', self._progress)
        return self.update_response(self.encoder.encode_paused(self._progress))

    def update_progress(self, progress, message):
        """ Update the StatusStarted response and set the new progress
        expressed in percent.  The progress therefore must be a number
        between 0 and 99.  An optional message can be specified.
        """
        assert 0 <= progress < 100
        self._progress = int(progress)
        self.logger.info(
            "status: STARTED %d%% %s", self._progress, message or ""
        )
        self._callback('on_progress_update', self._progress, message)
        return self.update_response(
            self.encoder.encode_started(self._progress, message)
        )
