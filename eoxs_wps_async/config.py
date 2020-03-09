#-------------------------------------------------------------------------------
#
# Asynchronous WPS back-end - configuration parser
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

try:
    from configparser import NoOptionError, NoSectionError
except ImportError:
    # Python 2.*
    from ConfigParser import NoOptionError, NoSectionError

from eoxserver.core.decoders.config import Reader, Option
from eoxserver.core.config import get_eoxserver_config

# maximum allowed time out value in seconds
MAX_TIME_OUT = 3600.0 # 1 hour

# default maximum number of simultaneous connections
DEF_MAX_CONNECTIONS = 128

# default "silent" connection time-out in seconds
DEF_CONNECTION_TIMEOUT = 10.0

# default socket polling time-out in seconds
DEF_POLL_TIMEOUT = 1.0

# default number of parallel workers
DEF_NUMBER_OF_WORKERS = 1

# default allowed maximum of queued jobs
DEF_MAX_QUEUED_JOBS = 64

# default number of processed jobs before worker restarts
DEF_MAX_NUMBER_OF_PROCESSED_JOBS = 1


def positive_int(value):
    """ Positive integer parser """
    value = int(value)
    if value > 0:
        return value
    raise ValueError("Not a positive integer!")


def positive_float_range(min_value, max_value):
    """ Positive integer parser """
    def _positive_float_range_(value):
        value = float(value)
        if min_value <= value <= max_value:
            if value > 0:
                return value
            raise ValueError("Not a positive float!")
        raise ValueError(
            "Float value is in the allowed range [%g, %g]!" %
            (min_value, max_value)
        )
    return _positive_float_range_


class WPSConfigReader(Reader):
    # pylint: disable=too-few-public-methods
    """ WPS backend configuration reader. """
    section = "services.ows.wps"
    path_temp = Option(required=True)
    path_perm = Option(required=True)
    path_task = Option(required=True)
    url_base = Option(required=True)
    socket_file = Option(required=True)
    socket_max_connections = Option(
        type=positive_int, default=DEF_MAX_CONNECTIONS
    )
    socket_connection_timeout = Option(
        type=positive_float_range(0.0, MAX_TIME_OUT),
        default=DEF_CONNECTION_TIMEOUT
    )
    socket_poll_timeout = Option(
        type=positive_float_range(0.0, MAX_TIME_OUT), default=DEF_POLL_TIMEOUT
    )
    num_workers = Option(
        type=positive_int, default=DEF_NUMBER_OF_WORKERS
    )
    max_processed_jobs = Option(
        type=positive_int, default=DEF_MAX_NUMBER_OF_PROCESSED_JOBS
    )
    max_queued_jobs = Option(
        type=positive_int, default=DEF_MAX_QUEUED_JOBS
    )

    @property
    def num_worker_processes(self):
        """ Get number of worker processes. """
        try:
            return max(self.num_workers, positive_int(self._config.get(
                self.section, 'num_worker_processes'
            )))
        except (NoOptionError, NoSectionError):
            return 2*self.num_workers


_WPS_CONFIG = None
def get_wps_config(config=None):
    """ Get WPS configuration. """
    global _WPS_CONFIG # pylint: disable=global-statement
    if not _WPS_CONFIG:
        _WPS_CONFIG = load_wps_config(config)
    return _WPS_CONFIG


def load_wps_config(config=None):
    """ Get WPS configuration. """
    return WPSConfigReader(config or get_eoxserver_config())
