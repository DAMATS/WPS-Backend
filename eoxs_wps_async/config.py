#-------------------------------------------------------------------------------
#
# Configuration parser.
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

from eoxserver.core.decoders import config
from eoxserver.core.config import get_eoxserver_config

# default maximum number of simultaneous connections
DEF_MAX_CONNECTIONS = 128

# default "silent" connection time-out in seconds
DEF_CONNECTION_TIMEOUT = 10.0

# default socket polling time-out in seconds
DEF_POLL_TIMEOUT = 1.0


class WPSConfigReader(config.Reader):
    # pylint: disable=too-few-public-methods
    """ WPS backend configuration reader. """
    section = "services.ows.wps"
    path_temp = config.Option(required=True)
    path_perm = config.Option(required=True)
    url_base = config.Option(required=True)
    socket_file = config.Option(required=True)
    socket_max_connections = config.Option(
        type=int, default=DEF_MAX_CONNECTIONS
    )
    socket_connection_timeout = config.Option(
        type=float, default=DEF_CONNECTION_TIMEOUT
    )
    socket_poll_timeout = config.Option(
        type=float, default=DEF_POLL_TIMEOUT
    )


def get_wps_config(config=None):
    """ Get WPS configuration. """
    return WPSConfigReader(config or get_eoxserver_config())
