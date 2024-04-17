#-------------------------------------------------------------------------------
#
# Asynchronous WPS back-end - decorators
#
# Authors: Martin Paces <martin.paces@eox.at>
#-------------------------------------------------------------------------------
# Copyright (C) 2023 EOX IT Services GmbH
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

from functools import wraps
from django.db import connections, reset_queries, close_old_connections


def all_connections_closed():
    """ Return True if all connections are closed. """
    for connection in connections.all():
        if connection.connection:
            return False
    return True


def close_connections():
    """ Force close of all opened DB connections """
    connections.close_all()


def reset_db_connections():
    """ Reset all DB connections. """
    for connection in connections.all():
        connection.close()
        connection.connect()
        connection.connection.cursor().close()


def db_connection(func):
    """ Decorator performing DB connection clean-up before and after
    the function call.
    """
    @wraps(func)
    def _db_connection_wrapper_(*args, **kwargs):
        #close_old_connections()
        close_connections()
        reset_queries()
        try:
            return func(*args, **kwargs)
        finally:
            reset_queries()
            close_connections()
    return _db_connection_wrapper_
