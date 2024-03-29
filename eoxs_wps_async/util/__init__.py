#-------------------------------------------------------------------------------
#
# Asynchronous WPS back-end - utilities
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

import time
from logging import LoggerAdapter


class Timer:
    """ Simple timer class. """

    def __init__(self):
        self.start = None
        self.reset()

    def reset(self):
        """ Reset timer. """
        self.start = time.time()

    @property
    def elapsed_time(self):
        """ Get elapsed time. """
        return time.time() - self.start


def format_exception(exception):
    """ Convert exception to a formatted string. """
    return f"{type(exception).__name__}: {exception}"


class cached_property():
    # pylint: disable=too-few-public-methods, invalid-name
    """
    Decorator that converts a method with a single self argument into a
    property cached on the instance.
    """
    def __init__(self, func):
        self.func = func

    def __get__(self, instance, type=None):
        if instance is None:
            return self
        res = instance.__dict__[self.func.__name__] = self.func(instance)
        return res


def fix_dir(path):
    "Add trailing slash to URL path."
    return path if path is None or path[-1] == '/' else path + '/'


class JobLoggerAdapter(LoggerAdapter):
    """ Logger adapter adding job_id to the log messages. """
    def process(self, msg, kwargs):
        """Add job id to the message."""
        return f"{self.extra['job_id']}: {msg}", kwargs
