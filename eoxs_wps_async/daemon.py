#-------------------------------------------------------------------------------
#
# Asynchronous WPS back-end - processing daemon - executable
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
# pylint: disable=too-many-instance-attributes, too-many-arguments, no-self-use

import sys
from sys import stderr
from os import environ
from os.path import basename
from logging import getLogger, DEBUG, INFO, Formatter, StreamHandler
import multiprocessing.util as mp_util
import django
from eoxserver.core import initialize as eoxs_initialize
from eoxs_wps_async.server import Daemon
from eoxs_wps_async.protocol.config import get_wps_config, NoOptionError

LOGGER_NAME = "eoxs_wps_async.daemon"


def main(argv):
    """ Main subroutine. """
    try:
        prm = parse_argv(argv)
    except ArgvParserError as exc:
        print_error(str(exc))
        print_usage(basename(argv[0]))
        return 1

    # configure Django settings module
    environ["DJANGO_SETTINGS_MODULE"] = prm["django_settings_module"]

    # add Django instance search path(s)
    for path in prm["search_path"]:
        if path not in sys.path:
            sys.path.append(path)

    # handling log-messages
    set_stream_handler(getLogger(), INFO)

    # initialize Django
    django.setup()

    # initialize the EOxServer component system.
    eoxs_initialize()

    # setup daemon logger
    logger = getLogger(LOGGER_NAME)
    set_stream_handler(logger, INFO)
    logger.info("Daemon is initialized.")

    # set verbose multi-processes debugging
    set_stream_handler(mp_util.get_logger())
    mp_util.get_logger().setLevel(mp_util.SUBWARNING)

    # load configuration
    conf = get_wps_config()

    # socket address
    if "address" in prm:
        family, address = prm["address"]
    else:
        try:
            family, address = conf.socket_family_and_address
        except NoOptionError:
            print_error("Neither socket file or address configured!")
            return 1

    try:
        Daemon(
            socket_family=family,
            socket_address=address,
            max_connections=conf.socket_max_connections,
            connection_timeout=conf.socket_connection_timeout,
            poll_timeout=conf.socket_poll_timeout,
            num_workers=conf.num_workers,
            num_worker_processes=conf.num_worker_processes,
            max_processed_jobs=conf.max_processed_jobs,
            max_queued_jobs=conf.max_queued_jobs,
            logger=logger,
        ).run()
    except Exception as exc:
        logger.error("Daemon failed! %s", exc, exc_info=True)
        raise

    return 0


def set_stream_handler(logger, level=DEBUG):
    """ Set stream handler to the logger. """
    formatter = Formatter('%(levelname)s: %(module)s: %(message)s')
    handler = StreamHandler()
    handler.setLevel(level)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(min(level, logger.level))


def print_error(message, *args):
    """ Print error message. """
    print(f"ERROR: {message % args}", file=stderr)


def print_usage(exename):
    """ Print simple usage. """
    print(
        f"USAGE: {exename} [-a|--address <ip4addr>:<port>]"
        " <eoxs-settings-module> [<eoxs-instance-path>]", file=stderr
    )


class ArgvParserError(Exception):
    """ Argumens' Parser exception. """


def parse_argv(argv):
    """ Parse command-line arguments. """
    data = {
        'search_path': [],
    }
    it_argv = iter(argv[1:])

    try:
        for arg in it_argv:
            if arg in ('-a', '--address'):
                data['address'] = ('AF_INET', inet_address(next(it_argv)))
            else:
                if 'django_settings_module' in data:
                    data['search_path'].append(arg)
                else:
                    data['django_settings_module'] = arg

        if 'django_settings_module' not in data:
            raise StopIteration

    except ValueError as error:
        raise ArgvParserError(str(error)) from None
    except StopIteration:
        raise ArgvParserError('Not enough input arguments') from None

    return data


if __name__ == "__main__":
    sys.exit(main(sys.argv))
