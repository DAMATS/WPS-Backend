#-------------------------------------------------------------------------------
#
# Asynchronous WPS back-end - threading utilities
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

from threading import Lock, Condition
from functools import wraps
from collections import deque


def locked(func, lockname='_lock'):
    """ Thread-locking object method decorator. """
    @wraps(func)
    def _wrapper_(self, *args, **kwargs):
        with getattr(self, lockname):
            return func(self, *args, **kwargs)
    return _wrapper_


class ThreadSet:
    """ Thread-safe set container. """

    def __init__(self):
        self._tset = set()
        self._lock = Lock()

    @locked
    def add(self, item):
        """ Add a new item to the set. """
        self._tset.add(item)

    @locked
    def remove(self, item):
        """ Remove an item from the set. """
        self._tset.remove(item)

    @locked
    def __iter__(self):
        """ Iterate all items in the set. """
        for item in self._tset:
            yield item


class Counter:
    """ Thread-safe integer counter. """

    def __init__(self, value=0):
        self._lock = Lock()
        self._value = value

    @property
    @locked
    def value(self):
        """ Get counter value. """
        return self._value

    @locked
    def increment(self):
        """ Increment counter by one. """
        self._value += 1
        return self._value

    @locked
    def decrement(self):
        """ Increment counter by one. """
        self._value -= 1
        return self._value


class Queue:
    """ Simple thread-safe FIFO queue. """

    class Empty(Exception):
        """ Empty queue exception. """

    class Full(Exception):
        """ Full queue exception. """

    def __init__(self, maxsize=1, timeout=1.0):
        self._items = deque()
        self._lock = Lock()
        self._cond = Condition(self._lock)
        self.maxsize = maxsize
        self.timeout = timeout

    @locked
    def push(self, item, check_size=True):
        """ Push item into the queue. """
        if check_size and len(self._items) >= self.maxsize:
            raise self.Full
        self._items.append(item)
        self._cond.notify()

    @locked
    def pull(self):
        """ Pull item from the queue or wait until there is one.
        The subroutine may raise Empty if the time-out is reached.
        """
        if not self._items:
            self._cond.wait(self.timeout)
            if not self._items:
                raise self.Empty
        return self._items.popleft()

    @locked
    def filter(self, cond):
        """ Get list of all items matching the given condition. """
        return [item for item in self._items if cond(item)]

    @locked
    def remove(self, cond):
        """ Remove all the items matching the given condition. """
        new_items = deque()
        removed_items = []
        for item in self._items:
            if cond(item):
                removed_items.append(item)
            else:
                new_items.append(item)
        self._items = new_items
        return removed_items

    @locked
    def __len__(self):
        return len(self._items)

    @property
    @locked
    def items(self):
        """ Get snapshot of the items. """
        return list(self._items)

    def __iter__(self):
        """ Non-blocking iteration over a snapshot of the queue items. """
        return iter(self.items)
