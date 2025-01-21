#-------------------------------------------------------------------------------
#
# File utilities
#
# Authors: Martin Paces <martin.paces@eox.at>
#-------------------------------------------------------------------------------
# Copyright (C) 2025 EOX IT Services GmbH
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

from os import remove, rename
from shutil import copy2


def copy_with_temp_file(src, dst, *, follow_symlinks=True):
    """ Copy file with an intermediate temporary file in the target directory,
    renamed to the destination filename once the copying is over.

    The intermediate temporary file is meant to prevent visibility of the
    incomplete destination file due to the non-atomic copy operation.

    Meant as a replacement of the shutil.copy2 function.
    """
    tmp_dst = f"{dst}.tmp"
    try:
        copy2(src, tmp_dst, follow_symlinks=follow_symlinks)
        rename(tmp_dst, src)
    except:
        try:
            remove(tmp_dst)
        except FileNotFoundError:
            pass
        raise
