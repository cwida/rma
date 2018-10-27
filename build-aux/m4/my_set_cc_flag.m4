#
# Copyright (C) 2018 Dean De Leo, email: dleo[at]cwi.nl
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#

# Set the current compiler option if the given flag is supported
# serial 1

# MY_SET_CC_FLAG(var_cflags, flag)
# -------------------------------------------
#
# Test whether <flag> is accepted by the current compiler. If so, it appends
# it to the shell variable <var_cflags>.
# The macro depends on AX_CHECK_COMPILE_FLAG from the Autoarchive.
AC_DEFUN([MY_SET_CC_FLAG], [ AX_CHECK_COMPILE_FLAG([$2], [AS_VAR_APPEND([$1], " $2")]) ])