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

# Check whether the C++17 std::filesystem library is available
# serial

# MY_CHECK_STDLIB_FILESYSTEM([action-if-true], [action-if-false])
# -------------------------------------------
# Check whether the current compiler can compile & link has a
# simple snippet using the teh C++ std <filesystem> library
# In case of success, it executes the `action-if-true', otherwise
# `action-if-false'
AC_DEFUN([MY_CHECK_STDLIB_FILESYSTEM], [
  AS_VAR_PUSHDEF([CACHEVAR],[my_cv_check_stdlib_filesystem])
  AC_MSG_CHECKING([whether _AC_LANG compiler supports the C++17 <filesystem> library])
  AC_LINK_IFELSE([AC_LANG_SOURCE([
  #if __has_include(<filesystem>)
  #include <filesystem>
  using namespace std;
  #else
  #include <experimental/filesystem>
  using namespace std::experimental;
  #endif 
  
  int main(int argc, const char* argv @<:@@:>@  ){
      filesystem::read_symlink("/tmp/test");
      return 0;
  }
  ])],
  [AC_MSG_RESULT([yes]); AS_VAR_SET(CACHEVAR,[yes]);],
  [AC_MSG_RESULT([no]); AS_VAR_SET(CACHEVAR,[no]);]);
  AS_VAR_IF(CACHEVAR, yes, [m4_default([$1], :)], [m4_default([$2], :)])
  AS_VAR_POPDEF([CACHEVAR])
])