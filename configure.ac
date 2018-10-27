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

#############################################################################
# Initialization
AC_PREREQ([2.69])
AC_INIT([rma (sequential)], [0.3])
AC_CONFIG_AUX_DIR([build-aux])
AC_PRESERVE_HELP_ORDER
AC_LANG([C++])

#############################################################################
# Main makefile
AC_CONFIG_FILES([Makefile])

#############################################################################
# Set the compiler $CC and $CXX. Prefer clang over gcc
old_CFLAGS="${CFLAGS}"; old_CXXFLAGS="${CXXFLAGS}" dnl Do not automatically set -g -O2
AC_PROG_CC([clang gcc icc cc])
AC_PROG_CXX([clang++ g++ icpc c++ cxx])
CFLAGS="${old_CFLAGS}"; unset old_CFLAGS; CXXFLAGS="${old_CXXFLAGS}"; unset old_CXXFLAGS;
AX_CXX_COMPILE_STDCXX_17 dnl Ask for C++17, thanks!

#############################################################################
# Check whether the user has explicitly set CPPFLAGS, CFLAGS and CXXFLAGS. If 
# so we try to avoid polluting these flags and respect the user setting
m4_divert_push([INIT_PREPARE]) 
if test "x${CPPFLAGS}" != "x"; then ac_user_cppflags="yes"; fi
if test "x${CFLAGS}" != "x"; then ac_user_cflags="yes"; fi
if test "x${CXXFLAGS}" != "x"; then ac_user_cxxflags="yes"; fi
m4_divert_pop([INIT_PREPARE])

#############################################################################
# Link with tcmalloc ?
AC_ARG_WITH([tcmalloc], AS_HELP_STRING([--with-tcmalloc], [Whether to link the program with tcmalloc, a replacement of the glibc malloc implementation.]))
AS_IF(
    [test "x${with_tcmalloc}" = "xyes"], [tcmalloc_system=yes; tcmalloc_bundle=yes;],
    [test "x${with_tcmalloc}" = "xsystem"], [tcmalloc_system=yes;],
    [test "x${with_tcmalloc}" = "xbundled" -o "x${with_tcmalloc}" = "xbundle"], [tcmalloc_bundle=yes;],
    [test "x${with_tcmalloc}" = "x"], [], dnl nop
    [AC_MSG_FAILURE([Invalid argument for the argument --with-tcmalloc="${with_tcmalloc}". Recognised arguments are: yes, system, bundled.])]
)

# Check whether tcmalloc is already available in the system
if( test "x${tcmalloc_system}" = "xyes" ); then
    AC_SEARCH_LIBS([tc_malloc], [tcmalloc_minimal tcmalloc], [tcmalloc_avail=yes])
fi
if( test "x${tcmalloc_bundle}" = "xyes" -a "x${tcmalloc_built}" != "xyes" ); then
    abs_top_builddir=$(realpath .)
    AX_SUBDIRS_CONFIGURE([third-party/gperftools-2.7], [[--enable-minimal], [--prefix=${abs_top_builddir}/third-party/gperftools-2.7]], [], [], [])
    EXTRA_LDFLAGS="${EXTRA_LDFLAGS} -L${abs_top_builddir}/third-party/gperftools-2.7/lib -Wl,-rpath=${abs_top_builddir}/third-party/gperftools-2.7/lib  -ltcmalloc_minimal"
    tcmalloc_avail=yes;
fi

if( test "x${tcmalloc_avail}" != "xyes" ); then
    if( test "x${tcmalloc_system}" = "xyes" -o "x${tcmalloc_bundle}" = "xyes" ); then
        AC_MSG_FAILURE([Unable to link the library tcmalloc])
    fi
fi

#############################################################################
# atomic
AC_SEARCH_LIBS([atomic_signal_fence], [atomic], [],
    [ AC_MSG_ERROR([missing prerequisite: this program requires libatomic to work (dependency for the OpenBwTree implementation)]) ])

#############################################################################
# pthreads
AC_SEARCH_LIBS([pthread_create], [pthread], [],
    [ AC_MSG_ERROR([missing prerequisite: this program requires pthreads to work (dependency for sqlite3)]) ])
    
#############################################################################
# libnuma
have_libnuma="yes"
AC_CHECK_HEADERS([numaif.h numa.h], [], [have_libnuma="no"; break;], [ [/* avoid default includes */] ])
AS_IF([test x"${have_libnuma}" == x"yes"], [AC_SEARCH_LIBS([numa_available], [numa], [], [have_libnuma="no"])])
if test x"${have_libnuma}" == x"yes"; then
    AC_MSG_NOTICE([libnuma support enabled...])
    CPPFLAGS="${CPPFLAGS} -DHAVE_LIBNUMA";
else
    AC_MSG_WARN([libnuma support disabled...])
fi

#############################################################################
# libpapi (profiler support)
have_libpapi="yes"
AC_CHECK_HEADERS([papi.h], [], [have_libpapi="no"; break;], [ [/* avoid default includes */] ])
AS_IF([test x"${have_libpapi}" == x"yes"], [AC_SEARCH_LIBS([PAPI_library_init], [papi], [], [have_libpapi="no"])])
if test x"${have_libpapi}" == x"yes"; then
    AC_MSG_NOTICE([libpapi support enabled...])
    CPPFLAGS="${CPPFLAGS} -DHAVE_LIBPAPI";
else
    AC_MSG_WARN([libpapi support disabled...])
fi


#############################################################################
# dynamic loader (dlopen & co)
AC_SEARCH_LIBS([dlopen], [dl], [],
    [ AC_MSG_ERROR([missing prerequisite: this program requires the dynamic loader library (-ldl) to work (dependency for sqlite3)]) ])

#############################################################################
# Debug flags (-g)
MY_ARG_ENABLE([debug], 
    [Whether to enable the debug flags], 
    [yes no], [yes]) 
dnl _my_set_debug_flags([CFLAGS], [C])
dnl _my_set_debug_flags([CXXFLAGS], [C++])
dnl first argument is the variable with the flags, the second argument is the language
m4_defun([_my_set_debug_flags], [
    m4_pushdef([_FLAGS], [m4_translit([$1], [+], [X])FLAGS]) dnl => CFLAGS, CXXFLAGS
    [if test -n "${ac_user_]m4_tolower(_FLAGS)[}"; then]
         AC_MSG_WARN([Action --enable-debug ignored as _FLAGS has been explicitly set through command line])
    else
        AS_VAR_APPEND([_FLAGS], [[" -g"]])
        # Force clang to emit the whole debug information
        AC_LANG_PUSH([$1])
        MY_SET_CC_FLAG([_FLAGS], [-fno-limit-debug-info])
        MY_SET_CC_FLAG([_FLAGS], [-fno-omit-frame-pointer])
        AC_LANG_POP([$1])
        
    fi
    m4_popdef([_FLAGS])
])
if( test x"${enable_debug}" = x"yes" ); then
    _my_set_debug_flags([C])
    _my_set_debug_flags([C++])
fi
m4_undefine([_my_set_debug_flags])

#############################################################################
# Assertions. Possible values:
# yes => nop
# no => CPPFLAGS += -DNDEBUG
# auto => yes if the debug flags are also enabled, no otherwise
MY_ARG_ENABLE([assert], 
    [Whether to enable assertions. The option 'auto' implies the assertions are enabled when --debug is specified], 
    [yes no auto], [auto])
    
if (test x"${enable_assert}" = x"auto"); then
    if (test x"${enable_debug}" = x"yes"); then
        enable_assert=yes
    else
        enable_assert=no
    fi
fi
if (test x"${enable_assert}" = x"yes"); then
    : ; # nop
elif (test x"${enable_assert}" = x"no"); then
    CPPFLAGS="${CPPFLAGS} -DNDEBUG"
else
    AC_MSG_ERROR([Invalid value for --enable-assert: ${enable_assert}])
fi

#############################################################################
# Warning flags (-Wall)
MY_ARG_ENABLE([warnings], 
    [Whether to enable all warnings (-Wall)], 
    [yes no], [yes])
m4_defun([_my_set_warnings], [
    m4_pushdef([_FLAGS], [m4_translit([$1], [+], [X])FLAGS]) dnl => CFLAGS, CXXFLAGS
    [if test -n "${ac_user_]m4_tolower(_FLAGS)[}"; then]
        AC_MSG_WARN([Action --enable-warnings ignored as _FLAGS has been explicitly set through command line])
    [else]
        AS_VAR_APPEND([_FLAGS], [" -Wall"])
    [fi]
    m4_popdef([_FLAGS])
])
if( test x"${enable_warnings}" = x"yes" ); then
    _my_set_warnings([C])
    _my_set_warnings([CXX])
fi
m4_undefine([_my_set_warnings])

#############################################################################
# Disable the warnings for the comparisons between signed & unsigned values
AC_LANG_PUSH([C]) 
MY_SET_CC_FLAG([CFLAGS], [-Wno-sign-compare]) 
AC_LANG_POP([C])
AC_LANG_PUSH([C++])
MY_SET_CC_FLAG([CXXFLAGS], [-Wno-sign-compare])
MY_SET_CC_FLAG([CXXFLAGS], [-Wno-overloaded-virtual]) 
AC_LANG_POP([C++])

#############################################################################
# Profiling. It sets the macro -DPROFILING
MY_ARG_ENABLE([profiling], 
    [Whether to enable profiling], 
    [yes no], [no])
if( test x"${enable_profiling}" = x"yes" ); then
    if test -n "${ac_user_cppflags}"; then
        AC_MSG_WARN([Action --enable-profiling ignored as CPPFLAGS have been explicitly set through command line])
    elif test x"${have_libpapi}" != x"yes"; then 
        AC_MSG_ERROR([cannot enable profiling without linking to the library PAPI. Install the library first, then run configure again.])
    else
        AS_VAR_APPEND([CPPFLAGS], [" -DPROFILING"])
    fi
fi

#############################################################################
# Optimization flags (-O3)
MY_ARG_ENABLE([optimize], [Whether to enable the optimization flags], [yes no], [no])

m4_defun([_my_set_optimization_flags], [
    m4_pushdef([_FLAGS], [m4_translit([$1], [+], [X])FLAGS]) dnl => CFLAGS, CXXFLAGS
    [if test -n "${ac_user_]m4_tolower(_FLAGS)[}"; then]
        AC_MSG_WARN([Action --enable-optimize ignored as _FLAGS has been explicitly set through command line])
    [else]
        if( test x"${enable_optimize}" = x"yes" ); then
            AS_VAR_APPEND([_FLAGS], [[" -O3"]])
            AC_LANG_PUSH([$1])
            MY_SET_CC_FLAG([_FLAGS], [-march=native])
            MY_SET_CC_FLAG([_FLAGS], [-mtune=native])
            MY_SET_CC_FLAG([_FLAGS], [-fno-stack-protector])
            AC_LANG_POP([$1])
        else
            AS_VAR_APPEND([_FLAGS], [[" -O0"]])
        fi
    [fi]
    m4_popdef([_FLAGS])
])
_my_set_optimization_flags([C])
_my_set_optimization_flags([C++])
m4_undefine([_my_set_optimization_flags])

#############################################################################
# Switch to LLVM libc++ (-stdlib=libc++)
MY_CHECK_STDLIB_LIBCXX([CXX="$CXX -stdlib=libc++"])

dnl #############################################################################
dnl # std::filesystem (C++17)
dnl MY_CHECK_STDLIB_FILESYSTEM([have_std_filesystem="yes"], [have_std_filesystem="no"])
dnl if test x"${have_std_filesystem}" == x"no"; then
dnl    AC_SEARCH_LIBS([_ZNKSt12experimental10filesystem2v17__cxx1118directory_iteratordeEv], [stdc++fs], [],
dnl        [ AC_MSG_ERROR([missing prerequisite: this program requires the C++17 std::filesystem library]) ]
dnl    )
dnl    dnl try again, this time linking -lstdc++fs
dnl    MY_CHECK_STDLIB_FILESYSTEM([],
dnl        [ AC_MSG_ERROR([missing prerequisite: this program requires the C++17 std::filesystem library]) ])
dnl fi

#############################################################################
# Support for pma_dfr
# This is the PMA implementation from the paper M. Durand, B. Raffin, F. Faure, A Packed 
# Memory Array to Keep Moving Particles, VRIPHYS 2012
AC_CHECK_FILE([${srcdir}/third-party/pma_dfr_inria/pma.h], [
    AS_VAR_APPEND([CPPFLAGS], [" -DHAVE_PMA_DFR"])
])

#############################################################################
# Remove extra blanks from our variables
EXTRA_CPPFLAGS=$(echo ${EXTRA_CPPFLAGS} | xargs)
CPPFLAGS=$(echo ${CPPFLAGS} | xargs);
CFLAGS=$(echo ${CFLAGS} | xargs);
EXTRA_CFLAGS=$(echo ${EXTRA_CFLAGS} | xargs);
CXXFLAGS=$(echo ${CXXFLAGS} | xargs);
EXTRA_CXXFLAGS=$(echo ${EXTRA_CXXFLAGS} | xargs);
EXTRA_LDFLAGS=$(echo ${EXTRA_LDFLAGS} | xargs);
# these two variables are only for presentation, overriding won't achieve much
ALL_CFLAGS=$(echo ${EXTRA_CPPFLAGS} ${CPPFLAGS} ${EXTRA_CFLAGS} ${CFLAGS} | xargs)
ALL_CXXFLAGS=$(echo ${EXTRA_CPPFLAGS} ${CPPFLAGS} ${EXTRA_CXXFLAGS} ${CXXFLAGS} | xargs)

LIBS="${LIBS} ${EXTRA_LDFLAGS}"

#############################################################################
# CC, CXX and linker additional output variables
AC_SUBST([EXTRA_CPPFLAGS])
AC_SUBST([EXTRA_CFLAGS])
AC_SUBST([EXTRA_CXXFLAGS])

#############################################################################
# Create the configure script
AC_OUTPUT

#############################################################################
# Final summary
echo \
"-------------------------------------------------
${PACKAGE_NAME} version ${PACKAGE_VERSION}
Compiler C..........: ${CC} ${ALL_CFLAGS}
Compiler C++........: ${CXX} ${ALL_CXXFLAGS}
Linker..............: ${LIBS}
Enable assertions...: ${enable_assert}
Enable debug........: ${enable_debug}
Enable optimize.....: ${enable_optimize}

Now type 'make -j'
--------------------------------------------------"
