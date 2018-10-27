/**
 * Copyright (C) 2018 Dean De Leo, email: dleo[at]cwi.nl
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

#ifndef PMA_SPIN_LOCK_HPP_
#define PMA_SPIN_LOCK_HPP_

#include <cassert>
#include <iostream>
#include <pthread.h>
#include "errorhandling.hpp"

class SpinLock {
    pthread_spinlock_t m_lock;

public:
    SpinLock(){
        int rc = pthread_spin_init(&m_lock, PTHREAD_PROCESS_PRIVATE);
        if(rc != 0){ RAISE_EXCEPTION(Exception, "Cannot initialise the spin lock: " << (rc);) }
    }

    ~SpinLock(){
        int rc = pthread_spin_destroy(&m_lock);
        if(rc != 0){ std::cerr << "[ERROR] Cannot destroy the spin lock: " << rc << std::endl; }
    }

    void lock(){
#if !defined(NDEBUG)
        int rc = pthread_spin_lock(&m_lock);
        assert(rc == 0 && "Cannot acquire the spin lock");
#else
        /* ignore the return code */ pthread_spin_lock(&m_lock);
#endif
    }

    void unlock(){
#if !defined(NDEBUG)
        int rc = pthread_spin_unlock(&m_lock);
        assert(rc == 0 && "Cannot release the spin lock");
#else
        /* ignore the return code */ pthread_spin_unlock(&m_lock);
#endif
    }
};

#endif /* PMA_SPIN_LOCK_HPP_ */
