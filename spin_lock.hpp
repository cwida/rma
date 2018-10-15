/*
 * spin_lock.hpp
 *
 *  Created on: 26 Aug 2018
 *      Author: Dean De Leo
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
