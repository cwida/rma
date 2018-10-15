/*
 * memory_pool.cpp
 *
 *  Created on: 11 Feb 2018
 *      Author: Dean De Leo
 */

#include "memory_pool.hpp"

#include <cstdlib> // malloc, free
#include <new>
#include <sstream>
#include <stdexcept>

#include "configuration.hpp"
#include "console_arguments.hpp"

using namespace std;


MemoryPool::MemoryPool(size_t capacity) : m_buffer((char*) malloc(capacity)), m_offset(0), m_capacity(capacity) {
    if(m_buffer == nullptr){ // malloc error
        throw std::bad_alloc();
    }
}

MemoryPool::~MemoryPool() {
    free(m_buffer); m_buffer = nullptr;
}

static size_t get_default_capacity(){
    try {
        return ARGREF(uint64_t, "memory_pool").get();
    } catch(configuration::ConsoleArgumentError& e){
        return 64 * 1024 * 1024; // hard default, 64 MB
    }
}

CachedMemoryPool::CachedMemoryPool() : CachedMemoryPool(get_default_capacity()){ }

CachedMemoryPool::CachedMemoryPool(size_t capacity) : m_pool(capacity), m_counter(0) {  }
