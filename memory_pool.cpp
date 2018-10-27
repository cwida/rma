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
