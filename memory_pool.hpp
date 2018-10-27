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

#ifndef MEMORY_POOL_HPP_
#define MEMORY_POOL_HPP_

//#include <cassert>
#include <cstddef>
#include <cstdlib>
//#include <iostream>
#include <new> // std::bad_alloc
#include <stdexcept>

// forward decl.
class MemoryPool;
class CachedMemoryPool;

/**
 * A simple memory pool
 */
class MemoryPool {
private:
    char* m_buffer; // underlying storage
    size_t m_offset; // current offset in the buffer
    const size_t m_capacity; // capacity of the underlying buffer

public:
    /**
     * Create a new memory pool with the given capacity
     */
    MemoryPool(size_t capacity);

    /**
     * Destructor
     */
    ~MemoryPool();

    /**
     * Acquire a piece of `n' bytes of memory from the pool. It returns
     * the pointer to the memory on success, otherwise nullptr.
     */
    void* allocate(size_t n){
        // not enough space
        if(m_capacity - m_offset < n){ return nullptr; }

        char* ptr = m_buffer + m_offset;
        m_offset += n;

        return ptr;
    }

    /**
     * Release the whole memory previously acquired from the pool
     */
    void release(){ m_offset = 0; }

    /**
     * Return the pointer to the start of the underlying buffer
     */
    void* begin() const { return m_buffer; };

    /*
     * Return the pointer to the end of the underlying buffer
     */
    void* end() const { return m_buffer + m_capacity; }
};

/**
 * An implementation of the C++ Allocator concept, relying on a CachedMemoryPool.
 */
template<typename T>
class CachedAllocator {
    CachedMemoryPool* m_pool;
public:
    using value_type = T;

    /**
     * Constructor
     */
    CachedAllocator<T>(CachedMemoryPool* pool);

    /**
     * Allocate storage suitable for n objects
     */
    T* allocate(size_t n);

    /**
     * Release the given pointer
     */
    void deallocate(T* ptr, size_t) noexcept;

    /**
     * Required by the C++ concept
     */
    bool operator==(const CachedAllocator<T>& other) const ;
    bool operator!=(const CachedAllocator<T>& other) const ;
};


/**
 * Try to serve an allocation request from the memory pool. Otherwise
 * it acquires storage from stdlib through malloc
 */
class CachedMemoryPool{
    MemoryPool m_pool; // underlying memory pool
    int m_counter; // number of allocations currently in use from the pool

    /**
     * Obtain a chunk of n bytes in the heap
     */
    void* allocate_raw(size_t n);

    /**
     * Deallocate the given chunk of memory
     */
    void deallocate_raw(void* ptr);

public:
    /**
     *  Initialise a memory pool with the capacity defined by the console argument --memory_pool,
     *  or the default 64MB if not set
     */
    CachedMemoryPool();

    /**
     * Initialise a memory pool with the given capacity
     */
    CachedMemoryPool(size_t capacity);

    /**
     * Allocate the space of n objects of type T
     */
    template<typename T>
    T* allocate(size_t n){
        return (T*) allocate_raw(n * sizeof(T));
    }

    /**
     * Deallocate the given object
     */
    void deallocate(void* ptr){
        deallocate_raw(ptr);
    }

    /**
     * Get a C++ allocator for this memory pool
     */
    template <typename T>
    CachedAllocator<T> allocator(){ return CachedAllocator<T>(this); }

    /**
     * Equality operator
     */
    bool operator==(const CachedMemoryPool& other){
        return m_pool.begin() == other.m_pool.begin();
    }

    /**
     * Check whether there is any allocation not released yet
     */
    bool empty() const { return m_counter == 0; }
};


/*****************************************************************************
 *                                                                           *
 *   Implementation                                                          *
 *                                                                           *
 *****************************************************************************/
template<typename T>
CachedAllocator<T>::CachedAllocator(CachedMemoryPool* pool) : m_pool(pool) {
    if (pool == nullptr){ throw std::invalid_argument("Null pointer"); }
}

template<typename T>
T* CachedAllocator<T>::allocate(size_t n){
    return m_pool->allocate<T>(n);
}

template<typename T>
void CachedAllocator<T>::deallocate(T* ptr, size_t) noexcept {
    m_pool->deallocate(ptr);
}

template<typename T>
bool CachedAllocator<T>::operator==(const CachedAllocator<T>& other) const{
    return this->m_pool == other.m_pool;
}
template<typename T>
bool CachedAllocator<T>::operator!=(const CachedAllocator<T>& other) const {
    return !((*this) == other);
}

inline
void* CachedMemoryPool::allocate_raw(size_t n){
    if(n == 0) return nullptr;

    void* ptr = m_pool.allocate(n);
    if(ptr == nullptr){ // failure
        ptr = (char*) malloc(n);
        if(ptr == nullptr) throw std::bad_alloc();
    } else { // success
        m_counter++;
    }

//    std::cout << "[CachedMemoryPool::allocate_raw] ptr: " << ptr << ", n: " << n << std::endl;
    return ptr;
}

inline
void CachedMemoryPool::deallocate_raw(void* ptr){
//    std::cout << "[CachedMemoryPool::deallocate_raw] ptr: " << ptr << std::endl;
    if(m_pool.begin() <= ptr && ptr <= m_pool.end()){
        m_counter--;
        if(m_counter == 0) m_pool.release();
    } else { // rely on the standard library
        free(ptr);
    }
}

#endif /* MEMORY_POOL_HPP_ */
