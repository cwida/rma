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

#ifndef MISCELLANEOUS_HPP_
#define MISCELLANEOUS_HPP_

#include <cinttypes>
#include <cstring> // memcpy
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "console_arguments.hpp"

/**
 * Convenience alias
 */
typedef std::unique_ptr<std::pair<int64_t, int64_t>[]> ptr_elements_t;

/**
 * Generate an array of pairs <int64_t, int64_t> of the given `size'. The elements generated:
 * - first component: these are the unique values from the sequence [1, size], randomly shuffled
 * - second component: it is equal to the first component * 1000;
 */
ptr_elements_t generate_array(size_t size, uint64_t seed = ARGREF(uint64_t, "seed_random_permutation"));

/**
 * Generate a random alpha numeric string
 */
std::string random_string(size_t length);

/**
 * Convert the content of a string to an integer (size_t). It raises a ConfigurationException in case of error
 */
std::size_t to_size_t(const std::string& argument);

/**
 * Get the current hostname where the program is running
 */
std::string hostname();

/**
 * Get the processor ID where the current thread is running
 */
int get_current_cpu();

/**
 * Get the Numa node for the given CPU
 */
int get_numa_id(int cpu_id);

/**
 * Get the Numa node associated to the CPU where the current thread is running
 */
int get_current_numa_node();

/**
 * Get the highest numa node in the system. Wrapper to `numa_max_node()'
 */
int get_numa_max_node();

/**
 * Pin the current thread to the current cpu and numa node. Disable memory allocations from other NUMA nodes.
 */
void pin_thread_to_cpu();

/**
 * Pin the current thread to the given cpu and numa node. Disable memory allocations from the other NUMA nodes.
 * Note: as new created threads will inherit the same cpu mask, it is important to invoke this call to pin
 * an execution/sequential worker, rather than the main thread.
 */
void pin_thread_to_cpu(int cpu_id, bool print_to_stdout = true);

/**
 * Pin the current thread to a random cpu, and its related numa node. Disable memory allocations from other NUMA nodes.
 */
void pin_thread_to_random_cpu();

/**
 * Pin the current thread to the CPUs running at the given NUMA node
 */
void pin_thread_to_numa_node(int numa_node);

/**
 * Get the size of a memory page for the current architecture, in bytes
 * The result is affected by the setting on huge pages  (--hugetlb)
 */
size_t get_memory_page_size();

/**
 * Split the string `s' in array of words separated by the given delimiter
 */
std::vector<std::string> split(const std::string& s, char delimiter = ',');

/**
 * Reset the pinning of the current thread
 */
void unpin_thread();

/**
 * Return a string representing the value n together with the proper unit: bytes, KB, GB, TB
 */
std::string to_string_with_unit_suffix(size_t n);

/**
 * Given a duration in either microseconds or milliseconds, retrieve a string
 * with the unit as suffix (e.g. "103 milliseconds")
 */
std::string to_string_with_time_suffix(uint64_t time, bool is_microseconds = true);

/**
 * Convert the given number into a string with 2 digits after the point.
 */
std::string to_string_2f(double v);

/**
 * Try to retrieve the last git commit. It returns the empty string in case of failure
 */
std::string git_last_commit();

/**
 * Convert the given array into a string with the format [array[0], array[1], ..., array[N-1]]
 */
std::string tuple2str(int64_t A[], size_t N);


/**
 * Profiling
 */
#if !defined(ONLY_IF_PROFILING_ENABLED)
    #if defined(PROFILING)
        #define ONLY_IF_PROFILING_ENABLED(stmt) stmt
    #else
        #define ONLY_IF_PROFILING_ENABLED(stmt)
    #endif
#endif

/**
 * Read the cpu timestamp counter
 */
inline uint64_t rdtscp(){
    uint64_t rax;
    asm volatile (
        "rdtscp ; shl $32, %%rdx; or %%rdx, %%rax; "
         : "=a" (rax)
         : /* no inputs */
         : "rcx", "rdx"
    );
    return rax;
}

/**
 * Compiler barrier
 */
inline void barrier(){
    __asm__ __volatile__("": : :"memory");
};

/**
 * Branch prediction macros
 */
#define LIKELY(x) __builtin_expect((x), 1)
#define UNLIKELY(x) __builtin_expect((x), 0)

/**
 * Prefetch the given pointer
 */
#define PREFETCH(ptr) __builtin_prefetch(ptr, /* 0 = read only, 1 = read/write */ 0 /*,  temporal locality 3 */)

/**
 * Number of bytes transferred in block between in the cache - memory hierarchy.
 * This is 64 in current Intel architectures. Still, according to Peter, Intel also
 * automatically fetches two cache lines on fault.
 * This parameter could be set by AutoTools
 */
#if !defined(CACHELINE)
#define CACHELINE 64
#endif

static_assert(CACHELINE % sizeof(int64_t) == 0, "Invalid value for the cache line");
static constexpr int ELEMENTS_PER_CACHELINE = CACHELINE / sizeof(int64_t);


void aligned_gather(int64_t* destination, int64_t* source, int num_blocks, int elements_per_block) noexcept;
void aligned_scatter(int64_t* destination, int64_t* source, int num_blocks, int elements_per_block) noexcept;
void interleaved_gather(int64_t* __restrict destination, int64_t* __restrict source, size_t blocks_per_segment, size_t source_sz) noexcept;
void interleaved_scatter(int64_t* __restrict destination, int64_t* __restrict source, size_t blocks_per_segment, size_t destination_sz) noexcept;

/**
 * 2^ceil(log2(x))
 */
size_t hyperceil(size_t x);

/**
 * Check if the given number is a power of 2
 */
template<typename T>
bool is_power_of_2(T x){
    // https://stackoverflow.com/questions/3638431/determine-if-an-int-is-a-power-of-2-or-not-in-a-single-line
    return ((x & ~(x-1))==x)? x : 0;
}

/**
 * Wrapper to the system call memfd_create, in case it's not exported by libc.
 */
int memfd_create(const char* name, unsigned int flags);

#endif /* MISCELLANEOUS_HPP_ */
