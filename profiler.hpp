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

#ifndef PROFILER_HPP_
#define PROFILER_HPP_

#include <cinttypes>
#include <ostream>

#include "errorhandling.hpp"

namespace details_profiler {

    class BaseProfiler{
        static bool library_initialised;
        static void initialise_library();

    public:
        BaseProfiler();

        /**
         * Return -1 if the event is not available, otherwise it's PAPI event code
         */
        static int get_event_code(const char* event_name);
    };


    class GenericProfiler : public BaseProfiler {
        static constexpr size_t m_events_capacity = 8; // 0: L1 misses, 1: LLC misses, 2: TLB misses

    protected:
        int m_events[m_events_capacity];
        int m_events_sz = 0;
        int m_event_set = 0;

        void add_events(const char* errorstring, const char* event_name);
        void add_events(const char* errorstring, const char* alternative_events[], size_t num_alternative_events);
        void register_events();
        void unregister_events();

        void start();
        void stop(long long* resultset);
        void snapshot(long long* resultset);

    public:
        GenericProfiler();
        ~GenericProfiler();
    };

} // namespace details_profiler

struct CachesSnapshot{
    uint64_t m_cache_l1_misses = 0; // number of misses in the L1
    uint64_t m_cache_llc_misses = 0; // number of misses in the LLC (=L3 assumed)
    uint64_t m_cache_tlb_misses = 0; // number of misses in the TLB (I think from LLC, assumes page-walk)

    void operator+=(CachesSnapshot snapshot);
};
std::ostream& operator<<(std::ostream& out, const CachesSnapshot& snapshot);

class CachesProfiler : public details_profiler::GenericProfiler {
#if defined(HAVE_LIBPAPI)
    CachesSnapshot m_current_snapshot;
#endif

public:
    CachesProfiler();
    ~CachesProfiler();

    void start();
    CachesSnapshot snapshot();
    CachesSnapshot stop();
};


struct BranchMispredictionsSnapshot{
    uint64_t m_conditional_branches = 0; // total number of conditional branch instructions
    uint64_t m_branch_mispredictions = 0; // total number of branch mispredictions
    uint64_t m_cache_l1_misses = 0; // number of cache misses in the L1
    uint64_t m_cache_llc_misses = 0; // number of cache misses in the LLC (=L3 assumed)

    void operator+=(BranchMispredictionsSnapshot snapshot);
};
std::ostream& operator<<(std::ostream& out, const BranchMispredictionsSnapshot& snapshot);

struct BranchMispredictionsProfiler : public details_profiler::GenericProfiler {
#if defined(HAVE_LIBPAPI)
    BranchMispredictionsSnapshot m_current_snapshot;
#endif

public:
    BranchMispredictionsProfiler();
    ~BranchMispredictionsProfiler();

    void start();
    BranchMispredictionsSnapshot snapshot();
    BranchMispredictionsSnapshot stop();
};

#endif /* PROFILER_HPP_ */
