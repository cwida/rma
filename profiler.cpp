/*
 * profiler.cpp
 *
 *  Created on: 3 Apr 2018
 *      Author: Dean De Leo
 */

#include "profiler.hpp"

#include <cassert>
#include <cstring>
#include <iostream> // debug only
#if defined(HAVE_LIBPAPI)
#include <papi.h>
#endif

#define ERROR(msg) RAISE_EXCEPTION(Exception, msg)

using namespace details_profiler;
using namespace std;

/*****************************************************************************
 *                                                                           *
 *   BaseProfiler                                                            *
 *                                                                           *
 *****************************************************************************/
namespace details_profiler {

// thread unsafe
bool BaseProfiler::library_initialised = false;

void BaseProfiler::initialise_library(){
    if(library_initialised) return; // already initialised
#if !defined(HAVE_LIBPAPI)
    ERROR("Library PAPI not available. Configure the program with the support for this library");
#else
    int rc = PAPI_library_init(PAPI_VER_CURRENT);
    if (rc != PAPI_VER_CURRENT){ ERROR("Library PAPI version mismatch"); }
    library_initialised = true;
#endif
}


BaseProfiler::BaseProfiler() { initialise_library(); }

int BaseProfiler::get_event_code(const char* event_name){
#if defined(HAVE_LIBPAPI)
    if(!library_initialised) ERROR("Library PAPI not initialised");

    // PAPI_event_name_to_code doesn't accept a const char* argument
    char buffer[PAPI_MAX_STR_LEN];
    strncpy(buffer, event_name, PAPI_MAX_STR_LEN);
    buffer[PAPI_MAX_STR_LEN -1] = '\0';
    int event_code { 0 };
    int rc = PAPI_event_name_to_code(buffer, &event_code);
    if(rc != PAPI_OK){ return -1; }

    PAPI_event_info_t event_info;
    rc = PAPI_get_event_info(event_code, &event_info);
    if(rc != PAPI_OK){ return -1; }

    if(IS_PRESET(event_info.event_code)){
        // check whether there exist some derivations to infer this event
        if(event_info.count > 0){
            return event_code;
        } else {
            return -1;
        }
    } else { // native event
        return event_code;
    }
#else
    return -1;
#endif
}


/*****************************************************************************
 *                                                                           *
 *   GenericProfiler                                                         *
 *                                                                           *
 *****************************************************************************/

GenericProfiler::GenericProfiler(){ }

GenericProfiler::~GenericProfiler(){
#if defined(HAVE_LIBPAPI)
    if(m_event_set != PAPI_NULL)
        unregister_events();
#endif
}
void GenericProfiler::add_events(const char* errorstring, const char* event_name){
    add_events(errorstring, &event_name, 1);
}

void GenericProfiler::add_events(const char* errorstring, const char* alternative_events[], size_t num_alternative_events){
#if defined(HAVE_LIBPAPI)

    if (m_events_sz >= m_events_capacity) ERROR("No space left to add the events: " << m_events_sz);
    int rc = -1;
    size_t i = 0;
    while(rc == -1 && i < num_alternative_events){
        rc = get_event_code(alternative_events[i]);

        // add the event code && stop the execution
        if(rc != -1){
            m_events[m_events_sz] = rc;
            m_events_sz++;
        }

        i++;
    }

    if(rc == -1) {
        cerr << "[" __FILE__ << ":" << __LINE__ << "] " << errorstring << endl;
        ERROR(errorstring);
    }
#endif
}

void GenericProfiler::register_events(){
#if defined(HAVE_LIBPAPI)
    int rc {0};

    m_event_set = PAPI_NULL;
    rc = PAPI_create_eventset(&m_event_set);
    if(rc != PAPI_OK) {
        cerr << "[" __FILE__ << ":" << __LINE__ << "] PAPI_create_eventset: " << PAPI_strerror(rc) << " (" << rc << ")" << endl;
        ERROR("Cannot create the event set (opaque object identifier for the PAPI library)");
    }

    rc = PAPI_add_events(m_event_set, m_events, m_events_sz);
    if(rc != PAPI_OK) {
        cerr << "[" __FILE__ << ":" << __LINE__ << "] PAPI_add_events: " << PAPI_strerror(rc) << " (" << rc << ")" << endl;
        ERROR("Cannot trace the interested set of events in this architecture");
    }
#endif
}

void GenericProfiler::unregister_events(){
#if defined(HAVE_LIBPAPI)
    int event_state = 0;
    int rc = PAPI_state(m_event_set, &event_state);
    if(rc == PAPI_OK && !(event_state & PAPI_STOPPED)){
        PAPI_stop(m_event_set, nullptr); // ignore rc
    }

    rc = PAPI_remove_events(m_event_set, m_events, m_events_sz);
    if(rc != PAPI_OK)
        cerr << "[" __FILE__ << ":" << __LINE__ << "] PAPI_remove_events: " << PAPI_strerror(rc) << " (" << rc << ")" << endl;

    rc = PAPI_destroy_eventset(&m_event_set);
    if(rc != PAPI_OK)
        cerr << "[" __FILE__ << ":" << __LINE__ << "] PAPI_destroy_eventset: " << PAPI_strerror(rc) << " (" << rc << ")" << endl;
#endif
}

void GenericProfiler::start(){
#if defined(HAVE_LIBPAPI)
    int rc = PAPI_start(m_event_set);
    if(rc != PAPI_OK){
        ERROR("[GenericProfiler::start] Cannot start the event set: " << PAPI_strerror(rc) << "(rc: " << rc << ")");
    }
#endif
}

void GenericProfiler::stop(long long* resultset){
#if defined(HAVE_LIBPAPI)
    int rc = PAPI_stop(m_event_set, (long long*) resultset);
    if(rc != PAPI_OK){
        ERROR("[GenericProfiler::stop] Cannot stop the event set: " << PAPI_strerror(rc) << "(rc: " << rc << ")");
    }
#endif
}

void GenericProfiler::snapshot(long long* resultset){
#if defined(HAVE_LIBPAPI)
    int rc = PAPI_accum(m_event_set, resultset);
    if(rc != PAPI_OK){
        ERROR("[GenericProfiler::snapshot] Cannot obtain a snapshot from the event set: " << PAPI_strerror(rc) << "(rc: " << rc << ")");
    }
#endif
}


} // namespace details_profiler

/*****************************************************************************
 *                                                                           *
 *   CachesProfiler                                                          *
 *                                                                           *
 *****************************************************************************/
CachesProfiler::CachesProfiler() {
    add_events("Cannot infer cache-1 faults", "PAPI_L1_DCM");
    // on my damn AMD box L3 events are uncore :/
    const char* LLC_events[] = {"PAPI_L3_DCM", "PAPI_L3_TCM", "LLC-LOAD-MISSES"};
    add_events("Cannot infer cache-3 faults", LLC_events, 3);
    const char* TLB_events[] = {"PAPI_TLB_DM", "PAPI_TLB_TM"};
    add_events("Cannot infer TLB misses", TLB_events, 2);
    register_events();
}

CachesProfiler::~CachesProfiler() { }

void CachesProfiler::start(){
    GenericProfiler::start();
}

CachesSnapshot CachesProfiler::snapshot(){
#if defined(HAVE_LIBPAPI)
    static_assert((sizeof(long long) * 3) == sizeof(m_current_snapshot), "Size mismatch, need to pass an array of types `long long'");
    GenericProfiler::snapshot((long long*) &m_current_snapshot);
    return m_current_snapshot;
#else
    return CachesSnapshot{};
#endif
}

CachesSnapshot CachesProfiler::stop(){
    CachesSnapshot m_result;

#if defined(HAVE_LIBPAPI)
    static_assert(sizeof(long long) *3 == sizeof(m_result), "Size mismatch, need to pass an array of types `long long'");
    GenericProfiler::stop((long long*) &m_result);
    m_result += m_current_snapshot;

    m_current_snapshot = {0,0,0};
#endif

    return m_result;
}

void CachesSnapshot::operator+=(CachesSnapshot snapshot){
    m_cache_l1_misses += snapshot.m_cache_l1_misses;
    m_cache_llc_misses += snapshot.m_cache_llc_misses;
    m_cache_tlb_misses += snapshot.m_cache_tlb_misses;
}

std::ostream& operator<<(std::ostream& out, const CachesSnapshot& snapshot){
    out << "L1 faults: " << snapshot.m_cache_l1_misses << ", " <<
            "LLC faults: " << snapshot.m_cache_llc_misses << ", " <<
            "TLB faults: " << snapshot.m_cache_tlb_misses;
    return out;
}

/*****************************************************************************
 *                                                                           *
 *   BranchMispredictionsProfiler                                            *
 *                                                                           *
 *****************************************************************************/

BranchMispredictionsProfiler::BranchMispredictionsProfiler() {
    add_events("Cannot infer conditional branches", "PAPI_BR_CN");
    add_events("Cannot infer branch mispredictions", "PAPI_BR_MSP");
    // on my damn AMD box L3 events are uncore :/
    add_events("Cannot infer cache-1 faults", "PAPI_L1_DCM");
    const char* LLC_events[] = {"PAPI_L3_DCM", "PAPI_L3_TCM", "LLC-LOAD-MISSES"};
    add_events("Cannot infer cache-3 faults", LLC_events, 3);
    register_events();
}

BranchMispredictionsProfiler::~BranchMispredictionsProfiler() { }

void BranchMispredictionsProfiler::start(){
    GenericProfiler::start();
}

BranchMispredictionsSnapshot BranchMispredictionsProfiler::snapshot(){
#if defined(HAVE_LIBPAPI)
    static_assert((sizeof(long long) * 4) == sizeof(m_current_snapshot), "Size mismatch, need to pass an array of types `long long'");
    GenericProfiler::snapshot((long long*) &m_current_snapshot);
    return m_current_snapshot;
#else
    return BranchMispredictionsSnapshot{};
#endif
}

BranchMispredictionsSnapshot BranchMispredictionsProfiler::stop(){
    BranchMispredictionsSnapshot m_result;

#if defined(HAVE_LIBPAPI)
    static_assert(sizeof(long long) *4 == sizeof(m_result), "Size mismatch, need to pass an array of types `long long'");
    GenericProfiler::stop((long long*) &m_result);
    m_result += m_current_snapshot;

    m_current_snapshot = {0,0,0};
#endif

    return m_result;
}

void BranchMispredictionsSnapshot::operator+=(BranchMispredictionsSnapshot snapshot){
    m_conditional_branches += snapshot.m_conditional_branches;
    m_branch_mispredictions += snapshot.m_branch_mispredictions;
    m_cache_l1_misses += snapshot.m_cache_l1_misses;
    m_cache_llc_misses += snapshot.m_cache_llc_misses;
}

std::ostream& operator<<(std::ostream& out, const BranchMispredictionsSnapshot& snapshot){
    out << "Conditional branches: " << snapshot.m_conditional_branches << ", " <<
            "Branch mispredictions: " << snapshot.m_branch_mispredictions << ", " <<
            "L1 cache faults: " << snapshot.m_cache_l1_misses << ", " <<
            "LLC cache faults: " << snapshot.m_cache_llc_misses;
    return out;
}
