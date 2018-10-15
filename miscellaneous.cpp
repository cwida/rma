/*
 * miscellaneous.cpp
 *
 *  Created on: 10 Jan 2017
 *      Author: Dean De Leo
 */
#include "miscellaneous.hpp"

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <cstdint> // rand
#include <cstdio> // popen
#include <cstring> // strerror
//#if __has_include(<filesystem>)
//#include <filesystem>
//#else
//#include <experimental/filesystem>
//#define WITH_EXPERIMENTAL_FILESYSTEM
//#endif
#include <iostream>
#include <libgen.h>
#include <memory>
#if defined(HAVE_LIBNUMA)
#include <numa.h>
#endif
#include <pthread.h>
#include <random>
#include <sched.h>
#include <sstream>
#include <sys/sysinfo.h> // get_nprocs
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <unistd.h> // gethostname, getcwd, syscall

#include "configuration.hpp"

#if !__has_include(<sys/memfd.h>)
#define MEMFD_CREATE_WRAPPER
#endif

using namespace std;
//#if defined(WITH_EXPERIMENTAL_FILESYSTEM)
//using namespace std::experimental; // filesystem
//#endif
//namespace fs = filesystem; // shortcut

// C++ standard library
static void shuffle_array(uint64_t seed, pair<int64_t, int64_t>* array, size_t N){
  using random_generator_t = mt19937_64;
  random_generator_t random_generator{seed};
  pair<int64_t, int64_t>* __restrict A = array;
  A[0] = std::pair<int64_t, int64_t>{1, 1000}; // init the first position
  for(size_t i = 1; i < N; i++){
    size_t j = uniform_int_distribution<size_t>{0, i -1}(random_generator);
    A[i] = A[j];
    A[j] = std::pair<int64_t, int64_t>{i +1, (i+1) * 1000}; // ptr to A[i];
  }
}

ptr_elements_t generate_array(size_t size, uint64_t seed){
    ptr_elements_t ptr(new pair<int64_t, int64_t>[size]);
    pair<int64_t, int64_t>* elts = ptr.get();
    shuffle_array(seed, elts, size);
    return ptr;
}

string random_string(size_t length){
    const string entries = "abcdefghijklmenopqrstuvxyzABCDEFGHIJKLMNOPQRSTUVXYZ0123456789";

    // obtain a seed from the system clock
    unsigned seed = chrono::system_clock::now().time_since_epoch().count();
    mt19937_64 generator(seed);
    uniform_int_distribution<int> distribution(0, (int) entries.size() -1);

    stringstream ss;
    for(size_t i = 0; i < length; i++){
        ss << entries[ distribution(generator) ];
    }

    return ss.str();
}

size_t to_size_t(const string& argument){
    istringstream ss(argument);
    size_t result;
    ss >> result;
    if(ss.fail()){
        RAISE_EXCEPTION(Exception, "Not a number: `" << argument << "'");
    }
    return result;
}

string hostname() {
    constexpr int len = 512;
    char buffer[len];
    auto rc = gethostname(buffer, len);
    if(rc != 0){
        RAISE_EXCEPTION(Exception, "[hostname] Cannot retrieve the hostname: " << strerror(errno)  << " (" << errno << ")");
    }
    string hostname{buffer};

    // Remove the suffix `.scilens.private' from the machines in the Scilens cluster
    const string scilens_suffix = ".scilens.private";
    if(hostname.length() >= scilens_suffix.length()){
        auto scilens_match = hostname.rfind(scilens_suffix);
        if(scilens_match != string::npos && scilens_match == hostname.length() - scilens_suffix.length()){
            hostname.replace(scilens_match, scilens_suffix.length(), nullptr, 0);
        }
    }

    return hostname;
}

int get_current_cpu(){
    return sched_getcpu();
}

int get_current_numa_node(){
    auto current_cpu = get_current_cpu();
    return get_numa_id(current_cpu);
}

int get_numa_max_node(){
    int max_node = -1;
#if defined(HAVE_LIBNUMA)
    if( numa_available() != -1)
        max_node = numa_max_node();
#endif
    return max_node;
}

int get_numa_id(int cpu_id){
    int numa_node = -1;
#if defined(HAVE_LIBNUMA)
    if( numa_available() != -1)
        numa_node = numa_node_of_cpu(cpu_id);
#endif
    return numa_node;
}

void pin_thread_to_cpu(){
    auto current_cpu = get_current_cpu();
    pin_thread_to_cpu(current_cpu);
}

void pin_thread_to_cpu(int target_cpu, bool print_to_stdout) {
    auto current_thread = pthread_self();
    cpu_set_t cpu_set;
    CPU_ZERO(&cpu_set);
    CPU_SET(target_cpu, &cpu_set);
    auto rc = pthread_setaffinity_np(current_thread, sizeof(cpu_set), &cpu_set);
    if (rc != 0) { RAISE_EXCEPTION(Exception, "[pin_thread_to_cpu] pthread_setaffinity_np, rc: " << rc); }

#if defined(HAVE_LIBNUMA)
    const bool use_libnuma { (numa_available() != -1) };
    auto current_node = use_libnuma ? numa_node_of_cpu(target_cpu) : -1;
#endif

    if (print_to_stdout && config().verbose()) {
        cout << "[pin_thread_to_cpu] Thread " << current_thread << " pinned to cpu: " << target_cpu;
#if defined(HAVE_LIBNUMA)
        cout << ", node: " << current_node;
#endif
        cout << endl;
    }

#if defined(HAVE_LIBNUMA)
    if (use_libnuma) {
//        numa_set_bind_policy(true); // not required: numa_set_membind is already strict (MPOL_BIND)
        auto nodemask_deleter = [](struct bitmask* ptr) {
            numa_free_nodemask(ptr);
        };
        unique_ptr<struct bitmask, decltype(nodemask_deleter)> nodemask_ptr {
                numa_allocate_nodemask(), nodemask_deleter };
        auto nodemask = nodemask_ptr.get();
        numa_bitmask_setbit(nodemask, current_node);
        numa_set_membind(nodemask);
    }
#endif
}

void pin_thread_to_random_cpu(){
    auto num_processors = get_nprocs(); // <sys/sysinfo.h>
    unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
    std::default_random_engine generator{ seed };
    auto outcome = uniform_int_distribution<int>(0, num_processors -1)(generator);
    pin_thread_to_cpu(outcome);
}

static void throw_error_numa_not_available(){
    RAISE_EXCEPTION(Exception, "[pin_thread_to_numa_node] NUMA is not available in this system");
}
#if !defined(HAVE_LIBNUMA)
void pin_thread_to_numa_node(int numa_node){
    throw_error_numa_not_available();
}
#else
void pin_thread_to_numa_node(int numa_node){
    if(numa_available() < 0){ throw_error_numa_not_available(); }
    int rc = numa_run_on_node(numa_node);
    if(rc != 0){
        RAISE_EXCEPTION(Exception, "[pin_thread_to_numa_node] Cannot pin the given node: " << numa_node << ", rc: " << rc << ", error: " << strerror(errno) << " (" << errno << "), ");
    }
}
#endif


void unpin_thread() {
    auto current_thread = pthread_self();
    if(config().verbose()){
        auto current_cpu = sched_getcpu();

        cout << "[unpin_thread] Thread: " << current_thread << ", cpu: " << current_cpu;
#if defined(HAVE_LIBNUMA)
        if(numa_available() != -1){
            cout << ", node: " << numa_node_of_cpu(current_cpu);
        }
#endif
        cout << endl;
    }

    cpu_set_t cpu_set;
    CPU_ZERO(&cpu_set);
    for(size_t i = 0, n = get_nprocs(); i < n; i++){ CPU_SET(i, &cpu_set); }
    auto rc = pthread_setaffinity_np(current_thread, sizeof(cpu_set), &cpu_set);
    if (rc != 0) { RAISE_EXCEPTION(Exception, "[unpin_thread] pthread_setaffinity_np, rc: " << rc); }

#if defined(HAVE_LIBNUMA)
    if(numa_available() != -1){
        numa_set_localalloc(); // MPOL_DEFAULT
    }
#endif
}

size_t get_memory_page_size() {
    if(!configuration::use_huge_pages()){
        long result = sysconf(_SC_PAGESIZE);
        if (result <= 0){ // a page should have at least 1 byte
            RAISE_EXCEPTION(Exception, "[get_memory_page_size] sysconf(_SC_PAGESIZE), error: " << strerror(errno) << " (" << errno << ")");
        }
        return static_cast<size_t>(result);
    } else {
        return (1ull << 21); /* 2 Mb */
    }
}

#if defined(MEMFD_CREATE_WRAPPER)
int memfd_create(const char* name, unsigned int flags){
    return syscall(SYS_memfd_create, name, flags);
}
#endif

vector<string> split(const string& s, char delimiter){
    vector<string> result;
    stringstream ss{s};
    string item;
    while(getline(ss, item, delimiter)){
        item.erase(begin(item), find_if(begin(item), end(item), [](int c){ return !isspace(c); }));
        item.erase(find_if(rbegin(item), rend(item), [](int c){ return !isspace(c); }).base(), end(item));
        if(!item.empty()) result.push_back(item);
    }

    return result;
}

string to_string_with_unit_suffix(size_t n){
    stringstream ss;
    double d = n;
    static const char* units[] = { "bytes", "KB", "MB", "GB", "TB "};
    static const size_t units_sz  = sizeof(units) / sizeof(units[0]);
    auto i = 0u;
    while(d > 1024 && i < units_sz){
        d /= 1024;
        i++;
    }

    ss << to_string_2f(d) << " " << units[i];
    return ss.str();
}

string to_string_with_time_suffix(uint64_t time, bool is_microseconds){
    if(!is_microseconds) return to_string_with_time_suffix(time * 1000, true);

    static const char* units[] = { "microsecs", "millisecs", "seconds" };
    constexpr size_t units_sz  = sizeof(units) / sizeof(units[0]);
    auto i = 0u;
    double d = time;
    while(d >= 1000.0 && (i+1) < units_sz){
        d /= 1000.0;
        i++;
    }

    stringstream ss;
    ss << to_string_2f(d) << " " << units[i];
    return ss.str();
}

string to_string_2f(double v){
    char buffer[64];
    snprintf(buffer, 64, "%.2f", v);
    return string(buffer);
}

/*********************************************************************************************************************
 *                                                                                                                   *
 *  git_last_commit()                                                                                                *
 *                                                                                                                   *
 *********************************************************************************************************************/
#if 0
namespace {
    // Helper object to restore the original working directory
    struct RestoreWorkingDirectory{
        std::string m_wd;
        RestoreWorkingDirectory(const std::string& old_working_directory) : m_wd(old_working_directory) { }
        ~RestoreWorkingDirectory(){ chdir(m_wd.c_str()); }
    };
}

static bool file_exists(const string& path){
    //FIXME: C++17 <filesystem> => fs::exists(path)
    struct stat stat_;
    int rc = stat(path.c_str(), &stat_);
    if (rc == 0)
        return true;
    else if ( errno == ENOENT ){
        return false;
    } else {
        // we don't really know, but whatever the error we are not able to access this file.
        // Treat it like it does not exist.
        return false;
    }
}

static string basedir_executable(){
    auto process_id = getpid();
    stringstream ss;
    ss << "/proc/" << process_id << "/exe";
    string strpath = ss.str();

    /*
     * Even though it is recognised as a symlink,  unfortunately fs::read_link(path) does
     * not seem to work (yet) with these types of links :/
     */
    constexpr size_t buffer_sz = 512;
    char buffer[buffer_sz];
    int rc = readlink(strpath.c_str(), buffer, buffer_sz);
    if (rc == -1){
        cerr << "[git_last_commit] Cannot read the link " << strpath << endl;
        return "";
    }
    strpath = string(buffer, rc);

    // FIXME: C++17 <filesystem>
//    fs::path path = strpath;
//    return path.parent_path();

    auto strlength = min(buffer_sz -1, strpath.size());
    memcpy(buffer, strpath.c_str(), strlength);
    buffer[strlength] = '\0';
    return dirname(buffer);
}

static string get_srcdir_from_makefile(){
//    if(!fs::exists("Makefile")) return "";
    if(!file_exists("Makefile")) return "";

    constexpr size_t buffer_sz = 512;
    char buffer[buffer_sz];

    FILE* fp = popen("make -n -p Makefile | awk '/^srcdir\\s*:?=/ {print $NF}'", "r");
    if(fp == nullptr){
        cerr << "[git_last_commit] WARNING: Cannot execute make: " << strerror(errno) << endl;
        return "";
    }
    char* result = fgets(buffer, buffer_sz, fp);
    if(result != buffer){
        cerr << "[git_last_commit] WARNING: Cannot read the result from make: " << strerror(errno) << endl;
    } else {
        // (chomp) truncate the string at the first '\n'
        strtok(result, "\n");
    }

    fclose(fp);
    return result;
}

static string git_read_last_commit(){
    FILE* fp = popen("git log -1 | awk 'NR == 1 && $1 == \"commit\" {print $2}'", "r");

    constexpr size_t buffer_sz = 512;
    char buffer[buffer_sz];

    if(fp == nullptr){
        cerr << "[git_last_commit] WARNING: Cannot retrieve the last git version: " << strerror(errno) << endl;
        return "";
    }

    char* result = fgets(buffer, buffer_sz, fp);
    if(result != buffer){
        cerr << "[git_last_commit] WARNING: Cannot read the result from git: " << strerror(errno) << endl;
    } else {
        // (chomp) truncate the string at the first '\n'
        strtok(result, "\n");
    }

    fclose(fp);
    return result;
}
#endif

string git_last_commit(){
//    { // First try to find the last git commit from the executable path
//
//        // helper buffer
//        constexpr size_t buffer_sz = 512;
//        char buffer[buffer_sz];
//
//        // Save the current working directory
//        char* tmp = getcwd(buffer, buffer_sz);
//        if(tmp == nullptr){
//            cerr << "[git_last_commit] ERROR: cannot retrieve the current working directory: " << strerror(errno) << endl;
//            return "";
//        }
//        RestoreWorkingDirectory current_working_dir{tmp};
//
//        // first of all try to understand where the executable is
//        auto basedir = basedir_executable();
//        if(!basedir.empty()){
//            int rc = chdir(basedir.c_str());
//            if( rc != 0 ){
//                cerr << "[git_last_commit] ERROR: cannot change the current working directory to " << basedir << ": " << strerror(errno) << endl;
//            } else {
//                // try to get the source directory from the makefile (in case of out-of-the-source build)
//                auto srcdir = get_srcdir_from_makefile();
//                if(!srcdir.empty()){
//                    rc = chdir(srcdir.c_str());
//                    if( rc != 0 ){
//                        cerr << "[git_last_commit] ERROR: cannot change the current working directory to " << srcdir << ": " << strerror(errno) << endl;
//                    }
//                }
//
//                // now try to find the git commit from this working directory
//                string git_commit = git_read_last_commit();
//                if(!git_commit.empty()) return git_commit;
//            }
//        }
//    }
//
//    // Otherwise try with the current directory
//    return git_read_last_commit();
    return "not supported";
}

string tuple2str(int64_t A[], size_t N){
    stringstream ss;
    ss << "[";
    for(size_t i = 0; i < N; i++){
        if(i > 0) ss << ", ";
        ss << A[i];
    }
    ss << "]";
    return ss.str();
}

size_t hyperceil(size_t value){
    return (size_t) pow(2, ceil(log2(static_cast<double>(value))));
}

template<int num_elements>
static void aligned_gather_single(int64_t* __restrict destination, int64_t* __restrict source){
    int64_t* __restrict source_aligned = (int64_t*) __builtin_assume_aligned(source, CACHELINE);
    for(int i = 0; i < num_elements; i++){
        destination[i] = source_aligned[i];
    }
}
template<int num_elements>
static void aligned_gather_multiple(int64_t* __restrict destination, int64_t* __restrict source, int numblocks){
    for(int i = 0; i < numblocks; i++){
        aligned_gather_single<num_elements>(destination, source);

        source += ELEMENTS_PER_CACHELINE;
        destination += num_elements;
    }
}

void aligned_gather(int64_t* destination, int64_t* source, int num_blocks, int elements_per_block) noexcept{
    static_assert(ELEMENTS_PER_CACHELINE == 8, "Broken implementation!");
    switch(elements_per_block){
    case 0: /* nop */; break;
    case 1: aligned_gather_multiple<1>(destination, source, num_blocks); break;
    case 2: aligned_gather_multiple<2>(destination, source, num_blocks); break;
    case 3: aligned_gather_multiple<3>(destination, source, num_blocks); break;
    case 4: aligned_gather_multiple<4>(destination, source, num_blocks); break;
    case 5: aligned_gather_multiple<5>(destination, source, num_blocks); break;
    case 6: aligned_gather_multiple<6>(destination, source, num_blocks); break;
    case 7: aligned_gather_multiple<7>(destination, source, num_blocks); break;
    case 8: memcpy(destination, source, num_blocks * ELEMENTS_PER_CACHELINE * sizeof(destination[0])); break;
    default: assert(num_blocks == 0 && "Invalid value for `elements per block'");
    }
}


template<int num_elements>
static void aligned_scatter_single(int64_t* __restrict destination, int64_t* __restrict source){
    int64_t* __restrict destination_aligned = (int64_t*) __builtin_assume_aligned(destination, CACHELINE);
    for(int i = 0; i < num_elements; i++){
        destination_aligned[i] = source[i];
    }
}

template<int num_elements>
static void aligned_scatter_multiple(int64_t* __restrict destination, int64_t* __restrict source, int numblocks){
    for(int i = 0; i < numblocks; i++){
        aligned_scatter_single<num_elements>(destination, source);

        source += num_elements;
        destination += ELEMENTS_PER_CACHELINE;
    }
}

void aligned_scatter(int64_t* destination, int64_t* source, int num_blocks, int elements_per_block) noexcept {
    static_assert(ELEMENTS_PER_CACHELINE == 8, "Broken implementation!");
    switch(elements_per_block){
    case 0: /* nop */; break;
    case 1: aligned_scatter_multiple<1>(destination, source, num_blocks); break;
    case 2: aligned_scatter_multiple<2>(destination, source, num_blocks); break;
    case 3: aligned_scatter_multiple<3>(destination, source, num_blocks); break;
    case 4: aligned_scatter_multiple<4>(destination, source, num_blocks); break;
    case 5: aligned_scatter_multiple<5>(destination, source, num_blocks); break;
    case 6: aligned_scatter_multiple<6>(destination, source, num_blocks); break;
    case 7: aligned_scatter_multiple<7>(destination, source, num_blocks); break;
    case 8: memcpy(destination, source, num_blocks * ELEMENTS_PER_CACHELINE * sizeof(destination[0])); break;
    default: assert(num_blocks == 0 && "Invalid value for `elements per block'");
    }
}

void interleaved_gather(int64_t* __restrict destination, int64_t* __restrict source, size_t blocks_per_segment, size_t source_sz) noexcept {
    size_t elements_per_block = source_sz / blocks_per_segment;
    size_t overfilled_blocks = source_sz % blocks_per_segment;
    aligned_gather(destination, source, overfilled_blocks, elements_per_block +1);
    source += overfilled_blocks * ELEMENTS_PER_CACHELINE;
    destination += overfilled_blocks * (elements_per_block +1);
    aligned_gather(destination, source, blocks_per_segment - overfilled_blocks, elements_per_block);
}

void interleaved_scatter(int64_t* __restrict destination, int64_t* __restrict source, size_t blocks_per_segment, size_t destination_sz) noexcept {
    size_t elements_per_block = destination_sz / blocks_per_segment;
    size_t overfilled_blocks = destination_sz % blocks_per_segment;
    aligned_scatter(destination, source, overfilled_blocks, elements_per_block +1);
    source += overfilled_blocks * (elements_per_block +1);
    destination += overfilled_blocks * ELEMENTS_PER_CACHELINE;
    aligned_scatter(destination, source, blocks_per_segment - overfilled_blocks, elements_per_block);
}

