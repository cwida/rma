#define CATCH_CONFIG_MAIN
#include "third-party/catch/catch.hpp"

#include "buffered_rewired_memory.hpp"
#include "miscellaneous.hpp"
#include "rewired_memory.hpp"

using namespace std;

TEST_CASE("sanity"){
    // Allocate 4 extents, where each extent is 3 times the page size
    constexpr size_t extent_const = 3;
    constexpr size_t num_extents = 4;
    RewiredMemory rmem { extent_const, num_extents };

    REQUIRE(rmem.get_extent_size() == extent_const * get_memory_page_size());
//    REQUIRE(rmem.get_user_capacity() == rmem.get_extent_size() * num_extents);
//    REQUIRE(rmem.get_used_buffers() == 0);

    size_t array_sz = rmem.get_allocated_memory_size() / sizeof(uint64_t);
    size_t values_per_extent = rmem.get_extent_size() / sizeof(uint64_t);
    uint64_t* array = (uint64_t*) rmem.get_start_address();

    // Initialise the array
    for(size_t i = 0; i < array_sz; i++){
        array[i] = i / values_per_extent;
    }

    // Validate the values in the array
    for(size_t i = 0; i < array_sz; i++){
        REQUIRE(array[i] == i / values_per_extent);
    }

    uint64_t* vmem[num_extents];
    for(size_t i = 0; i < num_extents; i++){
        vmem[i] = (uint64_t*) (reinterpret_cast<char*>(array) + i * rmem.get_extent_size());
    }

    // Exchange the first extent with the third and the second with the fourth
    rmem.swap(vmem[0], vmem[2]);
    rmem.swap(vmem[1], vmem[3]);
    REQUIRE(vmem[0][0] == 2);
    REQUIRE(vmem[1][0] == 3);
    REQUIRE(vmem[2][0] == 0);
    REQUIRE(vmem[3][0] == 1);

//    // Acquire two buffers
//    uint64_t* buffer1 = (uint64_t*) rmem.acquire_buffer();
//    for(size_t i = 0; i < values_per_extent; i++){
//        buffer1[i] = 5;
//    }
//    uint64_t* buffer2 = (uint64_t*) rmem.acquire_buffer();
//    for(size_t i = 0; i < values_per_extent; i++){
//        buffer2[i] = 6;
//    }
//    REQUIRE(rmem.get_used_buffers() == 2);
//
//    // Current situation. User space: [2, 3, 0, 1], buffers: [5, 6]
//    // Exchange the first buffer with the third extent
//    rmem.swap_and_release(vmem[2], buffer1);
//    REQUIRE(vmem[0][0] == 2);
//    REQUIRE(vmem[1][0] == 3);
//    REQUIRE(vmem[2][0] == 5); // <- The altered extent
//    REQUIRE(vmem[3][0] == 1);
//    REQUIRE(rmem.get_used_buffers() == 1);
//
//    // Acquire a new buffer
//    uint64_t* buffer3 = (uint64_t*) rmem.acquire_buffer();
//    // A bit of low level detail, but but buffer3 should have the same virtual address of buffer1 just released
//    REQUIRE(buffer1 == buffer3);
//    for(size_t i = 0; i < values_per_extent; i++){
//        buffer3[i] = 7;
//    }
//
//    // Current situation. User space: [2, 3, 5, 1], buffers: [6, 7]
//    rmem.swap_and_release(buffer3, vmem[0]);
//    rmem.swap_and_release(vmem[2], buffer2);
//    // Expected situation. User space: [7, 3, 6, 1];
//    REQUIRE(vmem[0][0] == 7); // altered
//    REQUIRE(vmem[1][0] == 3);
//    REQUIRE(vmem[2][0] == 6); // altered
//    REQUIRE(vmem[3][0] == 1);
//
//    // All used buffers should have been released
//    REQUIRE(rmem.get_used_buffers() == 0);
}


TEST_CASE("acquire_buffer"){
    // Allocate 12 extents, where each extent is 3 times the page size
    constexpr size_t extent_const = 3;
    constexpr size_t num_extents = 12;
    BufferedRewiredMemory rmem { extent_const, num_extents };

    REQUIRE(rmem.get_extent_size() == extent_const * get_memory_page_size());
    REQUIRE(rmem.get_used_buffers() == 0);

    uint64_t* array = (uint64_t*) rmem.get_start_address();

    uint64_t* vmem[num_extents];
    for(size_t i = 0; i < num_extents; i++){
        vmem[i] = (uint64_t*) (reinterpret_cast<char*>(array) + i * rmem.get_extent_size());
        vmem[i][0] = 0; // init
    }

    // acquire 12 buffers
    uint64_t* buffers[num_extents];
    for(size_t i = 0; i < num_extents; i++){
        buffers[i] = (uint64_t*) rmem.acquire_buffer();
        buffers[i][0] = num_extents + i;
    }

    // swap and release all acquired buffers
    for(size_t i =0; i < num_extents; i++){
        rmem.swap_and_release(vmem[i], buffers[i]);
    }

    // validate the content of the memory space
    for(size_t i = 0; i < num_extents; i++){
        REQUIRE(vmem[i][0] == num_extents + i);
    }

    REQUIRE(rmem.get_used_buffers() == 0); // all employed buffers should have been released
}
