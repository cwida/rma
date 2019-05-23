---
Introduction
---

This is the source code of the sequential implementation and of the experiments featured in the paper ''Packed memory arrays - Rewired'' [1]. It 
consists of a single program, `pmacomp`, inclusive of the data structures tested and the code to run the simulations.
Since the program makes use of a few O.S. dependent constructs (`libnuma`, `memfd_create`), it only supports Linux.

The driver `pmacomp` and the developed data structures are licensed under the GPL v3 terms. Still, the source code contains some third-party data structures, 
included for comparison purposes, that may be licensed according to different terms.

Note: a parallel implementation of Rewired Memory Arrays has been presented in [2], the [source code is also available on github](https://github.com/cwida/rma_concurrent).


---
Build
---

Requirements:
* Linux kernel v4.17+
* Autotools, [Autoconf 2.69+](https://www.gnu.org/software/autoconf/)
* A C++17 compliant compiler. We tested it with Clang 6, Clang 7 and GCC 8.2.
* libnuma 2.0+
* [libpapi 5.5+](http://icl.utk.edu/papi/)
* As in [3], memory rewiring is performed on [huge pages](https://www.kernel.org/doc/Documentation/vm/hugetlbpage.txt). Its support may need to be explicitly enabled by a privileged user. In our environment, we set:
```bash
echo 4294967296 > /proc/sys/vm/nr_overcommit_hugepages
echo 1 > /proc/sys/vm/overcommit_memory
```

To compile the whole suite of experiments use:
```
autoreconf -iv
mkdir build && cd build
../configure --enable-optimize --disable-debug
make -j
```

There is no "install" target.

---
Running the driver
---

All experiments were executed through the driver. The general invocation pattern is:
```
./pmacomp -e <experiment_name> [experiment_params] -d <distribution> [distribution_params] -a <data_structure> [data_structure_params] -v
```
Each experiment either creates or appends, if it already exists, the outcomes of the simulation into the SQLite3 database `results.sqlite3`. The database will
contain:
* A table `executions` with a global unique id, to join the other tables in a star schema, and the name of the experiment,
* A table `parameters` with all explicit and default parameters set and,
* An additional table, whose name depends on the experiment, with the actual results of the simulation. 

For the paper, all simulations have been repeated 15 times. Each run was executed with different seeds for the random generators.


##### Experiments

Each experiment stores the outcomes in a table with the same name in the database `results.sqlite3`. For the description of the command line arguments associated to each experiment, check `./pmacomp -h`. In the paper, the following experiments were used:

<table width="100%">
    <tr>
        <th>Name</th>
        <th>Description</th>
        <th>Data stored in results.sqlite3</th>
    </tr>
    <tr>
        <td><tt>step_insert_lookup</tt></td>
        <td>Starting from an empty data structure, perform <tt>I</tt> insertions and <tt>L</tt> point look ups of existing elements.</td>
        <td>For the insertions, <tt>type = 'insert'</tt>, the attribute <tt>initial_size</tt> &#40;this is a misnomer&#41; reports the final cardinality of the data structure and <tt>time</tt> the overall completion time.
        For the point look-ups, <tt>type = 'search'</tt>, the attribute <tt>initial_size</tt> reports the cardinality of the data structure when the operation was performed, while <tt>time</tt> is the completion time to perform all <tt>L</tt> point look-ups.
        The reported times are in milliseconds.</td>
    </tr>
    <tr>
        <td><tt>range_query</tt></td>
        <td>Insert <tt>I</tt> elements and perform <tt>L</tt> range queries</td>
        <td>The attribute <tt>interval</tt> reports the range of the scans, e.g. 0.01 implies 1% of the data structure. The attribute <tt>time</tt> reports the completion time, in milliseconds, to perform <tt>m</tt> scans, with <tt>m</tt> stored in the attribute <tt>num_lookups</tt>.</td>
    </tr>
    <tr>
        <td><tt>idls</tt></td>
        <td>First, insert <tt>initial_size</tt> elements. Eventually, repeatedly perform <tt>idls_group_size</tt> consecutive insertions followed by <tt>idls_group_size</tt> consecutive deletions of existing elements.</td>
        <td>The attributes <tt>inserts</tt> and <tt>deletes</tt> state the total number of insertions and deletions executed, while <tt>t_inserts</tt> and <tt>t_deletes</tt> the total amount of time spent respectively for the insertions and for the deletions, in milliseconds.</td>
    </tr>
    <tr>
        <td><tt>step_idls</tt></td>
        <td>Insert <tt>I</tt> elements in the data structure. Every <tt>idls_group_size</tt> elements, perform <tt>num_scans</tt> complete scans of the data structure.</td>
        <td>If the attribute <tt>type = 'insert'</tt>, the attribute <tt>elements</tt> reports the cardinality of the data structure, while <tt>time</tt> is the completion time, in milliseconds, to insert all elements so far.<br/>
        If the attribute <tt>type = 'scan_insert'</tt>, then the attribute <tt>time</tt> reports the time, in milliseconds, to perform <tt>num_scans</tt> on the data structure with the cardinality given by the attribute <tt>initial_size</tt>.</td> 
    </tr>
    <tr>
        <td><tt>aging</tt></td>
        <td>First, load the (a,b)-tree with <tt>initial_size</tt> elements. Then, perform <tt>I</tt> total updates, with <tt>idls_group_size</tt> consecutive insertions followed by <tt>idls_group_size</tt> consecutive deletions. After each iteration of insertions/deletions, perform <tt>num_scans</tt> complete scans of the data structure.</td>
        <td>The attribute <tt>round</tt> is the iteration number, whereas the attribute <tt>t_scans_millisecs</tt> reports the completion time, in milliseconds, to perform <tt>num_scans</tt> scans.</td>
    </tr> 
    <tr>
        <td><tt>bulk_loading</tt></td>
        <td>First, insert <tt>initial_size</tt> elements in the data structure with a uniform distribution. Then, perform <tt>num_batches</tt> batch loadings, each of size <tt>batch_size</tt>.</td>
        <td>For each batch, the attribute <tt>initial_size</tt> is the cardinality of the data structure <i>before</i> the loading, while <tt>time</tt> is the completion time, in microseconds, of the single loaded batch.</td>
    </tr> 
</table>

##### Distributions

For the paper, the following distributions were considered:

* `uniform`: the keys are a random permutation of the interval [1, total elements to insert].  
* `uniform --beta β`: the most significant 4 bytes of the keys are extracted from the uniform distribution of range [1, β]. The 4 least significant bytes are created using a unique counter. 
* `zipf --alpha α --beta β`: the most significant 4 bytes of the keys are generated from a random permutation of the outcomes of the Zipfian distribution of parameter α and range β. The 4 least significant bytes are created using a unique counter. 
* `apma_sequential`: sequential pattern, with the keys generated as 1, 2, 3, ...  The `apma_` prefix is because it mimics one of the patterns also examined in the APMA paper [9].

---
Repeating the experiments
---

##### Introduction

For the first plot, Figure 1a, Related work:
```bash
# PMA Baseline, source code: pma/btree/btreepma_v2.*
./pmacomp -e step_insert_lookup -I 1073741824 -d uniform -a btree_pma_v2 -b 64 -l 32 -v
./pmacomp -e step_insert_lookup -I 1073741824 -d apma_sequential -a btree_pma_v2 -b 64 -l 32 -v
./pmacomp -e step_insert_lookup -I 1073741824 -d zipf --alpha 1 --beta 134217728 -a btree_pma_v2 -b 64 -l 32 -v
./pmacomp -e step_insert_lookup -I 1073741824 -d zipf --alpha 1.5 --beta 134217728 -a btree_pma_v2 -b 64 -l 32 -v
./pmacomp -e range_query -I 1073741824 -L 1024 --rqint 0.01 -d uniform -a btree_pma_v2 -b 64 -l 32 -v
# DRF12 [4], source code not included here, request it directly to its authors.
./pmacomp -e step_insert_lookup -I 1073741824 -d uniform -a pma_dfr -v
./pmacomp -e step_insert_lookup -I 1073741824 -d apma_sequential -a pma_dfr -v
./pmacomp -e step_insert_lookup -I 1073741824 -d zipf --alpha 1 --beta 134217728 -a pma_dfr -v
./pmacomp -e step_insert_lookup -I 1073741824 -d zipf --alpha 1.5 --beta 134217728 -a pma_dfr -v
./pmacomp -e range_query -I 1073741824 -L 1024 --rqint 0.01 -d uniform -a pma_dfr -v
# PM14 [5], source code: pma/external/montes/*, pma/external.* (wrapper)
./pmacomp -e step_insert_lookup -I 1073741824 -d uniform -a pma_pabmont -v
./pmacomp -e step_insert_lookup -I 1073741824 -d apma_sequential -a pma_pabmont -v
./pmacomp -e step_insert_lookup -I 1073741824 -d zipf --alpha 1 --beta 134217728 -a pma_pabmont -v
./pmacomp -e step_insert_lookup -I 1073741824 -d zipf --alpha 1.5 --beta 134217728 -a pma_pabmont -v
./pmacomp -e range_query -I 1073741824 -L 1024 --rqint 0.01 -d uniform -a pma_pabmont -v
# KLS17 [6], source code: pma/external/iejoin/*, pma/external.* (wrapper)
./pmacomp -e step_insert_lookup -I 1073741824 -d uniform -a pma_khayyat -v
./pmacomp -e step_insert_lookup -I 1073741824 -d apma_sequential -a pma_khayyat -v
./pmacomp -e step_insert_lookup -I 1073741824 -d zipf --alpha 1 --beta 134217728 -a pma_khayyat -v
./pmacomp -e step_insert_lookup -I 1073741824 -d zipf --alpha 1.5 --beta 134217728 -a pma_khayyat -v
./pmacomp -e range_query -I 1073741824 -L 1024 --rqint 0.01 -d uniform -a pma_khayyat -v
# SLH17 [7], source code: pma/external/sha/*, pma/external.* (wrapper)
./pmacomp -e step_insert_lookup -I 1073741824 -d uniform -a pma_sha -v
./pmacomp -e step_insert_lookup -I 1073741824 -d apma_sequential -a pma_sha -v
./pmacomp -e step_insert_lookup -I 1073741824 -d zipf --alpha 1 --beta 134217728 -a pma_sha -v
./pmacomp -e step_insert_lookup -I 1073741824 -d zipf --alpha 1.5 --beta 134217728 -a pma_sha -v
./pmacomp -e range_query -I 1073741824 -L 1024 --rqint 0.01 -d uniform -a pma_sha -v
```

For the second plot, Figure 1b, (a,b)-trees:
```bash
# Source code: abtree/abtree.*
for B in 64 128 256 512; do 
    ./pmacomp -e step_insert_lookup -I 1073741824 -d uniform -a btree_v2 -b 64 -l $B -v
    ./pmacomp -e step_insert_lookup -I 1073741824 -d apma_sequential -a btree_v2 -b 64 -l $B -v
    ./pmacomp -e step_insert_lookup -I 1073741824 -d zipf --alpha 1 --beta 134217728 -a btree_v2 -b 64 -l $B -v
    ./pmacomp -e step_insert_lookup -I 1073741824 -d zipf --alpha 1.5 --beta 134217728 -a btree_v2 -b 64 -l $B -v
    ./pmacomp -e range_query -I 1073741824 -L 1024 --rqint 0.01 -d uniform -a btree_v2 -b 64 -l $B -v
done
```

For the third plot, Figure 1c, final contributions:
```bash
# (a,b)-tree B=128, source code: abtree/abtree.*
./pmacomp -e step_insert_lookup -I 1073741824 -d uniform -a btree_v2 -b 64 -l 128 -v
./pmacomp -e step_insert_lookup -I 1073741824 -d apma_sequential -a btree_v2 -b 64 -l 128 -v
./pmacomp -e step_insert_lookup -I 1073741824 -d zipf --alpha 1 --beta 134217728 -a btree_v2 -b 64 -l 128 -v
./pmacomp -e step_insert_lookup -I 1073741824 -d zipf --alpha 1.5 --beta 134217728 -a btree_v2 -b 64 -l 128 -v
./pmacomp -e range_query -I 1073741824 -L 1024 --rqint 0.01 -d uniform -a btree_v2 -b 64 -l 128 -v
# RMA B=128 (update thresholds), source code: pma/adaptive/int2/* 
./pmacomp -e step_insert_lookup -I 1073741824 -d uniform -a apma_int2b -b 65 -l 128 --hugetlb --extent_size 1 -v
./pmacomp -e step_insert_lookup -I 1073741824 -d apma_sequential -a apma_int2b -b 65 -l 128 --hugetlb --extent_size 1 -v
./pmacomp -e step_insert_lookup -I 1073741824 -d zipf --alpha 1 --beta 134217728 -a apma_int2b -b 65 -l 128 --hugetlb --extent_size 1 -v
./pmacomp -e step_insert_lookup -I 1073741824 -d zipf --alpha 1.5 --beta 134217728 -a apma_int2b -b 65 -l 128 --hugetlb --extent_size 1 -v
./pmacomp -e range_query -I 1073741824 -L 1024 --rqint 0.01 -d uniform -a apma_int2b -b 65 -l 128 --hugetlb --extent_size 1 -v
# (a,b)-tree B=256, source code: abtree/abtree.*
./pmacomp -e step_insert_lookup -I 1073741824 -d uniform -a btree_v2 -b 64 -l 256 -v
./pmacomp -e step_insert_lookup -I 1073741824 -d apma_sequential -a btree_v2 -b 64 -l 256 -v
./pmacomp -e step_insert_lookup -I 1073741824 -d zipf --alpha 1 --beta 134217728 -a btree_v2 -b 64 -l 256 -v
./pmacomp -e step_insert_lookup -I 1073741824 -d zipf --alpha 1.5 --beta 134217728 -a btree_v2 -b 64 -l 256 -v
./pmacomp -e range_query -I 1073741824 -L 1024 --rqint 0.01 -d uniform -a btree_v2 -b 64 -l 256 -v
# RMA B=256 (update thresholds), source code: pma/adaptive/int2/* 
./pmacomp -e step_insert_lookup -I 1073741824 -d uniform -a apma_int2b -b 65 -l 256 --hugetlb --extent_size 1 -v
./pmacomp -e step_insert_lookup -I 1073741824 -d apma_sequential -a apma_int2b -b 65 -l 256 --hugetlb --extent_size 1 -v
./pmacomp -e step_insert_lookup -I 1073741824 -d zipf --alpha 1 --beta 134217728 -a apma_int2b -b 65 -l 256 --hugetlb --extent_size 1 -v
./pmacomp -e step_insert_lookup -I 1073741824 -d zipf --alpha 1.5 --beta 134217728 -a apma_int2b -b 65 -l 256 --hugetlb --extent_size 1 -v
./pmacomp -e range_query -I 1073741824 -L 1024 --rqint 0.01 -d uniform -a apma_int2b -b 65 -l 256 --hugetlb --extent_size 1 -v
# Static dense arrays, source code: abtree/dense_array.*
./pmacomp -e range_query -I 1073741824 -L 1024 --rqint 0.01 -d uniform -a dense_array -l 64 --hugetlb -v
```

##### Node and segment size

These are the simulations whose outcomes are represented in Figure 10, where the ART+B-Tree is compared to the RMA at different leaf/segment capacities.

```bash
for B in 32 128 512 2048; do 
    # ART [8] / B+Tree, source code: abtree/art.* (wrapper), third-party/art/*
    ./pmacomp -e step_insert_lookup -I 1073741824 -L 33554432 -d uniform -a art -l $B -v
    ./pmacomp -e range_query -I 1073741824 -L 1024 -d uniform -a art -l $B -v
    # RMA (update thresholds), source code: pma/adaptive/int2/* 
    ./pmacomp -e step_insert_lookup -I 1073741824 -L 33554432 -d uniform -a apma_int2b -b 65 -l $B --hugetlb --extent_size 1 -v
    ./pmacomp -e range_query -I 1073741824 -L 1024 -d uniform -a apma_int2b -b 65 -l $B --hugetlb --extent_size 1 -v
done
```

The paper mentions the difference with the STX-Tree [10]. To repeat this experiment:
```bash
# In the STX-Tree, the node and leaf capacities must be specified at compile time. Compile the driver with:
make EXTRA_CXXFLAGS="-DSTX_BTREE_INDEX_B=64 -DSTX_BTREE_LEAF_B=128" -j
# To run the experiments:
./pmacomp -e step_insert_lookup -I 1073741824 -L 33554432 -d uniform -a btree_stx -b 64 -l 128 -v
./pmacomp -e range_query -I 1073741824 -L 1024 -d uniform -a btree_stx -b 64 -l 128 -v
```

##### Adaptive rebalancing
This is the comparison of the RMA with adaptive rebalancing turned on and off and with the original APMA strategy [9]. The outcomes of this experiment are showed in the paper in Figure 11.
```bash
# RMA without adaptive rebalancing, source code: pma/btree/btreepmacc7.*
./pmacomp -e step_insert_lookup -I 1073741824 -d uniform -a btreecc_pma7b -b 65 -l 128 --hugetlb --extent_size 1 -v
./pmacomp -e idls --initial_size 1073741824 -I 2147483648 --idls_group_size 1024 -d uniform --beta 134217728 -a btreecc_pma7b -b 65 -l 128 --hugetlb --extent_size 1 -v
for alpha in $(seq 0.25 0.25 3); do # alpha = 0.25, 0.5, 0.75, ...
    ./pmacomp -e step_insert_lookup -I 1073741824 -d zipf --alpha $alpha --beta 134217728 -a btreecc_pma7b -b 65 -l 128 --hugetlb --extent_size 1 -v
    ./pmacomp -e idls --initial_size 1073741824 -I 2147483648 --idls_group_size 1024 -d zipf --alpha $alpha --beta 134217728 -a btreecc_pma7b -b 65 -l 128 --hugetlb --extent_size 1 -v
done

# RMA with adaptive rebalancing, source code: pma/adaptive/int2/* 
./pmacomp -e step_insert_lookup -I 1073741824 -d uniform -a apma_int2b -b 65 -l 128 --hugetlb --extent_size 1 -v
./pmacomp -e idls --initial_size 1073741824 -I 2147483648 --idls_group_size 1024 -d uniform --beta 134217728 -a apma_int2b -b 65 -l 128 --hugetlb --extent_size 1 -v
for alpha in $(seq 0.25 0.25 3); do # alpha = 0.25, 0.5, 0.75, ...
    ./pmacomp -e step_insert_lookup -I 1073741824 -d zipf --alpha $alpha --beta 134217728 -a apma_int2b -b 65 -l 128 --hugetlb --extent_size 1 -v
    ./pmacomp -e idls --initial_size 1073741824 -I 2147483648 --idls_group_size 1024 -d zipf --alpha $alpha --beta 134217728 -a apma_int2b -b 65 -l 128 --hugetlb --extent_size 1 -v
done

# Original APMA strategy [9], source code: pma/adaptive/bh07_v2/*
./pmacomp -e step_insert_lookup -I 1073741824 -d uniform --beta 134217728 -a bh07_v2b -b 65 -l 128 --hugetlb --extent_size 1 -v
for alpha in $(seq 0.25 0.25 3); do # alpha = 0.25, 0.5, 0.75, ...
    ./pmacomp -e step_insert_lookup -I 1073741824 -d zipf --alpha $alpha --beta 134217728 -a bh07_v2b -b 65 -l 128 --hugetlb --extent_size 1 -v
done
```

##### Density thresholds

This section compares the update thresholds with the scan thresholds in terms of both the insertion and the scan throughput. In the paper, the results are depicted in Figure 12. 
The graphs also show the results of the same experiment for the (a,b)-tree and the static dense array. 

```bash
# (a,b)-tree, source code: abtree/abtree.*
./pmacomp -e step_idls --initial_size 16777216 -I 1073741824 --idls_group_size 16777216 --num_scans 16 -d uniform --beta 134217728 -a btree_v2 -b 64 -l 128 -v
./pmacomp -e step_idls --initial_size 16777216 -I 1073741824 --idls_group_size 16777216 --num_scans 16 -d apma_sequential --beta 134217728 -a btree_v2 -b 64 -l 128 -v
# RMA / update thresholds, source code: pma/adaptive/int2/*  
./pmacomp -e step_idls --initial_size 16777216 -I 1073741824 --idls_group_size 16777216 --num_scans 16 -d uniform --beta 134217728 -a btreecc_pma7b -b 65 -l 128 --hugetlb --extent_size 1 -v
./pmacomp -e step_idls --initial_size 16777216 -I 1073741824 --idls_group_size 16777216 --num_scans 16 -d apma_sequential --beta 134217728 -a btreecc_pma7b -b 65 -l 128 --hugetlb --extent_size 1 -v
# RMA / scan thresholds, source code: pma/adaptive/int3/*
./pmacomp -e step_idls --initial_size 16777216 -I 1073741824 --idls_group_size 16777216 --num_scans 16 -d uniform --beta 134217728 -a apma_int3 -b 65 -l 128 --hugetlb --extent_size 1 -v
./pmacomp -e step_idls --initial_size 16777216 -I 1073741824 --idls_group_size 16777216 --num_scans 16 -d apma_sequential --beta 134217728 -a apma_int3 -b 65 -l 128 --hugetlb --extent_size 1 -v
# Static dense arrays, source code: abtree/static_abtree.*
./pmacomp -e step_insert_scan --initial_size 16777216 -I 1073741824 --idls_group_size 16777216 --num_scans 16 -d uniform -a dense_array -l 64 --hugetlb -v
```

##### Aging

In the paper, Figure 13a shows how the throughput of scans in built (a,b)-trees deteriorates once random updates are executed. 

```bash
# Source code: abtree/abtree.*, experiment: pma/experiments/aging.*
TEMP=/tmp # an existing directory on a partition with at least 25 GB of free space
./pmacomp -e aging --temp $TEMP --initial_size 1073741824 -I 2147483648 --idls_group_size 1048576 --num_scans 1 --scan_warmup 25 -d uniform --beta 4294967295 -a btree_v2 -b 64 -l 128 -v 
```

##### Bulk loading

This section compares the top-down strategy of [4] with the bottom-up algorithm described in the paper. Again, we do not have the permission the redistribute the source code of [4], it 
needs to be explicitly requested to its authors and placed in the folder third-party/pma_dfr_inria.

```bash
# RMA without bulk loading, source code: pma/adaptive/int2/*
./pmacomp -e bulk_loading --initial_size 536870912 -I 1073741824 --initial_size_uniform --batch_size 1 --num_batches 536870912 -d uniform -a apma_int2b -b 65 -l 128 --hugetlb --extent_size 1 -v
for alpha in $(seq 0.25 0.25 3); do # alpha = 0.25, 0.5, 0.75, ...
    ./pmacomp -e bulk_loading --initial_size 536870912 -I 1073741824 --initial_size_uniform --batch_size 1 --num_batches 536870912 -d zipf --alpha $alpha --beta 134217728 -a apma_int2b -b 65 -l 128 --hugetlb --extent_size 1 -v
done

# RMA, with bottom-up bulk loading and with memory rewiring turned on, source code: pma/btree/btreepmacc7.*
./pmacomp -e bulk_loading --initial_size 536870912 -I 1073741824 --initial_size_uniform --batch_size 1048576 --num_batches 512 -d uniform -a btreecc_pma7b -b 65 -l 128 --hugetlb --extent_size 1 -v
for alpha in $(seq 0.25 0.25 3); do # alpha = 0.25, 0.5, 0.75, ...
    ./pmacomp -e bulk_loading --initial_size 536870912 -I 1073741824 --initial_size_uniform --batch_size 1048576 --num_batches 512 -d zipf --alpha $alpha --beta 134217728 -a btreecc_pma7b -b 65 -l 128 --hugetlb --extent_size 1 -v
done

# RMA, with bottom-up bulk loading and with memory rewiring turned off, source code: pma/btree/btreepmacc5.*
./pmacomp -e bulk_loading --initial_size 536870912 -I 1073741824 --initial_size_uniform --batch_size 1048576 --num_batches 512 -d uniform -a btreecc_pma5b -b 65 -l 128 -v
for alpha in $(seq 0.25 0.25 3); do # alpha = 0.25, 0.5, 0.75, ...
    ./pmacomp -e bulk_loading --initial_size 536870912 -I 1073741824 --initial_size_uniform --batch_size 1048576 --num_batches 512 -d zipf --alpha $alpha --beta 134217728 -a btreecc_pma5b -b 65 -l 128 -v
done

# DFR [4], with top-down bulk loading, source code not included
./pmacomp -e bulk_loading --initial_size 536870912 -I 1073741824 --initial_size_uniform --batch_size 1048576 --num_batches 512 -d uniform -a pma_dfr -v
for alpha in $(seq 0.25 0.25 3); do # alpha = 0.25, 0.5, 0.75, ...
    ./pmacomp -e bulk_loading --initial_size 536870912 -I 1073741824 --initial_size_uniform --batch_size 1048576 --num_batches 512 -d zipf --alpha $alpha --beta 134217728 -a pma_dfr -v
done
```

##### Contributions

The last graph, Figure 14, in the paper shows the cumulative contribution of each feature in the throughput of insertions and scans.

```bash
# Baseline, source code: pma/btree/btreepma_v2.*
./pmacomp -e step_insert_lookup -I 1073741824 -d uniform -a btree_pma_v2 -b 64 -l 32 -v
./pmacomp -e step_insert_lookup -I 1073741824 -d apma_sequential -a btree_pma_v2 -b 64 -l 32 -v
./pmacomp -e step_insert_lookup -I 1073741824 -d zipf --alpha 1 --beta 134217728 -a btree_pma_v2 -b 64 -l 32 -v
./pmacomp -e step_insert_lookup -I 1073741824 -d zipf --alpha 1.5 --beta 134217728 -a btree_pma_v2 -b 64 -l 32 -v
./pmacomp -e range_query -I 1073741824 -L 1024 --rqint 0.01 -d uniform -a btree_pma_v2 -b 64 -l 32 -v
# Clustering, source code: pma/btree/btreepma_v4.*
./pmacomp -e step_insert_lookup -I 1073741824 -d uniform -a btree_pma_v4a -l 128 -v
./pmacomp -e step_insert_lookup -I 1073741824 -d apma_sequential -a btree_pma_v4a -l 128 -v
./pmacomp -e step_insert_lookup -I 1073741824 -d zipf --alpha 1 --beta 134217728 -a btree_pma_v4a -l 128 -v
./pmacomp -e step_insert_lookup -I 1073741824 -d zipf --alpha 1.5 --beta 134217728 -a btree_pma_v4a -l 128 -v
./pmacomp -e range_query -I 1073741824 -L 1024 --rqint 0.01 -d uniform -a btree_pma_v4a -l 128 -v
# Fixed size segments, source code: pma/btree/btreepma_v4.*
./pmacomp -e step_insert_lookup -I 1073741824 -d uniform -a btree_pma_v4b -b 64 -l 128 -v
./pmacomp -e step_insert_lookup -I 1073741824 -d apma_sequential -a btree_pma_v4b -b 64 -l 128 -v
./pmacomp -e step_insert_lookup -I 1073741824 -d zipf --alpha 1 --beta 134217728 -a btree_pma_v4b -b 64 -l 128 -v
./pmacomp -e step_insert_lookup -I 1073741824 -d zipf --alpha 1.5 --beta 134217728 -a btree_pma_v4b -b 64 -l 128 -v
./pmacomp -e range_query -I 1073741824 -L 1024 --rqint 0.01 -d uniform -a btree_pma_v4b -b 64 -l 128 -v
# Static index, source code: pma/btree/btreepmacc5.*
./pmacomp -e step_insert_lookup -I 1073741824 -d uniform -a btreecc_pma5b -b 65 -l 128 -v
./pmacomp -e step_insert_lookup -I 1073741824 -d apma_sequential -a btreecc_pma5b -b 65 -l 128 -v
./pmacomp -e step_insert_lookup -I 1073741824 -d zipf --alpha 1 --beta 134217728 -a btreecc_pma5b -b 65 -l 128 -v
./pmacomp -e step_insert_lookup -I 1073741824 -d zipf --alpha 1.5 --beta 134217728 -a btreecc_pma5b -b 65 -l 128 -v
./pmacomp -e range_query -I 1073741824 -L 1024 --rqint 0.01 -d uniform -a btreecc_pma5b -65 -l 128 -v
# Memory rewiring, source code: pma/btree/btreepmacc7.*
./pmacomp -e step_insert_lookup -I 1073741824 -d uniform -a btreecc_pma7b -b 65 -l 128 --hugetlb --extent_size 1 -v
./pmacomp -e step_insert_lookup -I 1073741824 -d apma_sequential -a btreecc_pma7b -b 65 -l 128 --hugetlb --extent_size 1 -v
./pmacomp -e step_insert_lookup -I 1073741824 -d zipf --alpha 1 --beta 134217728 -a btreecc_pma7b -b 65 -l 128 --hugetlb --extent_size 1 -v
./pmacomp -e step_insert_lookup -I 1073741824 -d zipf --alpha 1.5 --beta 134217728 -a btreecc_pma7b -b 65 -l 128 --hugetlb --extent_size 1 -v
./pmacomp -e range_query -I 1073741824 -L 1024 --rqint 0.01 -d uniform -a btreecc_pma7b -65 -l 128 --hugetlb --extent_size 1 -v
# Adaptive rebalancing, source code: pma/adaptive/int2/*
./pmacomp -e step_insert_lookup -I 1073741824 -d uniform -a apma_int2b -b 65 -l 128 --hugetlb --extent_size 1 -v
./pmacomp -e step_insert_lookup -I 1073741824 -d apma_sequential -a apma_int2b -b 65 -l 128 --hugetlb --extent_size 1 -v
./pmacomp -e step_insert_lookup -I 1073741824 -d zipf --alpha 1 --beta 134217728 -a apma_int2b -b 65 -l 128 --hugetlb --extent_size 1 -v
./pmacomp -e step_insert_lookup -I 1073741824 -d zipf --alpha 1.5 --beta 134217728 -a apma_int2b -b 65 -l 128 --hugetlb --extent_size 1 -v
./pmacomp -e range_query -I 1073741824 -L 1024 --rqint 0.01 -d uniform -a apma_int2b -65 -l 128 --hugetlb --extent_size 1 -v
```
 

---
References:
---
1. D. De Leo, P. Boncz. Packed Memory Arrays - Rewired. In ICDE 2019. [Paper](https://ir.cwi.nl/pub/28649).
2. D. De Leo, P. Boncz. Fast concurrent reads and updates with PMAs. In GRADES 2019. [Paper](https://ir.cwi.nl/pub/28679), [Source code](https://github.com/cwida/rma_concurrent).
3. F. Schuhknecht, J. Dittrich, A. Sharma. RUMA has it: rewired user-space memory access is possible! In VLDB 2016. [Paper](http://www.vldb.org/pvldb/vol9/p768-schuhknecht.pdf).
4. M. Durand, B. Raffin, and F. Faure. A packed memory array to keep moving particles sorted. In VRIPHYS, 2012. [Paper](https://hal.inria.fr/hal-00762593/document).
5. P. Montes. Packed-memory array, 2014. Source code available online at [http://github.com/pabmont/pma](http://github.com/pabmont/pma)
6. Z. Khayyat, W. Lucia, M. Singh, M. Ouzzani, P. Papotti, J.-A. Quiané-Ruiz, N. Tang, and P. Kalnis. Fast and scalable inequality joins. In VLDB 2017. [Paper](https://link.springer.com/article/10.1007/s00778-016-0441-6), [Source code](https://drive.google.com/file/d/1fqy1uC-CcYpZSvOGZOq3uoT4hg3kdqfp/view?usp=sharing).
7. M. Sha, Y. Li, B. He, and K.-L. Tan. Accelerating dynamic graph analytics on gpus. VLDB, 2017. [Paper](https://www.vldb.org/pvldb/vol11/p107-sha.pdf), [Source code](https://github.com/desert0616/gpma_bfs_demo/blob/master/cpu_baseline/containers/pma_dynamic_graph.hpp).
8. V. Leis, A. Kemper, and T. Neumann. The adaptive radix tree: Artful indexing for main-memory databases. In ICDE, 2013. [Paper](https://ieeexplore.ieee.org/document/6544812), [Source code](http://github.com/flode/ARTSynchronized).
9. M. Bender and H. Hu. An adaptive packed-memory array. In TODS, 2007. [Paper](https://dl.acm.org/citation.cfm?doid=1292609.1292616).
10. T. Bingmann. STX B+ tree C++ template classes v0.9, 2013. Available online at: [http://github.com/bingmann/stx-btree](http://github.com/bingmann/stx-btree)
