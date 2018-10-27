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

#ifndef PMA_ABTREE_v2_HPP_
#define PMA_ABTREE_v2_HPP_

#include "pma/interface.hpp"
#include "pma/iterator.hpp"

#include <cinttypes>
#include <ostream>
#include <unordered_map>
#include <utility>

namespace abtree {

/**
 * Basic implementation of a B+ tree
 */
class ABTree : public pma::InterfaceRQ {
    ABTree(const ABTree&) = delete;
    ABTree& operator= (ABTree&) = delete;

    struct Node {
        // remove the ctors
        Node() = delete;
        Node(const Node&) = delete;
        Node& operator=(const Node&) = delete;

        size_t N;

        bool empty() const;
    };

    struct InternalNode : public Node {
        // remove the ctors
        InternalNode() = delete;
        InternalNode(const InternalNode&) = delete;
        InternalNode& operator=(const InternalNode&) = delete;

//        int64_t* keys() const;
//        Node** children() const;
    };

    int64_t* KEYS(const InternalNode* inode) const;
    Node** CHILDREN(const InternalNode* inode) const;

    struct Leaf : public Node {
        // remove the ctors
        Leaf() = delete;
        Leaf(const Leaf&) = delete;
        Leaf& operator=(const Leaf&) = delete;

        Leaf* next;
        Leaf* previous;

//        int64_t* keys() const;
//        int64_t* values() const;
    };

    int64_t* KEYS(const Leaf* leaf) const;
    int64_t* VALUES(const Leaf* leaf) const;

  class Iterator : public pma::Iterator {
    friend class ABTree;
    const ABTree* tree;
    int64_t max;
    Leaf* block;
    size_t pos;

    Iterator(const ABTree* tree, int64_t max, Leaf* leaf, int64_t pos);

  public:
    virtual bool hasNext() const override;
    virtual std::pair<int64_t, int64_t> next() override;
  };

  const size_t intnode_a; // lower bound for internal nodes
  const size_t intnode_b; // upper bound for internal nodes
  const size_t leaf_a; // lower bound for leaves
  const size_t leaf_b; // upper bound for leaves
  bool common_memsize = false; // whether all nodes must be allocated with the same size
  const size_t min_sizeof_inode; // the minimum size, in bytes, of an allocated InternalNode
  const size_t min_sizeof_leaf; // the minimum size, in bytes, of an allocated Leaf
  Node* root; // current root
  int64_t cardinality; // number of elements inside the B-Tree
  int height =1; // number of levels, or height of the tree
  mutable size_t num_nodes_allocated; // internal profiling
  mutable size_t num_leaves_allocated; // internal profiling
  bool record_leaf_statistics = false; // whether to record the leaf distances when deleting the tree

  void initialize_from_array(std::pair<int64_t, int64_t>* elements, size_t size, bool do_sort = true);

  // Validate the parameters a, b (lowerbound and upperbound respectively)
  void validate_bounds() const;

  void validate_bounds(const Node* node, int depth) const;

  // Check that that the given `node' respect the tree bounds
  // If not, it raises a std::range_error exception
  void validate_rec(const Node* node, int depth) const;

  size_t get_lowerbound(int depth) const;
  size_t get_upperbound(int depth) const;

  // Create a new node / leaf
  InternalNode* create_internal_node() const;
  Leaf* create_leaf() const;

  // Determine the memory size of an internal node / leaf
  size_t init_memsize_internal_node() const;
  size_t init_memsize_leaf() const;

  // Get the memory size of an internal node / leaf
  size_t memsize_internal_node() const;
  size_t memsize_leaf() const;

  // Delete an existing node / leaf
  void delete_node(Node* node, int depth) const;

  std::unique_ptr<ABTree::Iterator> create_iterator(int64_t max, Leaf* block, int64_t) const;
  std::unique_ptr<ABTree::Iterator> leaf_scan(Leaf* leaf, int64_t min, int64_t max) const;

  // It splits the child of `node' at index `child' in half and adds the new node as
  // a new child of `node'.
  void split(InternalNode* inode, size_t child_index, int child_depth);

  // It increases the height of tree by 1, by splitting the current root in half and
  // introducing a new root.
  void split_root();

  // Insert the given key/value in the subtree rooted at the given `node'.
  void insert(Node* node, int64_t key, int64_t value, int depth);

  void merge(InternalNode* node, size_t child_index, int child_depth);
  void rotate_left(InternalNode* node, size_t child_index, int child_depth, size_t num_nodes);
  void rotate_right(InternalNode* node, size_t child_index, int child_depth, size_t num_nodes);
  void rebalance_lb(InternalNode* node, size_t child_index, int child_depth);
  void rebalance_rec(Node* node, int64_t range_min, int64_t range_max, int depth);

  // Attempts to reduce the height of the tree, checking whether the root has only one child.
  bool reduce_tree();

  // It removes the key/values in the interval [range_min, range_max] for the subtree rooted
  // at the given `node'. It returns `true' if any of the nodes in the given subtree does not
  // respect the constraint of their size in [A, B], false otherwise.
  // The parameter `min' is an output variable to return the minimum value in the subtree.
  bool remove_keys(Node* node, int64_t range_min ,int64_t range_max, int depth, int64_t* min);

  // It removes the children of node in the interval [index, index + length).
  void remove_subtrees(InternalNode* node, size_t index, size_t length, int children_depth);

  // Helper method, it performs the recursion of remove_subtrees
  void remove_subtrees_rec0(Node* node, int depth);

  // Remove the given interval from the sub-tree starting at node
  void remove(Node* node, int64_t keymin, int64_t keymax, int depth);

  // Remove a single element from the tree
  int64_t remove(Node* node, int64_t key, int depth, int64_t* omin);

  // Debugging
  void dump_data(std::ostream&, Node* node, int depth) const;

  // Permute the allocated nodes with different allocation sizes for inodes/leaves (common_memsize == false)
  void permute_allocated_nodes_diff(uint64_t random_seed);
  struct pand_state;
  void pand_main(pand_state& state); // main entry
  void pand_dfs_visit_abtree(pand_state& state); // init the state for m_inodes, m_leaves and m_parents, recursively visit the nodes in the B-Tree
  void pand_dfs_visit_abtree(pand_state& state, Node* node, int depth);
  void pand_permute_inodes(pand_state& state);
  void pand_permute_leaves(pand_state& state);

  // Permute the allocated nodes with equal allocation sizes for inodes/leaves (common_memsize == true)
  void permute_allocated_nodes_nodiff(uint64_t random_seed);
  struct pann_state;
  void pann_main(pann_state& state); // main entry
  void pann_dfs_visit(pann_state& state, Node* node, int depth); // visit all nodes in the AB-Tree, init the state

  // Save the leaf distance in the database
  void record_leaf_distance_memory() const;


  /**
   * Check whether the nodes at the given height are leaves or internal node
   * @param depth in [0, height-1]
   * @return true if the given node is a leaf, false otherwise
   */
  bool is_leaf(int depth) const;

#if !defined(NDEBUG)
  void sanity_check();
  void sanity_check(Node* node, int depth, int64_t minimum, std::unordered_map<Node*, bool>& dict);
#else
  void sanity_check() { };
#endif

public:

  /**
   * Create a new B-Tree with params [B/2, B]
   */
  explicit ABTree(size_t B);

  /**
   * Create a new B-Tree with params [A, B]
   */
  ABTree(size_t A, size_t B);


  /**
   * Create a new B-Tree with the bounds [iA, iB] for the inner
   * nodes and [lA, lB] for the leaves
   */
  ABTree(size_t iA, size_t iB, size_t lA, size_t lB);


  /**
   * Create a new B-Tree with params [B/2, B] and initialize with the given
   * array of elements
   */
  ABTree(size_t B, std::pair<int64_t,int64_t>* elements, size_t elements_sz);

  /**
   * Create a new B-Tree with params [A, B] and initialize with the given
   * array of elements
   */
  ABTree(size_t A, size_t B, std::pair<int64_t,int64_t>* elements, size_t elements_sz);

  /**
   * Create a new B-Tree with the bounds [iA, iB] for the inner nodes and [lA, lB] for
   * the leaves, and initialize with the given array of elements
   */
  ABTree(size_t iA, size_t iB, size_t lA, size_t lB, std::pair<int64_t,int64_t>* elements, size_t elements_sz);

  /**
   * Destructor
   */
  virtual ~ABTree();

  /**
   * Insert the given key/value into the B-Tree
   */
  virtual void insert(int64_t key, int64_t value) override;

  /**
   * It returns the value for the given `key', or -1 if it is not present. In case
   * of duplicates, it returns the value of one of the duplicates.
   */
  virtual int64_t find(int64_t key) const noexcept override;

  /**
   * Returns an iterator for all keys in the interval [min, max]
   */
  virtual std::unique_ptr<pma::Iterator> find(int64_t min, int64_t max) const override;

  /**
   * Benchmark interface. Sum all elements in the interval [min, max]
   */
  virtual pma::Interface::SumResult sum(int64_t min, int64_t max) const override;

  /**
   * Remove the element having the given key and returns its value. In case
   * of duplicates, it removes only one of the elements in an unspecified manner.
   * If no element has the given key, it returns -1
   */
  int64_t remove(int64_t key) override;

  /**
   * Remove all elements contained in the interval [min, max]
   */
  void remove(int64_t min, int64_t max);

  /**
   * Report the the number of elements contained in the B-Tree
   */
  size_t size() const override;

  /**
   * Report the memory footprint (in bytes) of the whole data structure
   */
  size_t memory_footprint() const override;

  /**
   * It dumps the content of the B-Tree in the given output stream
   */
  virtual void dump_data(std::ostream&) const;

  /**
   * It dumps the memory usage of the B-Tree in the given output stream
   */
  virtual void dump_memory(std::ostream&) const;

  /**
   * It dumps the content of the B-Tree to the standard output
   */
  virtual void dump() const override;

  /**
   * Load the given elements in the array. It assumes the data structure is empty
   * before the method is invoked.
   */
  virtual void load(const std::pair<int64_t, int64_t>* elements, size_t elements_sz);

  /**
   * Verify that all nodes in the tree respect the proper bounds. If the validation fails,
   * a std::range_error exception is raised
   */
  void validate() const;

  /**
   * Set whether both the internal nodes and the leaves of the B-Tree must be allocated
   * with the same memory footprint. The only reasonable scenario to set this value to
   * `true' is if the node allocations need to be permuted randomly afterwards.
   * If this setting is false (default), a permutation will only swap internal nodes with
   * internal nodes, and leaves with leaves, without swapping inodes with leaves.
   *
   * This setting can be changed only when the tree is empty.
   */
  void set_common_memsize_nodes(bool value);

  /**
   * Whether to record the average distance among the leaves in the tree. If set, when
   * the tree is deleted, a final pass among all the leaves of the tree is performed. Some
   * statistics, such as the avg/min/max/dev/median distance in the logical memory among
   * consecutive leaves is recorded in the table `btree_leaf_statistics'.
   */
  void set_record_leaf_statistics(bool value);

  /**
   * Intercept a batch of inserts has been executed, and randomly permute the node in memory
   * if the parameter --abtree_random_permutation has been set.
   */
  void build() override;

  /**
   * Get the maximum key currently stored in the (a,b)-tree, or -1 if the tree is empty
   */
  int64_t key_max() const;

  /**
   * Randomly permute the allocated nodes in memory. Used to exacerbate an aged B+-Tree.
   *
   * Use #set_common_memsize_nodes(true) if you intend to use this method.
   */
  void permute(uint64_t random_seed);

  /**
   * Compute the average distance & cardinality among the leaves in the AB-trees
   */
  struct LeafStatistics {
      uint64_t m_num_leaves;
      uint64_t m_distance_avg;
      uint64_t m_distance_min;
      uint64_t m_distance_max;
      uint64_t m_distance_stddev;
      uint64_t m_distance_median;
      uint64_t m_cardinality_avg;
      uint64_t m_cardinality_min;
      uint64_t m_cardinality_max;
      uint64_t m_cardinality_stddev;
      uint64_t m_cardinality_median;
  };
  LeafStatistics get_stats_leaf_distance() const;
};

} // namespace abtree

#endif /* PMA_ABTREE_v2_HPP_ */
