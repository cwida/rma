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

#ifndef CBYTEARRAY_HPP_
#define CBYTEARRAY_HPP_

#include <algorithm>
#include <cinttypes>
#include <cstddef>
#include <iterator>
#include <memory>
#include <utility>

namespace distribution {

class CByteArray; // forward decl.
class CByteReference;
class CByteIterator;

/**
 * Reference to an element in CByteArray
 */
class CByteReference {
    friend class CByteArray;
    friend class CByteIterator;

    CByteArray* m_array;
    const size_t m_index;

protected:
    CByteReference(CByteArray* array, size_t index);

public:
    operator int64_t() const;

    CByteReference& operator=(int64_t);
    CByteReference& operator=(const CByteReference& value);
};

// Required by std::sort
void swap(CByteReference x, CByteReference y);

class CByteIterator {
public:
    // std iterator madness, all 5 aliases are required to delete those implicitly made by iterator_traits ...
    using value_type = int64_t;
    using difference_type = std::ptrdiff_t;
    using reference = CByteReference;
    using pointer = CByteReference*;
    using iterator_category = std::random_access_iterator_tag;

private:
    CByteArray* m_array; // actual instance attached to this iterator
    size_t m_offset; // current position

public:
    /**
     * DefaultConstructible, it provides an invalid iterator. Expected by the Iterator concept [bah...]
     */
    CByteIterator();

    CByteIterator(CByteArray* array, size_t position);

    /**
     * Copy constructor/operator
     */
    CByteIterator(const CByteIterator&) = default;
    CByteIterator& operator=(const CByteIterator&) = default;

    // De-Reference operator
    CByteReference operator*() const;

    // Random access
    CByteReference operator[](std::ptrdiff_t d) const; // x[d]

    // Move ahead
    CByteIterator& operator++(); // x++
    CByteIterator operator++(int); // ++x
    CByteIterator& operator+=(std::ptrdiff_t d); // x += d
    CByteIterator operator+(std::ptrdiff_t d); // x + d

    // Move backwards
    CByteIterator& operator--(); // x--
    CByteIterator operator--(int);  // --x
    CByteIterator& operator-=(std::ptrdiff_t d); // x -= d
    CByteIterator operator-(std::ptrdiff_t d); // x - d

    // Comparison operators
    bool operator==(const CByteIterator&) const;
    bool operator!=(const CByteIterator&) const;
    bool operator<(const CByteIterator&) const;
    bool operator>(const CByteIterator&) const;
    bool operator<=(const CByteIterator&) const;
    bool operator>=(const CByteIterator&) const;

    // Difference
    std::ptrdiff_t operator+(const CByteIterator& it) const; // d = x + y
    std::ptrdiff_t operator-(const CByteIterator& it) const; // d = x - y
};

class CByteArray {
    /*const*/ size_t m_bytes_per_element; // it can be changed in a move assignment
    size_t m_capacity; // the capacity of this array
    char* m_array; // underlying storage

    // it would need to duplicate the array
    CByteArray(CByteArray&) = delete;
    CByteArray& operator=(CByteArray&) = delete;

public:
    /**
     * Create a new array for the given capacity, able to hold keys of the same size of `capacity'
     */
    CByteArray(size_t capacity);

    /**
     * Create a new array with the given capacity, using `bytes_per_element' to hold each element.
     */
    CByteArray(size_t bytes_per_element, size_t capacity);

    /**
     * Move constructor
     */
    CByteArray(CByteArray&&);

    /**
     * Default destructor
     */
    ~CByteArray();

    /**
     * Move operator
     */
    CByteArray& operator=(CByteArray&&);

    /**
     * Retrieve the value at the given position. No boundary checks are performed.
     */
    int64_t get_value_at(size_t index) const;

    /**
     * Set the value at the given position. No boundary checks are performed.
     */
    void set_value_at(size_t index, int64_t value);


    /**
     * Array operator. Get/Set the element at the given position.
     */
    CByteReference operator[](size_t index);
    const CByteReference operator[](size_t index) const;

    /**
     * Retrieve the capacity of the container
     */
    size_t capacity() const;

    CByteIterator begin();

    CByteIterator end();

    /**
     * Sort all the values in the container
     */
    void sort();

    /**
     * Sort the values inside the given interval [begin, end)
     */
    void sort(size_t begin, size_t end);

    /**
     * Concatenate multiple arrays together. The result is a new array.
     */
    static std::unique_ptr<CByteArray> merge(CByteArray** arrays, size_t arrays_sz);

    /**
     * Compute the number of bytes required to stored the given value.
     */
    static size_t compute_bytes_per_elements(size_t value);
};

} // namespace distribution


#endif /* CBYTEARRAY_HPP_ */
