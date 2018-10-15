/*
 * Copyright (c) 2014 Pablo Montes <pabmont@gmail.com>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#ifndef __PABMONT_UTIL_H_
#define __PABMONT_UTIL_H_

#include <assert.h>
#include <stdint.h>
#include <stdbool.h>



/*
 * Returns the 1-based index of the last (i.e., most significant) bit set in x.
 */
// Apparently inline has a different meaning in C, acting simply as hint
static inline uint64_t last_bit_set (uint64_t x) {
  assert (x > 0);
  return (sizeof (uint64_t) * 8 - __builtin_clzll (x));
}

// Apparently inline has a different meaning in C, acting simply as hint
static inline uint64_t floor_lg (uint64_t x) {
  return (last_bit_set (x) - 1);
}

// Apparently inline has a different meaning in C, acting simply as hint
static inline uint64_t ceil_lg (uint64_t x) {
  return (last_bit_set (x - 1));
}

/* Returns the largest power of 2 not greater than x
 * (i.e., $2^{\lfloor \lg x \rfloor}$).
 */
// Apparently inline has a different meaning in C, acting simply as hint
static inline uint64_t hyperfloor (uint64_t x) {
  return (1 << floor_lg (x));
}

/* Returns the smallest power of 2 not less than x
 * (i.e., $2^{\lceil \lg x \rceil}$).
 */
// Apparently inline has a different meaning in C, acting simply as hint
static inline uint64_t hyperceil (uint64_t x) {
  return (1 << ceil_lg (x));
}

// Apparently inline has a different meaning in C, acting simply as hint
static inline uint64_t ceil_div (uint64_t x, uint64_t y) {
  assert (x > 0);
  return (1 + ((x - 1) / y));
}

#endif  /* __PABMONT_UTIL_H_ */
