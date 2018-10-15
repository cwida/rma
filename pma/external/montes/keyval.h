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

#ifndef __KEYVAL_H_
#define __KEYVAL_H_

#include <stdint.h>
#include <stdbool.h>

typedef int64_t pma_key_t;
typedef int64_t pma_val_t;

typedef struct {
  pma_key_t key;
  pma_val_t val;
} pma_element_t;

/* Returns true if keyval is empty and false otherwise. */
static inline bool keyval_empty (const pma_element_t *keyval) {
  return (keyval->key == -1);
}

/* Sets keyval to be empty. */
static inline void keyval_clear (pma_element_t *keyval) {
  keyval->key = -1;
//  keyval->val = 0ULL;
}

#endif  /* __KEYVAL_H_ */
