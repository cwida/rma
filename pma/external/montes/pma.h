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

#ifndef __PABMONT_PMA_H_
#define __PABMONT_PMA_H_

#ifdef __cplusplus
extern "C" {
#endif

#include <stdio.h>

#include <assert.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdlib.h>

#include "keyval.h"
#include "util.h"

struct _pma;
typedef struct _pma pma_t, *PMA;

PMA pma_create (void);
PMA pma_from_array (pma_element_t *array, uint64_t n);
void pma_destroy (PMA *pma);
bool pma_find (PMA pma, pma_key_t key, int64_t *index);
bool pma_insert (PMA pma, pma_key_t key, pma_val_t val);
void pma_insert_after (PMA pma, int64_t i, pma_key_t key, pma_val_t val);
bool pma_delete (PMA pma, pma_key_t key);
void pma_delete_at (PMA pma, int64_t i);
void pma_get (PMA pma, int64_t i, pma_element_t *keyval);
uint64_t pma_capacity (PMA p);
uint64_t pma_count (PMA p);

/* TODO: For testing purposes only. */
uint8_t pma_segment_size (PMA pma);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif  /* __PABMONT_PMA_H_ */
