/**
 * Implementation of PMA by user jraizes:
 * https://github.com/jraizes/PackedMemoryArray
 *
 * Licensed according to the MIT License:
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2016 encyclopedea
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

#ifndef JRAIZES_PKD_MEM_ARR
#define JRAIZES_PKD_MEM_ARR
#ifdef __cplusplus
extern "C" {
#endif

# include <inttypes.h>
# include <stdlib.h>
# include <limits.h>
# include <math.h>
# include <stdio.h>

// The elements contained in the PMA
typedef struct {
    int64_t key;
    int64_t value;
} raizes_PMA_element; // renamed to avoid conflicts

typedef struct packed_memory_array{
    raizes_PMA_element* array;
    char* bitmap;
    int	size;
    int	count;
} raizes_PMA;  // renamed to avoid conflicts

int PMA_get_count(raizes_PMA* pma);
raizes_PMA_element* PMA_get_pointer(raizes_PMA* pma, int64_t key);
int PMA_find(raizes_PMA* pma, int64_t key);
int PMA_insert(raizes_PMA* pma, int64_t key, int64_t value);
raizes_PMA* PMA_new();
void PMA_print_bitmap(raizes_PMA* p);
void PMA_print_content(raizes_PMA* m);
int PMA_remove(raizes_PMA* pma, int64_t key);
void PMA_free(raizes_PMA* pma);

#ifdef __cplusplus
} /* extern "C" */
#endif
#endif /* JRAIZES_PKD_MEM_ARR */
