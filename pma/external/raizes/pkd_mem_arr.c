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

#include "pkd_mem_arr.h"

# include   <inttypes.h> // PRId64
# include   <stdbool.h>

# define    U_TRHLD 0.5
# define    L_TRHLD 0.25
# define    BASE_SIZE   8
# define    U_FACTOR    2
# define    D_FACTOR    0.5

#define PMA_element raizes_PMA_element
#define PMA raizes_PMA
/*
 * The following MACROS are courtesy of Professor Darrell Long
 * They were provided for an assigment in his class, and I have
 * seen fit to use them here.
 */
# define    SETBIT(A, k) A[(k) >> 3] |=  (01 << ((k) & 07))
# define    CLRBIT(A, k) A[(k) >> 3] &= ~(01 << ((k) & 07))
# define    GETBIT(A, k) (A[(k) >> 3] & (01 << ((k) & 07))) >> ((k) & 07)

typedef PMA_element element_t;

// prototypes
//void get_array(PMA* pma, int* arr);
static int count_in_range(PMA* pma, int b_lim, int t_lim);
static int closest_in_range(PMA* pma, int index, int b_lim, int t_lim);
static int next_greater(PMA* pma, int index);
static void redistribute(PMA* pma, int* nbrhd, int insert, int64_t key, int64_t value);
static void resize(PMA* pma, double factor, int insert, int64_t key, int64_t value);
static int* find_lower_neighborhood(PMA* pma, int index);
static int* find_upper_neighborhood(PMA* pma, int index);
static int rebalance(PMA* pma, int index, int insert, int64_t key, int64_t value);
static int* make_triple(int arg1, int arg2, int arg3);
static int* recursive_find(PMA* pma, int64_t key, int b_lim, int t_lim);
static int find(PMA* pma, int64_t key, int exact);
static element_t* get_pointer(PMA* pma, int64_t key);

PMA* PMA_new(){
	PMA* pma	= (PMA*) malloc(sizeof(PMA));

	pma->size	= BASE_SIZE;
	pma->count	= 0;
	pma->array	= (element_t*) malloc(sizeof(element_t) * pma->size);
	pma->bitmap	= (char*) malloc(ceil(pma->size / CHAR_BIT));

	int i;
	for (i = 0; i < ceil(pma->size / CHAR_BIT); i++){
		pma->bitmap[i] = 0;
	}

	return pma;
}

void PMA_free(PMA* pma){
	free(pma->bitmap);
	free(pma->array);
	free(pma);
}

/*
 * Inserts an element into the packed memory array.
 * Returns the number of moves (0 if not rebalanced).
 */
int PMA_insert(PMA* pma, int64_t key, int64_t value){
	int index = find(pma, key, 0);
	pma->count++;

	if (GETBIT(pma->bitmap, index)){
		return rebalance(pma, index, 1, key, value);
	}

	pma->array[index].key = key;
	pma->array[index].value = value;
	SETBIT(pma->bitmap, index);
	return 0;
}

/*
 * Attempts to remove an element from the array.
 * Returns 0 if unsuccessful (element did not exist),
 * or the number of moves made if successful (will 
 * likely be greater than 0).
 *
 * NOTE: The element is not actually removed. Rather,
 * it's location is simply set to unused.
 */
int PMA_remove(PMA* pma, int64_t key){
	int index = find(pma, key, 1);

	if (index == -1){
		return 0;
	}
	CLRBIT(pma->bitmap, index);
	pma->count--;
	return rebalance(pma, index, 0, -1, -1);
}

/*
 * Attempts to find an element in the array and return
 * a pointer to it. Returns NULL on failure.
 */
static element_t* get_pointer(PMA* pma, int64_t key){
	int index = find(pma, key, 1);
	return (index == -1) ? NULL : pma->array + index;
}

element_t* PMA_get_pointer(PMA* pma, int64_t key){
    return get_pointer(pma, key);
}

/**
 * Interface for the function #find
 */
int PMA_find(PMA* pma, int64_t key){
    return find(pma, key, 1);
}

/*
 * Finds an element in the packed memory array.
 * If exact is 1, attempts to find the exact element and
 * returns -1 on failure. If exact is 0, attempts to find
 * a place to insert the element.
 */
static int find(PMA* pma, int64_t key, int exact){
	int* range = recursive_find(pma, key, -1, pma->size);
	int index, next;
	
	if (exact){
		index = range[2];
	} else{
		/* If an exact match was found, find the next 
		   used slot and put it between the two.
		   Otherwise, place it in the middle of the range	*/
		if (range[2] != -1){
			next = next_greater(pma, range[2]);
			/* Ensure that next is not -1 */
			next = (next == -1) ? pma->size - 1 : next;
			index = (range[2] + next) / 2;
		} else{
			index = (range[0] + range[1]) / 2;
		}
		
	}

	free(range);
	return index;
}

/*
 * The back-end for find. Recursively locates a range
 * of values that includes the target, and returns the range.
 * If an exact match is found, its index is stored in the
 * 3rd location of the array returned (this is -1 otherwise).
 *
 * NOTE: The array returned MUST be freed.
 */
static int* recursive_find(PMA* pma, int64_t key, int b_lim, int t_lim){
//	int index = -1;
	int cur;
	if (t_lim - b_lim <= 1){
			return make_triple(b_lim, t_lim, -1);
	}

	cur = (b_lim + t_lim) / 2;
	cur = closest_in_range(pma, cur, b_lim, t_lim);

	if (cur == -1 || pma->array[cur].key == key){
		return make_triple(b_lim, t_lim, cur);

	}else if (pma->array[cur].key > key){
		return recursive_find(pma, key, b_lim, cur);

	} else{
		return recursive_find(pma, key, cur, t_lim);
	}

}

/*
 * A helper function. Places 3 arguments into a newly
 * allocated array.
 *
 * NOTE: the array returned MUST be freed.
 */
static int* make_triple(int arg1, int arg2, int arg3){
	int* triple = (int*) malloc(sizeof(int) * 3);
	triple[0] = arg1;
	triple[1] = arg2;
	triple[2] = arg3;
	return triple;
}


/*
 * Rebalances the packed memory array.
 * If insert is 1, elem will be inserted into the array in
 * the appropriate location and the rebalance will occur
 * using the upper threshold. If insert is 0, then the 
 * rebalance will occur using the lower threshold.
 */
static int rebalance(PMA* pma, int index, int insert, int64_t key, int64_t value){
	int moves;
	int* nbrhd;
	if (insert){
		nbrhd = find_upper_neighborhood(pma, index);
	} else{
		nbrhd = find_lower_neighborhood(pma, index);
	}


	/* If the neighborhood encompasses the entire array, 
	   a resize is required. Otherwise, redistribute
	   as normal										*/
	if (nbrhd[0] == 0 && nbrhd[1] == pma->size){
		double factor = (insert == 1) ? U_FACTOR : D_FACTOR;
		resize(pma, factor, insert, key, value);
	} else{
		redistribute(pma, nbrhd, insert, key, value);
	}

	moves = nbrhd[2];
	free(nbrhd);
	return moves;
}

/*
 * Locates a neighborhood for a rebalance. Uses the upper
 * threshold.
 *
 * Returned array format: [bottom, top_lim, count]
 *
 * NOTE: the array returned MUST be freed.
 */
static int* find_upper_neighborhood(PMA* pma, int index){
	int count;
	int width = 2;
	int prev_bot, prev_top;
	int bot = floor(index / width) * width;
	int top = bot + width;
	double progression = U_TRHLD / log2(pma->size);
	double local_thrhld = 1 - progression;

	count = count_in_range(pma, bot - 1, top) + 1;

	
	while (width < pma->size && (double)count / width > local_thrhld){

		width *= 2;
		prev_bot = bot;
		prev_top = top;
		bot = floor(index / width) * width;
		top = bot + width;
		local_thrhld -= progression;

		count += count_in_range(pma, bot - 1, prev_bot);
		count += count_in_range(pma, prev_top - 1, top);
	}

	return make_triple(bot, top, count);
}

/*
 * Locates a neighborhood for a rebalance. Uses the lower
 * threshold.
 *
 * Returned array format: [bottom, top_lim, count]
 *
 * NOTE: the array returned MUST be freed.
 */
static int* find_lower_neighborhood(PMA* pma, int index){
	int count;
	int width = 2;
	int prev_bot, prev_top;
	int bot = floor(index / width) * width;
	int top = bot + width;
	double progression = L_TRHLD / log2(pma->size);
	double local_thrhld = progression;

	count = count_in_range(pma, bot - 1, top);

	while (width < pma->size && (double)count / width < local_thrhld){
		width *= 2;
		prev_bot = bot;
		prev_top = top;
		bot = floor(index / width) * width;
		top = bot + width;
		local_thrhld += progression;

		count += count_in_range(pma, bot - 1, prev_bot);
		count += count_in_range(pma, prev_top - 1, top);
	}

	return make_triple(bot, top, count);
}

/*
 * Resizes the packed memory array by the factor argument.
 * If insert is 1, inserts elem into the packed memory 
 * array during the resizing.
 */
static void resize(PMA* pma, double factor, int insert, int64_t key, int64_t value){
	int new_size = ceil(pma->size * factor);
	element_t* new_array = (element_t*) malloc(sizeof(element_t) * new_size);
	char* new_bitmap = (char*) malloc(ceil((double)new_size / CHAR_BIT));
	double dist = new_size * 1.0 / pma->count;
	int count = 0;
	int cur = -1;
	
	/* Clear the new bitmap */
	int i;
	for (i = 0; i < ceil((double)new_size / CHAR_BIT); i++){
		new_bitmap[i] = 0;
	}

	while (count < pma->count){
		cur = next_greater(pma, cur);

		if (insert && (cur == -1 || key < pma->array[cur].key)){
		    element_t* ptr = new_array + (int)(count * dist);
			ptr->key = key;
			ptr->value = value;
			SETBIT(new_bitmap, (int)(count * dist));
			insert = 0;
			count++;
		}

		if (cur != -1){
//            new_array[(int)(count * dist)] = pma->array[cur];
		    element_t* ptr = new_array + (int)(count * dist);
		    ptr->key = pma->array[cur].key;
		    ptr->value = pma->array[cur].value;
			SETBIT(new_bitmap, (int)(count * dist));
			count++;
		}

	}

	free(pma->array);
	free(pma->bitmap);
	pma->array = new_array;
	pma->bitmap = new_bitmap;
	pma->size = new_size;
}

/*
 * Redistributes elements in the neighborhood evenly.
 * If insert is 1, inserts elem into the packed memory
 * array during the redistribution.
 */
static void redistribute(PMA* pma, int* nbrhd, int insert, int64_t key, int64_t value){
	double dist = (nbrhd[1] - nbrhd[0]) / (double)nbrhd[2];
	element_t* temp = (element_t*) malloc(sizeof(element_t) * nbrhd[2]);
	int count = 0;
	int cur = nbrhd[0] - 1;

	/* Copy everything to a separate array. This is simpler
	   than topological sorting.								*/
	while (count < nbrhd[2]){
		cur = next_greater(pma, cur);

		if (insert && (cur >= nbrhd[1] || cur == -1 || key < pma->array[cur].key)){
			temp[count].key = key;
			temp[count].value = value;
			insert = 0;
			count++;
		}

		if (cur < nbrhd[1] && cur != -1){
		    // temp[count] = pma->array[cur];
		    temp[count].key = pma->array[cur].key;
		    temp[count].value = pma->array[cur].value;
			CLRBIT(pma->bitmap, cur);
			count++;
		}
	}

	for (count = 0; count < nbrhd[2]; count++){
		pma->array[nbrhd[0] + (int)(count * dist)] = temp[count];
		SETBIT(pma->bitmap, nbrhd[0] + (int)(count * dist));
	}

	free(temp);
}


/*
 * Finds the next position which is both used and 
 * whose index is greater than the index argument.
 * Returns -1 on failure.
 */
static int next_greater(PMA* pma, int index){
	int next	= -1;
	index += 1;

	while (next == -1 && index < pma->size){
		if (GETBIT(pma->bitmap, index)){
			next = index;
		}
		index += 1;
	}

	return next;
}


/*
 * Finds the closest used position in the packed memory
 * array (including index) within a range (non-inclusive). 
 * Returns -1 on failure.
 */
static int closest_in_range(PMA* pma, int index, int b_lim, int t_lim){
	int closest = -1;
	int radius = 0;
	int fail = 0;

	while (!fail && closest == -1){
		fail = 1;

		if (t_lim > index + radius){
			fail = 0;
			if (GETBIT(pma->bitmap, index + radius)){
				closest = index + radius;
			}
		}

		if (b_lim < index - radius){
			fail = 0;
			if (GETBIT(pma->bitmap, index - radius)){
				closest = index - radius;
			}
		}

		radius++;
	}

	return closest;
}

/*
 * Counts the number of elements in a packed memory array
 * within a certain range (non-inclusive).
 */
static int count_in_range(PMA* pma, int b_lim, int t_lim){
	int count = 0;
	int i;

	for (i = b_lim + 1; i < t_lim; i++){
		count += GETBIT(pma->bitmap, i);
	}

	return count;
}


/*
 * Writes the contents of the packed memory array to a 
 * a regular array;
 */
//void get_array(PMA* pma, int* arr){
//	int cur = -1;
//	int count = 0;
//
//	while (count < pma->count){
//		cur = next_greater(pma, cur);
//		arr[count] = pma->array[cur];
//		count++;
//	}
//}


/*
 * Returns the number of elements in the packed
 * memory array.
 *
 * Mostly here for abstraction purposes.
 */
int PMA_get_count(PMA* pma){
	return pma->count;
}


/*
 * Prints the contents of the packed memory
 * array in order.
 */
void PMA_print_content(PMA* m){
	int i;
	bool first = true;
	printf("\n[");

	for (i = 0; i < m->size; i++){
		if (GETBIT(m->bitmap, i)){
		    if(!first){ printf(", "); } else { first = false; }
		    element_t e = m->array[i];
			printf("<%"PRId64",%"PRId64">", e.key, e.value);
		} //else{
		//	printf("unused, ");
		//}
	}
	printf("]\n");
}


void PMA_print_bitmap(PMA* p){
	int i;
	printf("\nBitmap: ");
	for (i = 0; i < p->size; i++){
		printf("%d", GETBIT(p->bitmap, i));
	}
	printf("\n");
}
