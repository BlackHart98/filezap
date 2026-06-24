#ifndef BITSET_H
#define BITSET_H

#include <stdio.h>
#include <string.h>
#include <limits.h>


#include "why_so_arena.h"
#include "array_list.h"

#define BITSET_LOCAL static

#define MSB     (1 << 7)

typedef struct bitset_t {
    unsigned char *bits;
    size_t size;
    size_t len;
} bitset_t;


BITSET_LOCAL bitset_t
bitset_init(arena_allocator_t *allocator, size_t size);


BITSET_LOCAL void
bitset_add(bitset_t *bitset, size_t item);


BITSET_LOCAL char
bitset_test(bitset_t *bitset, size_t item);


BITSET_LOCAL void
bitset_toggle(bitset_t *bitset, size_t item);


BITSET_LOCAL void
bitset_clear(bitset_t *bitset);


#ifdef BITSET_IMPLEMENTATION

bitset_t
bitset_init(arena_allocator_t *allocator, size_t len)
{
    assert((8 <= len)&&"Length should be greater than 8");
    size_t size = (len / 8) + (((len % 8) > 0)? 1 : 0);
    slice_t buf_slice = arena_allocator_alloc(allocator, unsigned char, size);
    if (NULL == buf_slice.ptr) return (bitset_t){0};
    memset(buf_slice.ptr, 0, buf_slice.len_in_bytes);
    return (bitset_t){
        .bits = (unsigned char *)buf_slice.ptr,
        .len = len,
        .size = buf_slice.len_in_bytes,
    };
}


void
bitset_add(bitset_t *bitset, size_t item)
{
    assert((NULL != bitset)&&"Bitset is NULL");
    size_t index = item / 8;
    size_t sub_str_index = (bitset->len - (index * 8)) - (bitset->len - item);
    bitset->bits[index] |= (unsigned char)(MSB >> sub_str_index);
}


char
bitset_test(bitset_t *bitset, size_t item)
{
    assert((NULL != bitset)&&"Bitset is NULL");
    assert((bitset->len > item)&&"Query item should be 0 <= item < bitset->len");
    size_t index = item / 8;
    size_t sub_str_index = (bitset->len - (index * 8)) - (bitset->len - item);
    return bitset->bits[index] & (unsigned char)(MSB >> sub_str_index);
}


void
bitset_toggle(bitset_t *bitset, size_t item)
{
    assert((NULL != bitset)&&"Bitset is NULL");
    size_t index = item / 8;
    size_t sub_str_index = (bitset->len - (index * 8)) - (bitset->len - item);
    bitset->bits[index] &= ~(unsigned char)(MSB >> sub_str_index);
}


void
bitset_clear(bitset_t *bitset)
{
    memset(bitset->bits, 0, bitset->size);
}


#endif

#endif /* BITSET_H */