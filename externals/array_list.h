#ifndef ARRAY_LIST_H
#define ARRAY_LIST_H

#include <assert.h>


#include "why_so_arena.h"

#define ARRAY_LIST_LOCAL static

#define array_list_init_capacity(allocator, T, len)    array_list_init_capacity_fn(allocator, len, sizeof(T))
 

typedef struct array_list_t{
    const size_t size; // type size
    size_t capacity;
    size_t len;
    char* ptr;
} array_list_t;

ARRAY_LIST_LOCAL array_list_t 
array_list_init_capacity_fn(arena_allocator_t *allocator, size_t init_capacity, size_t size);

ARRAY_LIST_LOCAL int 
array_list_append_item_fn(arena_allocator_t *allocator, array_list_t *dst, const char *item);

ARRAY_LIST_LOCAL int 
array_list_append_slice_fn(arena_allocator_t *allocator, array_list_t *dst, const slice_t buf);

ARRAY_LIST_LOCAL void 
array_list_max_bound_fn(array_list_t *dst);


ARRAY_LIST_LOCAL int 
array_list_get_item_fn(array_list_t *dst, char *item, size_t pos);


ARRAY_LIST_LOCAL int 
array_list_insert_item_fn(array_list_t *dst, char *item, size_t pos);


ARRAY_LIST_LOCAL void 
array_list_reset_fn(array_list_t *array_list);


#ifdef ARRAY_LIST_IMPLEMENTATION

array_list_t 
array_list_init_capacity_fn(arena_allocator_t *allocator, size_t init_capacity, size_t size)
{
    assert((0 < init_capacity)&&"init size should be > 0");
    slice_t string_slice = arena_allocator_alloc_aligned(allocator, (init_capacity * size), size, DEFAULT_ALIGNMENT);
    if(0 == string_slice.len_in_bytes) return (array_list_t){0};
    return (array_list_t){
        .capacity = init_capacity,
        .len = 0,
        .size = size,
        .ptr = (char *)string_slice.ptr
    };
}


void 
array_list_max_bound_fn(array_list_t *dst)
{
    dst->len = dst->capacity;
}


int 
array_list_get_item_fn(array_list_t *dst, char *item, size_t pos)
{
    if (pos > dst->len) return 1;
    memmove(item, &(dst->ptr[pos * dst->size]), dst->size);
    return 0;
}


int 
array_list_insert_item_fn(array_list_t *dst, char *item, size_t pos)
{
    if (pos > dst->len) return 1;
    memmove(&(dst->ptr[pos * dst->size]), item, dst->size);
    return 0;
}




// side-effect: Table doubling
int 
array_list_append_item_fn(arena_allocator_t *allocator, array_list_t *dst, const char *item)
{
    size_t expected_len = dst->size * (1 + dst->len);
    slice_t new_slice = (slice_t){.len_in_bytes = dst->size * dst->capacity, .ptr = dst->ptr};
    if (0 == new_slice.len_in_bytes) return 1;
    if (dst->capacity * dst->size < expected_len){
        new_slice = arena_allocator_resize_aligned(allocator, new_slice, expected_len << 1, dst->size, DEFAULT_ALIGNMENT);
        dst->capacity = (1 + dst->len) << 1;
    }
    memmove(&(new_slice.ptr[dst->len * dst->size]), item, dst->size);
    dst->len += 1;
    dst->ptr = new_slice.ptr;
    return 0;

}


int 
array_list_append_slice_fn(arena_allocator_t *allocator, array_list_t *dst, const slice_t buf)
{
    size_t expected_len = (dst->size * dst->len) + buf.len_in_bytes;
    slice_t new_slice = (slice_t){.len_in_bytes = dst->size * dst->capacity, .ptr = dst->ptr};
    if (0 == new_slice.len_in_bytes) return 1;
    if (dst->capacity * dst->size < expected_len){
        new_slice = arena_allocator_resize_aligned(allocator, new_slice, expected_len << 1, dst->size, DEFAULT_ALIGNMENT);
        dst->capacity = ((size_t)(buf.len_in_bytes/dst->size) + dst->len);
    }
    memmove(&(new_slice.ptr[dst->len * dst->size]), buf.ptr, buf.len_in_bytes);
    dst->len += (size_t)(buf.len_in_bytes/dst->size);
    dst->ptr = new_slice.ptr;
    return 0;

}

void 
array_list_reset_fn(array_list_t *array_list)
{
    array_list->len = 0;
}



#endif

#endif /* ARRAY_LIST_H */