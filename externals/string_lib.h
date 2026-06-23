#ifndef STRING_LIB_H
#define STRING_LIB_H

#include <assert.h>
#include <string.h>


#include "why_so_arena.h"
#include "array_list.h"

#define STRING_LIB_LOCAL static

typedef struct string_t{
    size_t capacity;
    size_t len;
    char* ptr;
} string_t;

STRING_LIB_LOCAL string_t 
string_lib_init_capacity(arena_allocator_t *allocator, size_t init_size);

STRING_LIB_LOCAL string_t 
string_lib_init_with_strlit(arena_allocator_t *allocator, const char *str_lit);

STRING_LIB_LOCAL string_t 
string_lib_init_slice(arena_allocator_t *allocator, slice_t src_slice);

STRING_LIB_LOCAL slice_t 
string_lib_to_slice(const string_t *dst);


STRING_LIB_LOCAL slice_t 
string_lib_to_slice_chunk(const string_t *dst, size_t offset, size_t len);


STRING_LIB_LOCAL int 
string_lib_append_string(arena_allocator_t *allocator, string_t *dst, string_t *src);

STRING_LIB_LOCAL int 
string_lib_append_strlit(arena_allocator_t *allocator, string_t *dst, const char *str_lit);

STRING_LIB_LOCAL int 
string_lib_append_char(arena_allocator_t *allocator, string_t *dst, const char src_char);

STRING_LIB_LOCAL int 
string_lib_to_cstring(arena_allocator_t *allocator, string_t *string, char **cstring);

STRING_LIB_LOCAL int
string_lib_append_slice(arena_allocator_t *allocator, string_t *dst, const slice_t str_slice);

STRING_LIB_LOCAL int
string_lib_string_equal(string_t *lhs, string_t *rhs);

STRING_LIB_LOCAL int 
string_lib_slice_to_cstring(arena_allocator_t *allocator, slice_t src_slice, char **cstring);

STRING_LIB_LOCAL slice_t 
string_lib_cstring_in_slice(string_t *str, slice_t *cstring_slice);


STRING_LIB_LOCAL array_list_t
string_lib_split_string(arena_allocator_t *allocator, string_t *str, slice_t pattern_slice);

STRING_LIB_LOCAL string_t
string_lib_shrink_len(string_t *str, size_t shrink_len);

typedef struct string_fragment_t {
    slice_t data;
    struct string_fragment_t *next;
} string_fragment_t;


typedef struct string_builder_t {
    arena_allocator_t *allocator;
    string_fragment_t *head;
    string_fragment_t *tail;
    size_t count;
} string_builder_t;


STRING_LIB_LOCAL string_builder_t 
string_lib_sb_init(arena_allocator_t *allocator);


STRING_LIB_LOCAL int 
string_lib_sb_append_format(string_builder_t *sb, const char *format, ...);


STRING_LIB_LOCAL int 
string_lib_sb_append_slice(string_builder_t *sb, const slice_t *slice);


STRING_LIB_LOCAL int 
string_lib_sb_append(string_builder_t *sb, const char *str);


STRING_LIB_LOCAL string_t 
string_lib_sb_get_string(string_builder_t *sb, arena_allocator_t *allocator);



#ifdef STRING_LIB_IMPLEMENTATION

string_t 
string_lib_init_capacity(arena_allocator_t *allocator, size_t init_size)
{
    assert((0 < init_size)&&"init size should be > 0");
    slice_t string_slice = arena_allocator_alloc(allocator, char, init_size);
    return (string_t){
        .capacity = init_size,
        .len = 0,
        .ptr = (char *)string_slice.ptr
    };
}


string_t 
string_lib_init_slice(arena_allocator_t *allocator, slice_t src_slice)
{
    slice_t string_slice = arena_allocator_alloc(allocator, char, src_slice.len_in_bytes << 1);
    memmove(string_slice.ptr, src_slice.ptr, src_slice.len_in_bytes);
    return (string_t){
        .capacity = src_slice.len_in_bytes << 1,
        .len = src_slice.len_in_bytes,
        .ptr = (char *)string_slice.ptr
    };
}

// the literal being passed is cloned into the string object
string_t 
string_lib_init_with_strlit(arena_allocator_t *allocator, const char *str_lit)
{
    size_t str_len = strlen(str_lit);
    size_t init_size = (str_len << 1);
    
    slice_t string_slice = arena_allocator_alloc(allocator, char, init_size);
    if (0 == string_slice.len_in_bytes) return (string_t){0};
    memcpy(string_slice.ptr, str_lit, str_len);

    return (string_t){
        .capacity = string_slice.len_in_bytes,
        .len = str_len,
        .ptr = (char *)string_slice.ptr
    };
}


// side-effect: Table doubling
int 
string_lib_append_string(arena_allocator_t *allocator, string_t *dst, string_t *src)
{
    size_t len = src->len;
    size_t expected_len = dst->len + src->len;
    slice_t new_slice = (slice_t){.len_in_bytes = dst->capacity, .ptr = dst->ptr};
    if (0 == new_slice.len_in_bytes) return 1;
    if (dst->capacity < expected_len){
        dst->capacity = expected_len << 1;
        new_slice = arena_allocator_resize(allocator, char, new_slice, dst->capacity);
    }
    memmove(&(new_slice.ptr[dst->len]), src->ptr, len);
    dst->len = expected_len;
    dst->ptr = new_slice.ptr;
    return 0;

}


int 
string_lib_append_strlit(arena_allocator_t *allocator, string_t *dst, const char *str_lit)
{
    if (NULL == str_lit) return 0;
    size_t len = strlen(str_lit);
    size_t expected_len = dst->len + len;
    slice_t new_slice = (slice_t){.len_in_bytes = dst->capacity, .ptr = dst->ptr};
    if (0 == new_slice.len_in_bytes) return 1;
    if (dst->capacity < expected_len){
        dst->capacity = expected_len << 1;
        new_slice = arena_allocator_resize(allocator, char, new_slice, dst->capacity);
    }
    memmove(&(new_slice.ptr[dst->len]), str_lit, len);
    dst->len = expected_len;
    dst->ptr = new_slice.ptr;
    return 0;

}


int
string_lib_append_slice(arena_allocator_t *allocator, string_t *dst, const slice_t str_slice)
{
    if (NULL == str_slice.ptr) return 0;
    size_t expected_len = dst->len + str_slice.len_in_bytes;
    slice_t new_slice = (slice_t){.len_in_bytes = dst->capacity, .ptr = dst->ptr};
    if (NULL == new_slice.ptr) return 1;
    if (dst->capacity < expected_len){
        dst->capacity = expected_len << 1;
        new_slice = arena_allocator_resize(allocator, char, new_slice, dst->capacity);
    }
    memmove(&(new_slice.ptr[dst->len]), str_slice.ptr, str_slice.len_in_bytes);
    dst->len = expected_len;
    dst->ptr = new_slice.ptr;
    return 0;
}


slice_t 
string_lib_to_slice(const string_t *dst)
{
    return (slice_t){
        .ptr = dst->ptr,
        .len_in_bytes = dst->len,
    };
}


// side-effect: Table doubling
int 
string_lib_append_char(arena_allocator_t *allocator, string_t *dst, const char src_char)
{
    size_t expected_len = dst->len + 1;
    slice_t new_slice = (slice_t){.len_in_bytes = dst->capacity, .ptr = dst->ptr};
    if (NULL == new_slice.ptr) return 1;
    if (dst->capacity < expected_len){
        dst->capacity = expected_len << 1;
        new_slice = arena_allocator_resize(allocator, char, new_slice, dst->capacity);
    }
    memmove(&(new_slice.ptr[dst->len]), &src_char, 1);
    dst->len = expected_len;
    dst->ptr = new_slice.ptr;
    return 0;
}


int 
string_lib_to_cstring(arena_allocator_t *allocator, string_t *string, char **cstring)
{
    slice_t cstring_slice = arena_allocator_alloc(allocator, char, string->len + 1);
    if (NULL == cstring_slice.ptr) return 1;
    assert((0 != cstring_slice.len_in_bytes)&&"Unable to allocate slice");
    memset(cstring_slice.ptr, 0, cstring_slice.len_in_bytes);
    memmove(cstring_slice.ptr, string->ptr, string->len);
    *cstring = (char *)cstring_slice.ptr;
    return 0;
}


int 
string_lib_slice_to_cstring(arena_allocator_t *allocator, slice_t src_slice, char **cstring)
{
    slice_t cstring_slice = arena_allocator_alloc(allocator, char, src_slice.len_in_bytes + 1);
    if (NULL == cstring_slice.ptr) return 1;
    assert((0 != cstring_slice.len_in_bytes)&&"Unable to allocate slice");
    memset(cstring_slice.ptr, 0, cstring_slice.len_in_bytes);
    memmove(cstring_slice.ptr, src_slice.ptr, src_slice.len_in_bytes);
    *cstring = (char *)cstring_slice.ptr;
    return 0;
}


int
string_lib_string_equal(string_t *lhs, string_t *rhs)
{
    slice_t l = string_lib_to_slice(lhs);
    slice_t r = string_lib_to_slice(rhs);
    return slice_equal(&l, &r);
}


array_list_t
string_lib_split_string(arena_allocator_t *allocator, string_t *str, slice_t pattern_slice)
{
    array_list_t string_splits = array_list_init_capacity(allocator, slice_t, 1);
    if (NULL == string_splits.ptr) return (array_list_t){0};
    size_t offset = 0;
    size_t i = offset;
    for (; i < str->len; i++){
        if ((i + pattern_slice.len_in_bytes) >= str->len) break;
        slice_t temp = string_lib_to_slice_chunk(str, i, pattern_slice.len_in_bytes);
        if (slice_equal(&pattern_slice, &temp)){
            slice_t item = string_lib_to_slice_chunk(str, offset, (i - offset));
            int ret = array_list_append_item_fn(allocator, &string_splits, (char *)&item);
            if (0 != ret) return (array_list_t){0};
            i += pattern_slice.len_in_bytes;
            offset = i;
        }
    }
    if (i < str->len){
        slice_t item = string_lib_to_slice_chunk(str, offset, (str->len - offset));
        int ret = array_list_append_item_fn(allocator, &string_splits, (char *)&item);
        if (0 != ret) return (array_list_t){0};
    }
    return string_splits;
}


slice_t 
string_lib_to_slice_chunk(const string_t *dst, size_t offset, size_t len)
{
    assert(((offset + len) <= dst->len)&&"Slice is out of bounds");
    return (slice_t){
        .ptr = &(dst->ptr[offset]),
        .len_in_bytes = len
    };
}


string_t
string_lib_shrink_len(string_t *str, size_t shrink_len)
{
    assert((shrink_len <= str->len)&&"Shrink length should be less or equal than string length");
    return (string_t){
        .capacity = str->capacity,
        .len = shrink_len,
        .ptr = str->ptr
    };
}


slice_t 
string_lib_cstring_in_slice(string_t *str, slice_t *cstring_slice)
{
    assert((str->len < cstring_slice->len_in_bytes)&&"Slice cannot hold string");
    memcpy(cstring_slice->ptr, str->ptr, str->len);
    memset(cstring_slice->ptr + str->len, 0, cstring_slice->len_in_bytes - str->len);
    return (slice_t){
        .ptr = cstring_slice->ptr,
        .len_in_bytes = str->len,
    };
}


string_builder_t 
string_lib_sb_init(arena_allocator_t *allocator)
{
    return (string_builder_t){
        .allocator = allocator,
        .count = 0,
        .head = NULL,
        .tail = NULL,
    };
}


int 
string_lib_sb_append(string_builder_t *sb, const char *strlit)
{
    string_fragment_t *new_node = (string_fragment_t *) arena_allocator_alloc_item(sb->allocator, string_fragment_t);
    string_t str = string_lib_init_with_strlit(sb->allocator, strlit);
    new_node->data = string_lib_to_slice(&str);
    new_node->next = NULL;
    string_fragment_t *current_node = sb->head;
    while(current_node->next->next != NULL){
        current_node = current_node->next;
    }
    current_node->next->next = new_node;
    return 0;
}


string_t 
string_lib_sb_get_string(string_builder_t *sb, arena_allocator_t *allocator)
{
    string_fragment_t *current_node = sb->head;
    if (NULL != current_node){
        string_t str = string_lib_init_slice(allocator, current_node->data);
        current_node = current_node->next;
        while (NULL != current_node) {
            int ret = string_lib_append_slice(allocator, &str, current_node->data);
            if (0 != ret) return (string_t){0};
            current_node = current_node->next;
        }
        return str;
    }
    return (string_t){0};
}





#endif

#endif /* STRING_LIB_H */