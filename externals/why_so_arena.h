// #define WSA_IMPLEMENTATION
// I have to handle fragmentation, to avoid waste

#ifndef WHY_SO_ARENA_H
#define WHY_SO_ARENA_H

#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <string.h>
#include <assert.h>
#include <stdalign.h>


#include <assert.h>

#define ARENA_LOCAL static
#define ARENA_PUB   

// Slice utils
#define GET_SLICE_LEN(slice, type_)                     slice.len_in_bytes/sizeof(type_)
#define BEGIN_ITR(slice, type_)                         (type_ *)slice.ptr
#define END_ITR(itr, slice, type_)                      itr < (type_ *)slice.ptr + GET_SLICE_LEN(slice, type_)
#define BOUNDS(slice, type_)                            (type_ *)slice.ptr + GET_SLICE_LEN(slice, type_)

// Allocator utils
#define KB(byte)                                        (byte * 1024UL)
#define MB(byte)                                        (byte * 1024UL * 1024UL)
#define DEFAULT_ALIGNMENT                               (2 * sizeof(void *))
#define DEFAULT_PAGE_SIZE                               KB(2)
// #define MAX_ALIGNMENT                                   _Alignof(max_align_t)


// These are the goodies
#define arena_allocator_alloc(allocator, T, len)                    arena_allocator_alloc_aligned(allocator, len, sizeof(T), DEFAULT_ALIGNMENT)
#define arena_allocator_alloc_item(allocator, T)                    arena_allocator_alloc_item_aligned(allocator, sizeof(T), DEFAULT_ALIGNMENT);
#define arena_allocator_resize(allocator, T, old_slice, new_len)    arena_allocator_resize_aligned(allocator, old_slice, new_len, sizeof(T), DEFAULT_ALIGNMENT)
#define arena_allocator_init_page_default(allocator, capacity)      arena_allocator_init(allocator, capacity, DEFAULT_PAGE_SIZE)
#define arena_allocator_dup(allocator, input_slice)                 arena_allocator_dup_aligned(allocator, input_slice, DEFAULT_ALIGNMENT)


// Slice yippy!
typedef struct slice_t {
    void *ptr;
    size_t len_in_bytes;
} slice_t;

typedef struct const_slice_t {
    const void *buf;
    const size_t len_in_bytes;
} const_slice_t;


typedef struct allocator_vtable {
	void (*free)(void *ptr);
	slice_t (*alloc)(size_t len, size_t size_);
} allocator_vtable;


// linear allocator
typedef struct arena_t {
    unsigned char* base_address;
    size_t capacity; // capacity in bytes
    size_t prev_offset;
    size_t offset;
} arena_t;


// Linked list with allocator
typedef struct arena_linked_node_t {
    arena_t arena;
    struct arena_linked_node_t* next;
} arena_linked_node_t;


typedef struct arena_allocator_t {
    size_t page_size;
    allocator_vtable allocator;
    arena_linked_node_t *linkedlist;
    arena_linked_node_t *tail_linkedlist;
} arena_allocator_t;


ARENA_LOCAL slice_t
make_slice(void *object, size_t len_in_bytes);

ARENA_LOCAL slice_t
make_const_slice(char *object);

ARENA_LOCAL const_slice_t
make_const_slice_v1(const char *object);

ARENA_LOCAL int
slice_equal(const slice_t *lhs, const slice_t *rhs);

ARENA_LOCAL slice_t 
interface_alloc(size_t len, size_t size_);

ARENA_LOCAL void 
interface_free(void *ptr);
// void interface_realloc(void *ptr); I will implement this is my free time


// I just kept it minimal

ARENA_LOCAL const allocator_vtable c_allocator = (allocator_vtable){.free = interface_free, .alloc = interface_alloc,};


ARENA_LOCAL slice_t 
interface_alloc(size_t len, size_t size_){
    void *ptr = malloc(len * size_);
    if (NULL == ptr) return (slice_t){};
	return make_slice(ptr, len * size_);
}


ARENA_LOCAL void 
interface_free(void *ptr){
    free(ptr);
}


// @todo: Implement resize/realloc
ARENA_LOCAL arena_t 
arena_init(allocator_vtable allocator, size_t capacity);

ARENA_LOCAL uintptr_t 
align_forward(uintptr_t ptr, uintptr_t alignment_);

ARENA_LOCAL slice_t 
arena_alloc_aligned(arena_t *arena, size_t len, size_t size_, size_t alignment_);

ARENA_LOCAL slice_t 
arena_resize_aligned(arena_t *arena, slice_t old_slice, size_t len, size_t size_, size_t alignment_);

ARENA_LOCAL void 
arena_reset(arena_t *arena);

ARENA_LOCAL void 
arena_deinit(allocator_vtable allocator, arena_t *arena);

ARENA_LOCAL arena_allocator_t 
arena_allocator_init(allocator_vtable allocator, size_t capacity, size_t page_size);

ARENA_LOCAL slice_t 
arena_allocator_alloc_aligned(arena_allocator_t *arena_allocator, size_t len, size_t size_, size_t alignment_);

ARENA_LOCAL void* 
arena_allocator_alloc_item_aligned(arena_allocator_t *arena_allocator, size_t size_, size_t alignment_);

ARENA_LOCAL void 
arena_allocator_reset(arena_allocator_t *arena_allocator);

ARENA_LOCAL void 
arena_allocator_deinit(arena_allocator_t *arena_allocator);


ARENA_LOCAL slice_t
arena_allocator_resize_aligned(arena_allocator_t *arena_allocator, slice_t allocated_slice, size_t new_len, size_t new_size, size_t alignment_);

ARENA_LOCAL slice_t
arena_allocator_dup_aligned(arena_allocator_t *arena_allocator, slice_t input_slice, size_t alignment_);



#ifdef WSA_IMPLEMENTATION 
arena_allocator_t 
arena_allocator_init(allocator_vtable allocator, size_t capacity, size_t page_size)
{
    assert((0 < page_size)&&"Page size should always be greater than zero");
    assert((0 < capacity)&&"Capacity should always be greater than zero");
    if (capacity < page_size){capacity = page_size;}
    arena_linked_node_t *node = NULL;
    slice_t slice = allocator.alloc(1, sizeof(*node));
    if (0 == slice.len_in_bytes) return (arena_allocator_t){};
    node = slice.ptr;
    arena_t new_arena = arena_init(allocator, capacity);
    if (NULL == new_arena.base_address) return (arena_allocator_t){};
    *node = (arena_linked_node_t){.arena = new_arena, .next = NULL,};
    return (arena_allocator_t){
        .page_size = page_size,
        .allocator = allocator,
        .linkedlist = node,
        .tail_linkedlist = node,
    };
}


slice_t 
arena_allocator_alloc_aligned(arena_allocator_t *arena_allocator, size_t len, size_t size_, size_t alignment_)
{
    assert((NULL != arena_allocator->linkedlist)&&"arena_t allocator was not initialized");
    arena_linked_node_t *current_node = arena_allocator->tail_linkedlist;
    slice_t result = arena_alloc_aligned(&(current_node->arena), len, size_, alignment_);
    if (!result.ptr){
        // printf("Creating new sizeable arena\n");
        slice_t slice = arena_allocator->allocator.alloc(1, sizeof(*current_node->next));
        if (0 == slice.len_in_bytes) return (slice_t){};
        arena_linked_node_t *new_node = slice.ptr;
        if (NULL == new_node) return (slice_t){};

        size_t page_allocation = arena_allocator->page_size;
        while (page_allocation < size_ * len) page_allocation += arena_allocator->page_size;
        arena_t new_arena = arena_init(arena_allocator->allocator, page_allocation);
        if (NULL == new_arena.base_address) return (slice_t){};

        *new_node = (arena_linked_node_t){
            .arena = new_arena,
            .next = NULL,
        };
        current_node->next = new_node;
        arena_allocator->tail_linkedlist = current_node->next;
        return arena_alloc_aligned(&(new_node->arena), len, size_, alignment_);
    }
    return result;
}


void* 
arena_allocator_alloc_item_aligned(arena_allocator_t *arena_allocator, size_t size_, size_t alignment_)
{
    slice_t item_slice = arena_allocator_alloc_aligned(arena_allocator, 1, size_, alignment_);
    return item_slice.ptr;
}


void 
arena_allocator_reset(arena_allocator_t *arena_allocator)
{
    arena_linked_node_t *current_node = arena_allocator->linkedlist;
    while (NULL != current_node){
        arena_reset(&(current_node->arena));
        current_node = current_node->next;
    }
}



void 
arena_allocator_deinit(arena_allocator_t *arena_allocator)
{
    arena_linked_node_t *current_node = arena_allocator->linkedlist;
    while(NULL != current_node){
        arena_linked_node_t *temp_next = current_node->next;
        arena_deinit(arena_allocator->allocator, &(current_node->arena));
        arena_allocator->allocator.free(current_node);
        current_node = NULL;
        current_node = temp_next;
    }
}



arena_t 
arena_init(allocator_vtable allocator, size_t capacity)
{
    slice_t buf = allocator.alloc(1, capacity);
    if (0 == buf.len_in_bytes) {
        return (arena_t){};
    } else {   
        return (arena_t){.base_address = buf.ptr, .capacity = capacity, .offset = 0};
    }
}



uintptr_t 
align_forward(uintptr_t ptr, uintptr_t alignment_)
{
    // Assume the aligment with always be a power of 2
    // check if the current offset is divisible by 2
    assert((0 == (alignment_ & (alignment_ - 1)))&&"Alignment should be in power of 2");
    uintptr_t modulo = ptr & (alignment_ - 1);
    if (0 != modulo) ptr = ptr + alignment_ - modulo;
    return ptr;
}




slice_t 
arena_alloc_aligned(arena_t *arena, size_t len, size_t size_, size_t alignment_)
{
    // assert((MAX_ALIGNMENT > alignment_)&&"Exceeded maximum alignment");
    uintptr_t curr_offset = (uintptr_t)arena->base_address + (uintptr_t)arena->offset;
    uintptr_t offset = align_forward(curr_offset, alignment_) - (uintptr_t) arena->base_address;
    // the we check if the arena can contain new item(s)
    if ((offset + (len*size_)) <= arena->capacity){
        void *allocated = &arena->base_address[offset];
        arena->offset = offset + (len * size_);
        arena->prev_offset = offset;
        return make_slice(allocated, len * size_);
    } else {
        return make_slice(NULL, 0);
    }
}


slice_t 
arena_resize_aligned(arena_t *arena, slice_t old_slice, size_t new_len, size_t size_, size_t alignment_)
{   
    slice_t new_slice = arena_alloc_aligned(arena, new_len, size_, alignment_);
    memmove(new_slice.ptr, old_slice.ptr, old_slice.len_in_bytes);
    return new_slice;
}


void 
arena_reset(arena_t *arena)
{
    arena->offset = 0;
    arena->prev_offset = 0;
}


void 
arena_deinit(allocator_vtable allocator, arena_t *arena)
{
    if (arena->base_address != NULL) allocator.free(arena->base_address);
    arena->base_address = 0;
    arena->capacity = 0;
    arena->offset = 0;
    arena->prev_offset = 0;
}


slice_t
arena_allocator_resize_aligned(arena_allocator_t *arena_allocator, slice_t allocated_slice, size_t new_len, size_t size_, size_t alignment_)
{
    assert((0 < new_len)&&"New length should always be greater than zero");
    assert((0 < size_)&&"New size should always be greater than zero");
    assert((NULL != allocated_slice.ptr)&&"Slice should not be NULL, try to allocatoe it");
    assert((NULL != arena_allocator->linkedlist)&&"arena_t allocator was not initialized");
    slice_t result = (slice_t){};
    if ((new_len * size_) < allocated_slice.len_in_bytes) result = (slice_t){.len_in_bytes = new_len * size_, .ptr = allocated_slice.ptr};
    else {
        // Search for slice's arena
        arena_linked_node_t *current_node = arena_allocator->linkedlist;
        uintptr_t arena_lower_bound = 0; uintptr_t arena_upper_bound = 0;

        while (NULL != current_node){
            arena_lower_bound = (uintptr_t)current_node->arena.base_address;
            arena_upper_bound = arena_lower_bound + current_node->arena.capacity;
            if (arena_lower_bound <= (uintptr_t) allocated_slice.ptr 
                && arena_upper_bound > (uintptr_t) allocated_slice.ptr){
                break;
            }
            current_node = current_node->next;
        }
        assert((NULL != current_node)&&"Slice does not point to any arena, ensure you are using the arena the was use to create the slice");
        slice_t new_slice = arena_allocator_alloc_aligned(arena_allocator, new_len, size_, alignment_);
        assert((0 != new_slice.len_in_bytes)&&"Empty slice");
        memmove(new_slice.ptr, allocated_slice.ptr, allocated_slice.len_in_bytes);
        result = new_slice;
    }
    return result;
}


slice_t
arena_allocator_dup_aligned(arena_allocator_t *arena_allocator, slice_t input_slice, size_t alignment_)
{
    assert((0 != input_slice.len_in_bytes)&&"Cannot duplicate empty slice");
    slice_t new_slice = arena_allocator_alloc_aligned(arena_allocator, input_slice.len_in_bytes, 1, alignment_);
    if (0 == new_slice.len_in_bytes) return (slice_t){0};
    memmove(new_slice.ptr, input_slice.ptr, input_slice.len_in_bytes);
    return new_slice;
}


slice_t
make_slice(void *object, size_t len_in_bytes)
{
    if (NULL == object) {return (slice_t){0};}
    return (slice_t){
        .ptr = object,
        .len_in_bytes = len_in_bytes,
    };
}


const_slice_t
make_const_slice_v1(const char *object)
{
    size_t len_in_bytes = 0;
    size_t i = 0;
    if (NULL == object) {return (const_slice_t){0};}
    while ('\0' != object[i]){i++;}
    len_in_bytes = i * sizeof(char);
    return (const_slice_t){
        .buf = object,
        .len_in_bytes = len_in_bytes,
    };
}


slice_t
make_const_slice(char *object)
{
    size_t len_in_bytes = 0;
    size_t i = 0;
    if (NULL == object) {return (slice_t){0};}
    while ('\0' != object[i]){i++;}
    len_in_bytes = i * sizeof(char);
    return (slice_t){
        .ptr = object,
        .len_in_bytes = len_in_bytes,
    };
}


int
slice_equal(const slice_t *lhs, const slice_t *rhs)
{
    if (lhs->len_in_bytes != rhs->len_in_bytes) return 0;
    if (0 == memcmp(lhs->ptr, rhs->ptr, lhs->len_in_bytes)) return 1;
    return 0;
}
#endif

#endif

