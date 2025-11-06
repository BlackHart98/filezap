#ifndef _SPLITTER_H_
#define _SPLITTER_H_

#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <assert.h>
#include <stdarg.h>
#include <errno.h>
#if !defined(_WIN32)
    #include <pthread.h>
    #include <sqlite3.h>
    #include <unistd.h>
    #include <fcntl.h>
    #include <sys/types.h>
    #include <sys/stat.h>
#else
    #include "sqlite3.h"
#endif

#include "stb_ds.h"
#include "json.h"
#include "../hash/xxhash.h"


#define MKFIFO_IF_ONLY_EXISTS(fifo_path, mode) \
    do {\
        int ret = mkfifo(fifo_path, mode);\
        (void)ret;\
    }while(0)

#define FZ_INFO 0
#define FZ_WARNING 1
#define FZ_ERROR 2

#define KB(size_) (size_ * 1024UL) 
#define MB(size_) (size_ * 1024UL * 1024UL) 
#define MAX_THREADS 3
#define MAX_QUEUE_LENGTH 10
#define RESERVED KB(1)
#define LARGE_RESERVED KB(4)
#define XSMALL_RESERVED 256
#define XXSMALL_RESERVED 128
#define MAX_MANIFEST_SIZE MB(64)

#define RETURN_DEFER(val) do{result = val; goto defer;} while(0)
#define SERIALIZE_CHUNK(buffer, chunk_checksum, cutpoint, chunk_size)\
    do{\
        snprintf(\
            buffer,\
            sizeof(buffer),\
            "{\"chunk_checksum\":\"%016llx\",\"cutpoint\":%lu,\"chunk_size\":%lu}",\
            chunk_checksum,\
            cutpoint,\
            chunk_size);\
    }while(0)


#if defined(__GNUC__) || defined(__clang__)
//   https://gcc.gnu.org/onlinedocs/gcc-4.7.2/gcc/Function-Attributes.html
#    ifdef __MINGW_PRINTF_FORMAT
#        define FZ_PRINTF_FORMAT(STRING_INDEX, FIRST_TO_CHECK) __attribute__ ((format (__MINGW_PRINTF_FORMAT, STRING_INDEX, FIRST_TO_CHECK)))
#    else
#        define FZ_PRINTF_FORMAT(STRING_INDEX, FIRST_TO_CHECK) __attribute__ ((format (printf, STRING_INDEX, FIRST_TO_CHECK)))
#    endif // __MINGW_PRINTF_FORMAT
#else
//   TODO: implement NOB_PRINTF_FORMAT for MSVC
#    define FZ_PRINTF_FORMAT(STRING_INDEX, FIRST_TO_CHECK)
#endif

#define REQUEST_FIFO "request"
#define RESPONSE_FIFO "response"

#define SEND_CONN_FLAG(flag) \
    do {\
        char number_as_str[XXSMALL_RESERVED] = {0};\
        snprintf(number_as_str, XXSMALL_RESERVED, "%lu", flag);\
        if (!fz_channel_write_response(channel, number_as_str, XXSMALL_RESERVED)) RETURN_DEFER(0);\
    } while(0)

#define RECV_CONN_FLAG(flag) \
    do {\
        char number_as_str[XXSMALL_RESERVED] = {0};\
        if (!fz_channel_read_response(channel, number_as_str, XXSMALL_RESERVED)) RETURN_DEFER(0);\
        flag = strtoul(number_as_str, NULL, 10);\
    } while(0)


typedef uintptr_t fz_ctx_desc_t;
typedef XXH64_hash_t fz_hex_digest_t;

typedef struct fz_chunk_t{
    fz_hex_digest_t chunk_checksum;
    char *src_file_path;
    size_t cutpoint;
    size_t chunk_size;
    size_t chunk_index;
} fz_chunk_t;


typedef struct fz_chunk_dlist_t{
    fz_chunk_t *buffer;
    size_t len;
} fz_chunk_dlist_t;

typedef struct fz_chunk_seq_t{
    fz_hex_digest_t *chunk_checksum;
    size_t *cutpoint;
    size_t *chunk_size;
    size_t chunk_seq_len;
} fz_chunk_seq_t;

typedef struct fz_file_manifest_t {
    char *file_name;
    fz_hex_digest_t file_checksum;

    fz_chunk_seq_t chunk_seq;

    size_t file_size;
    fz_ctx_desc_t source_id;
} fz_file_manifest_t;


typedef struct fz_ctx_attr_t {
    size_t chunk_size;
    size_t prefetch_size;
    size_t in_mem_buffer;
} fz_ctx_attr_t;


enum FZ_CHUNK_STRATEGY {
    FZ_FIXED_SIZED_CHUNK = (0x1 << 0),
};


enum FZ_HASHING_ALGORITHM {
    FZ_HASH_SHA256 = (0x1 << 0),
    FZ_HASH_XXHASH = (0x1 << 1)
};


enum FZ_CHANNEL_DESC_T {
    FZ_FIFO = (0x1 << 0),
    FZ_TCP_SOCKET = (0x1 << 1),
};


enum FZ_RECEIVER_MODE {
    FZ_RECEIVER_MODE = (0x1 << 0),
    FZ_SENDER_MODE = (0x1 << 1),
};


/* Very tentative channel struct */
typedef struct fz_channel_t{
    int type;
    char *channel_desc;
} fz_channel_t;


typedef struct fz_chunk_request_t{
    fz_chunk_t chunk_meta; /* I might change thisinto a pointer to a chunk instead */
    fz_hex_digest_t checksum;
    uintptr_t dest_id;

    /* This is supposed to hold the identifier for the source of the chunk */ 
    fz_ctx_desc_t source_id;
}fz_chunk_request_t;


typedef struct fz_chunk_response_t {
    fz_hex_digest_t checksum;

    size_t chunk_index;
} fz_chunk_response_t;


typedef struct fz_ringbuffer_t{
    int read_head, write_head;
    pthread_mutex_t mtx;
    pthread_cond_t is_empty;
    pthread_cond_t is_full;
    fz_chunk_request_t buffer[MAX_QUEUE_LENGTH];
} fz_ringbuffer_t;


typedef struct fz_dyn_queue_t{
    fz_chunk_response_t *buffer;
    size_t front, rear;
    size_t capacity;
    pthread_mutex_t mtx;
} fz_dyn_queue_t;


struct thread_arg {
    int *failed;
    fz_ringbuffer_t *wq;
    fz_channel_t *channel;
    pthread_mutex_t *mtx;
    pthread_cond_t *done_cv;
    size_t *failed_tasks;
    ssize_t *remaining_tasks;
    fz_dyn_queue_t *download_queue;
};

struct download_thread_arg {
    int *failed;
    fz_channel_t *channel;
    pthread_mutex_t *mtx;
    pthread_cond_t *done_cv;
    fz_dyn_queue_t *download_queue;

};


struct fz_fifo_channel_s{
    int request_d;
    int response_d;
    const char *response;
    const char *request;
    pthread_mutex_t mtx;
    pthread_cond_t done_cv;
};


typedef struct fz_ctx_t{
    fz_ctx_desc_t ctx_id;
    int chunk_strategy;
    int hash_algorithm;

    /* this is location where all the data and metadata are kept after chunking */
    const char* metadata_loc;
    const char* target_dir;
    fz_ctx_attr_t ctx_attrs;

    fz_ringbuffer_t wq;
    size_t max_threads;

    sqlite3 *db;
} fz_ctx_t;


typedef struct fz_cutpoint_list_t{
    fz_hex_digest_t *buffer;
    size_t *cutpoint;
    size_t *chunk_size;
    size_t cutpoint_len;
} fz_cutpoint_list_t;


struct cutpoint_map_s {char *key; fz_cutpoint_list_t *value;};
struct missing_chunks_map_s {fz_hex_digest_t key; int8_t value;};


extern int fz_ctx_init(fz_ctx_t *ctx, uintptr_t ctx_id, int chunk_strategy, const char *metadata_loc, const char *target_dir, const char *db_file, int *max_threads, fz_ctx_attr_t *ctx_attrs);
extern void fz_ctx_destroy(fz_ctx_t *ctx);
extern int fz_chunk_file(fz_ctx_t *ctx, fz_file_manifest_t *file_mnfst, const char* src_file_path);
extern int fz_commit_chunk_meta(fz_file_manifest_t *file_mnfst, int db_conn);

/* For the first iteration I will make use of a named pipe to simulate a socket communication channel then eventually replace with an actual socket */ 
extern int fz_send_file(fz_ctx_t *ctx, fz_channel_t *channel, const char *src_file_path);
extern int fz_receive_file(fz_ctx_t *ctx, fz_channel_t *channel);
extern int fz_serialize_manifest(fz_file_manifest_t *mnfst, char **json, size_t *json_size);
extern int fz_deserialize_manifest(const char *json, fz_file_manifest_t *mnfst);


/* Thread function for fetching chunks */
extern void* fz_fetch_chunk(void *);

/* Fetch file from manifest */ 
extern int fz_fetch_file(fz_ctx_t *ctx, fz_file_manifest_t *file_mnfst, fz_channel_t *channel, fz_dyn_queue_t *download_queue);
extern int fz_retrieve_file(fz_ctx_t *ctx, fz_file_manifest_t *file_mnfst, fz_channel_t *channel, char *file_name);
extern int fz_fetch_file_st(fz_ctx_t *ctx, fz_file_manifest_t *mnfst, fz_channel_t *channel, fz_dyn_queue_t *download_queue, struct cutpoint_map_s **cutpoint_map, struct missing_chunks_map_s **missing_chunks);

extern int fz_seed_local_files(fz_ctx_t *ctx, const char *dir);
extern int fz_seed_local_file(fz_ctx_t *ctx, const char *src_file_path, int db_conn);


extern int fz_chunk_init(fz_chunk_seq_t *chnk);
extern void fz_chunk_destroy(fz_chunk_seq_t *chnk);


extern int fz_file_manifest_init(fz_file_manifest_t *mnfst);
extern void fz_file_manifest_destroy(fz_file_manifest_t *mnfst);

extern int fz_file_prefetch(void);
extern int fz_file_manifest_to_chunk_list(fz_file_manifest_t *mnfst, fz_chunk_t *chunk_list, char *dest_file_path);
extern int fz_derive_receive_file_manifest(fz_ctx_t *ctx, fz_file_manifest_t *sndr_mnfst, fz_file_manifest_t *recv_mnfst);


extern void fz_ring_buffer_init(fz_ringbuffer_t *rb);
extern int fz_ring_buffer_empty(const fz_ringbuffer_t *rb);
extern int fz_ring_buffer_full(const fz_ringbuffer_t *rb);
extern int fz_enqueue(fz_ringbuffer_t *rb, fz_chunk_request_t req);
extern int fz_dequeue(fz_ringbuffer_t *rb, fz_chunk_request_t *req);
extern void fz_ring_buffer_destroy(fz_ringbuffer_t *rb);


extern int fz_dyn_queue_init(fz_dyn_queue_t *dyn_queue, size_t capacity);
extern int fz_dyn_enqueue(fz_dyn_queue_t *dyn_queue, fz_chunk_response_t item);
extern int fz_dyn_dequeue(fz_dyn_queue_t *dyn_queue, fz_chunk_response_t *item);
extern int fz_dyn_queue_empty(const fz_dyn_queue_t *dq);
extern int fz_dyn_queue_full(const fz_dyn_queue_t *dq);
extern void fz_dyn_queue_destroy(fz_dyn_queue_t *dyn_queue);


extern void xxhash_hexdigest(char *buffer, size_t stream_len, fz_hex_digest_t *digest);
extern int xxhash_hexdigest_from_file(FILE *fd, fz_hex_digest_t *digest);
extern int xxhash_hexdigest_from_file_prime(fz_hex_digest_t *digest, fz_hex_digest_t *digest_list, size_t digest_list_len);


extern int fz_serialize_response(fz_chunk_response_t *response, char **json, size_t *json_size);
extern int fz_deserialize_response(char *json, fz_chunk_response_t *response);


extern int fz_channel_init(fz_channel_t *channel, int channel_desc, int mode);
extern void fz_channel_destroy(fz_channel_t *channel);
extern int fz_channel_read_response(fz_channel_t *channel, char *buffer, size_t data_size, char *scratchpad, size_t scratchpad_size);
extern int fz_channel_write_response(fz_channel_t *channel, char *buffer, size_t data_size);
extern int fz_channel_read_request(fz_channel_t *channel, char *buffer, size_t data_size, char *scratchpad, size_t scratchpad_size);
extern int fz_channel_write_request(fz_channel_t *channel, char *buffer, size_t data_size);

extern int fz_cutpoint_list_init(fz_cutpoint_list_t *cutpoint_list);
extern void fz_cutpoint_list_destroy(fz_cutpoint_list_t *cutpoint_list);

extern int fz_fetch_chunks_from_file_cutpoint(
    fz_ctx_t *ctx, 
    fz_file_manifest_t *mnfst, 
    fz_chunk_t *chunk_buffer, 
    size_t nchunk, 
    struct cutpoint_map_s **cutpoint_map,
    struct missing_chunks_map_s **missing_chunks);


/* Query: find required chunk list */
extern int fz_query_required_chunk_list(fz_ctx_t *ctx, fz_file_manifest_t *mnfst, fz_chunk_t **chunk_buffer, size_t *nchunk, struct missing_chunks_map_s **missing_chunks);


extern int fz_commit_chunk_metadata(fz_ctx_t *ctx, fz_file_manifest_t *mnfst, char *dest_file_path);


extern void fz_log(int level, const char *fmt, ...) FZ_PRINTF_FORMAT(2, 3);

#endif /* _SPLITTER_H_ */