#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include "core.h"

#define DEFAULT_METADATA_LOC "tmp/"
#define SUPPORTED_STRATEGIES (FZ_FIXED_SIZED_CHUNK | FZ_GEAR_CDC_CHUNK)
#define SUPPORTED_HASHING (FZ_HASH_SHA256 | FZ_HASH_XXHASH)
#define FIXED_SIZED_DEFAULT KB(64)
#define IN_MEMORY_BUFFER_DEFAULT MB(1)
#define PREFETCH_DEFAULT 4

#define SET_CHUNK_PARAM_DEFAULTS(ctx, chunk_strategy) \
    do {\
        if (FZ_FIXED_SIZED_CHUNK & chunk_strategy){\
            (ctx)->ctx_attrs.chunk_size = FIXED_SIZED_DEFAULT;\
        } else {\
            \
        }\
        (ctx)->ctx_attrs.prefetch_size = PREFETCH_DEFAULT;\
        (ctx)->ctx_attrs.in_mem_buffer = IN_MEMORY_BUFFER_DEFAULT;\
    }while(0)

int fz_minimal_log_level = FZ_INFO;


extern int fz_ctx_init(
    fz_ctx_t *ctx,
    int chunk_strategy, 
    const char *metadata_loc, 
    const char *target_dir,
    const char *db_file,
    int *max_threads,
    fz_ctx_attr_t *ctx_attrs
){
    assert((NULL != db_file)&&"db file should not be null");
    int result = 1;
    int _max_threads = 0;
    int ret = 0;
    if (!(chunk_strategy & SUPPORTED_STRATEGIES)) return 0;
    if (NULL == target_dir) return 0;
    ctx->target_dir = target_dir;
    ctx->chunk_strategy = chunk_strategy;
    if (NULL != metadata_loc){
        ctx->metadata_loc = metadata_loc;
    } else {
        ctx->metadata_loc = DEFAULT_METADATA_LOC;
    }
    if (NULL == max_threads || 0 <= *max_threads){
        _max_threads = MAX_THREADS;
    } else {
        _max_threads = *max_threads;
    }
    fz_ring_buffer_init(&(ctx->wq));
    ctx->max_threads = _max_threads;
    if (NULL == ctx_attrs){
        SET_CHUNK_PARAM_DEFAULTS(ctx, chunk_strategy);
    } else {
        if (FZ_FIXED_SIZED_CHUNK & chunk_strategy){
            assert((0 > ctx_attrs->chunk_size)&&"");
            ctx->ctx_attrs = (fz_ctx_attr_t){.chunk_size = ctx_attrs->chunk_size,};
        } else {
            assert(0&&"Todo! chunk strategy not yet supported");
        }
    }
    ret = sqlite3_open(db_file, &(ctx->db));
    if (ret) {
        fz_log(FZ_ERROR, "Unable to create filezap database");
        RETURN_DEFER(0);
    }
    defer:
        return result;
}


extern void fz_ctx_destroy(fz_ctx_t *ctx){
    fz_ring_buffer_destroy(&(ctx->wq));
    if (NULL != ctx->db) sqlite3_close(ctx->db);
}


extern int fz_chunk_init(fz_chunk_seq_t *chnk){
    chnk->chunk_checksum = NULL;
    chnk->chunk_size = NULL;
    chnk->cutpoint = NULL;
    return 1;
}


extern void fz_chunk_destroy(fz_chunk_seq_t *chnk){
    if (NULL != chnk->chunk_checksum) free(chnk->chunk_checksum);
    if (NULL != chnk->cutpoint) free(chnk->cutpoint);
    if (NULL != chnk->chunk_size) free(chnk->chunk_size);
    chnk->chunk_checksum = NULL;
    chnk->cutpoint = NULL;
    chnk->chunk_size = NULL;
    chnk->chunk_seq_len = 0;
}


extern int fz_file_manifest_init(fz_file_manifest_t *mnfst){
    fz_chunk_init(&mnfst->chunk_seq);
    mnfst->file_checksum = 0;
    mnfst->source_id = 0;
    mnfst->file_name = NULL;
    mnfst->file_size = 0;

    return 1;
}


extern void fz_file_manifest_destroy(fz_file_manifest_t *mnfst){
    fz_chunk_destroy(&mnfst->chunk_seq);
    mnfst->file_checksum = 0;
    mnfst->source_id = 0;
    if (NULL != mnfst->file_name) free(mnfst->file_name);
    mnfst->file_name = NULL;
    mnfst->file_size = 0;
}


extern void fz_ring_buffer_init(fz_ringbuffer_t *ring_buffer){ 
    static_assert(2 <= MAX_QUEUE_LENGTH, "MAX_QUEUE_LENGTH should be at least 2");
    assert((NULL != ring_buffer)&&"Ring buffer argument should not be not");
    ring_buffer->read_head = 0;
    ring_buffer->write_head = 0;
    pthread_mutex_init(&ring_buffer->mtx, NULL);
    pthread_cond_init(&ring_buffer->is_empty, NULL);
    pthread_cond_init(&ring_buffer->is_full, NULL);
}


extern int fz_ring_buffer_full(const fz_ringbuffer_t *ring_buffer){
    int next_idx = (ring_buffer->write_head + 1) % MAX_QUEUE_LENGTH;
    if (next_idx == ring_buffer->read_head) return 1;
    return 0;
}


extern int fz_ring_buffer_empty(const fz_ringbuffer_t *ring_buffer){
    if (ring_buffer->write_head == ring_buffer->read_head) return 1;
    return 0;
}


extern void fz_ring_buffer_destroy(fz_ringbuffer_t *ring_buffer){
    ring_buffer->read_head = 0;
    ring_buffer->write_head = 0;
    pthread_mutex_destroy(&ring_buffer->mtx);
    pthread_cond_destroy(&ring_buffer->is_empty);
    pthread_cond_destroy(&ring_buffer->is_full);
}


extern int fz_enqueue(fz_ringbuffer_t *ring_buffer, fz_chunk_request_t item){
    pthread_mutex_lock(&ring_buffer->mtx);
    while (fz_ring_buffer_full(ring_buffer)){
        pthread_cond_wait(&ring_buffer->is_empty, &ring_buffer->mtx);
    }
    ring_buffer->buffer[ring_buffer->write_head] = item;
    ring_buffer->write_head = (ring_buffer->write_head + 1) % MAX_QUEUE_LENGTH;
    pthread_mutex_unlock(&ring_buffer->mtx);
    return 1;
}


extern int fz_dequeue(fz_ringbuffer_t *ring_buffer, fz_chunk_request_t *item){
    pthread_mutex_lock(&ring_buffer->mtx);
    if (fz_ring_buffer_empty(ring_buffer)){
        pthread_cond_signal(&ring_buffer->is_empty);
        pthread_mutex_unlock(&ring_buffer->mtx);
        return 0;
    }
    *item = ring_buffer->buffer[ring_buffer->read_head];
    ring_buffer->read_head = (ring_buffer->read_head + 1) % MAX_QUEUE_LENGTH;
    pthread_mutex_unlock(&ring_buffer->mtx);
    return 1;
}


extern int fz_dyn_queue_init(fz_dyn_queue_t *dyn_queue, size_t capacity){ 
    assert((0 < capacity)&&"Capacity should be greater than zero");
    dyn_queue->buffer = (fz_chunk_response_t *)calloc(capacity, sizeof(fz_chunk_response_t));
    if (NULL == dyn_queue->buffer) return 0;
    dyn_queue->front = 0;
    dyn_queue->rear = 0;
    pthread_mutex_init(&dyn_queue->mtx, NULL);
    return 1;
}


extern void fz_dyn_queue_destroy(fz_dyn_queue_t *dyn_queue){
    dyn_queue->front = 0;
    dyn_queue->rear = 0;
    pthread_mutex_destroy(&dyn_queue->mtx);
    if (NULL != dyn_queue->buffer) free(dyn_queue->buffer);
}


extern int fz_dyn_queue_full(const fz_dyn_queue_t *dyn_queue){
    if (dyn_queue->capacity == dyn_queue->front) return 1;
    return 0;
}


extern int fz_dyn_queue_empty(const fz_dyn_queue_t *dyn_queue){
    if (dyn_queue->front == dyn_queue->rear) return 1;
    return 0;
}


extern int fz_dyn_enqueue(fz_dyn_queue_t *dyn_queue, fz_chunk_response_t item){
    pthread_mutex_lock(&dyn_queue->mtx);
    if (fz_dyn_queue_full(dyn_queue)){
        if ((dyn_queue->rear - dyn_queue->front) + 1 == dyn_queue->capacity){
            size_t capacity = dyn_queue->capacity << 1;
            dyn_queue->buffer = realloc(dyn_queue->buffer, capacity * sizeof(fz_chunk_response_t));
            if (NULL == dyn_queue->buffer) {
                pthread_mutex_unlock(&dyn_queue->mtx);
                return 0;
            }
            dyn_queue->capacity = capacity;
        } else {
            memmove(dyn_queue->buffer, &dyn_queue->buffer[dyn_queue->front], (dyn_queue->rear - dyn_queue->front));
            dyn_queue->front = 0;
            dyn_queue->rear = (dyn_queue->rear - dyn_queue->front);
        }
    }
    dyn_queue->buffer[dyn_queue->rear] = item;
    dyn_queue->rear = (dyn_queue->rear + 1);
    pthread_mutex_unlock(&dyn_queue->mtx);
    return 1;
}


extern int fz_dyn_dequeue(fz_dyn_queue_t *dyn_queue, fz_chunk_response_t *item){
    pthread_mutex_lock(&dyn_queue->mtx);
    if (fz_dyn_queue_empty(dyn_queue)){
        pthread_mutex_unlock(&dyn_queue->mtx);
        return 0;
    }
    *item = dyn_queue->buffer[dyn_queue->front];
    dyn_queue->front = (dyn_queue->front + 1);
    pthread_mutex_unlock(&dyn_queue->mtx);
    return 1;
}


extern void fz_log(int level, const char *fmt, ...){
    if (level < fz_minimal_log_level) return;

    switch (level) {
        case FZ_INFO:
            fprintf(stderr, "[INFO] ");
            break;
        case FZ_WARNING:
            fprintf(stderr, "[WARNING] ");
            break;
        case FZ_ERROR:
            fprintf(stderr, "[ERROR] ");
            break;
        default:
            assert(0&&"Unreachable!");
    }

    va_list args;
    va_start(args, fmt);
    vfprintf(stderr, fmt, args);
    va_end(args);
    fprintf(stderr, "\n");
}


extern int fz_channel_init(fz_channel_t *channel, int channel_desc, int mode){
    int result = 1;
    char *buffer = NULL;
    channel->type = channel_desc;
    if (FZ_FIFO & channel_desc){
        buffer = calloc(1, sizeof(struct fz_fifo_channel_s));
        if (NULL == buffer) RETURN_DEFER(0);
        struct fz_fifo_channel_s *c_ptr = (struct fz_fifo_channel_s *)buffer;
#if !defined(_WIN32)
        MKFIFO_IF_ONLY_EXISTS(REQUEST_FIFO, 0666);
        MKFIFO_IF_ONLY_EXISTS(RESPONSE_FIFO, 0666);
#endif  
        pthread_cond_init(&(c_ptr->done_cv), NULL);
        pthread_mutex_init(&(c_ptr->mtx), NULL);

        /* Begin: establishing communication channel */
        c_ptr->request = REQUEST_FIFO;
        c_ptr->response = RESPONSE_FIFO;
        if (FZ_SENDER_MODE & mode){
            c_ptr->request_d = open(c_ptr->request, O_WRONLY);
            c_ptr->response_d = open(c_ptr->response, O_RDONLY);
        } else if (FZ_RECEIVER_MODE & mode){
            c_ptr->request_d = open(c_ptr->request, O_RDONLY);
            c_ptr->response_d = open(c_ptr->response, O_WRONLY);
        } else {
            fz_log(FZ_ERROR, "Failed to create FIFO connection channel");
            RETURN_DEFER(0);
        }
        if (-1 == c_ptr->request_d || -1 == c_ptr->response_d){
            if (-1 != c_ptr->request_d) close(c_ptr->request_d);
            if (-1 != c_ptr->response_d) close(c_ptr->response_d);
            fz_log(FZ_INFO, "Failed to establish channel");
            RETURN_DEFER(0);
        }
        /* end */

        channel->channel_desc = buffer; 
    } else if (FZ_TCP_SOCKET & channel_desc){
        assert(0&&"Todo: Not yet implemented the TCP socket channel");
    } else {
        fz_log(FZ_ERROR, "Unsupported channel, ensure channel passed is supported");
        RETURN_DEFER(0);
    }
    defer:
        if (!result && NULL != buffer){
            free(buffer); buffer = NULL;
        }
        return result;
}


extern void fz_channel_destroy(fz_channel_t *channel){
    if (FZ_FIFO & channel->type){
        struct fz_fifo_channel_s *c_ptr = (struct fz_fifo_channel_s *)channel->channel_desc;
        pthread_cond_destroy(&(c_ptr->done_cv));
        pthread_mutex_destroy(&(c_ptr->mtx));
        if (-1 != c_ptr->request_d || -1 != c_ptr->response_d){
            if (-1 != c_ptr->request_d) close(c_ptr->request_d);
            if (-1 != c_ptr->response_d) close(c_ptr->response_d);
        }
    }
    if (NULL != channel->channel_desc) free(channel->channel_desc);
    channel->channel_desc = NULL;
    channel->type = 0;
}


extern int fz_cutpoint_list_init(fz_cutpoint_list_t *cutpoint_list){
    cutpoint_list->buffer = NULL;
    cutpoint_list->cutpoint = NULL;
    cutpoint_list->chunk_size = NULL;
    cutpoint_list->cutpoint_len = 0;
    return 1;
}


extern void fz_cutpoint_list_destroy(fz_cutpoint_list_t *cutpoint_list){
    if (NULL != cutpoint_list->buffer) free(cutpoint_list->buffer);
    if (NULL != cutpoint_list->cutpoint) free(cutpoint_list->cutpoint);
    if (NULL != cutpoint_list->chunk_size) free(cutpoint_list->chunk_size);
    cutpoint_list->buffer = NULL;
    cutpoint_list->cutpoint = NULL;
    cutpoint_list->chunk_size = NULL;
    cutpoint_list->cutpoint_len = 0;
}