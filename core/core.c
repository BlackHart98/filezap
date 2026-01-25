#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <poll.h>
#include "core.h"

#define DEFAULT_METADATA_LOC "tmp/"
#define SUPPORTED_STRATEGIES (FZ_FIXED_SIZED_CHUNK | FZ_GEAR_CDC_CHUNK)
#define SUPPORTED_HASHING (FZ_HASH_SHA256 | FZ_HASH_XXHASH)
#define FIXED_SIZED_DEFAULT KB(64)
#define IN_MEMORY_BUFFER_DEFAULT MB(1)
#define PREFETCH_DEFAULT 4

#define SET_CHUNK_PARAM_DEFAULTS(ctx, chunk_strategy)\
    do {\
        if (FZ_FIXED_SIZED_CHUNK & chunk_strategy){\
            (ctx)->ctx_attrs.chunk_size = FIXED_SIZED_DEFAULT;\
        } else {\
            \
        }\
        (ctx)->ctx_attrs.prefetch_size = PREFETCH_DEFAULT;\
        (ctx)->ctx_attrs.in_mem_buffer = IN_MEMORY_BUFFER_DEFAULT;\
    }while(0)

#define SET_TCP_CHANNEL_DEFAULTS(channel_attr)\
    do{\
        (channel_attr)->address = "127.0.0.1";\
        (channel_attr)->port = 2000;\
    }while(0)

int fz_minimal_log_level = FZ_INFO;

static inline int fz_deserialize_config(const char *json, fz_config_t *config);


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

    if (NULL != metadata_loc) ctx->metadata_loc = metadata_loc;
    else ctx->metadata_loc = DEFAULT_METADATA_LOC;
    if (NULL == max_threads || 0 <= *max_threads) _max_threads = MAX_THREADS;
    else _max_threads = *max_threads;

    fz_ring_buffer_init(&(ctx->wq));
    ctx->max_threads = _max_threads;
    if (NULL == ctx_attrs) SET_CHUNK_PARAM_DEFAULTS(ctx, chunk_strategy);
    else {
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
            break;
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
        // buffer = calloc(1, sizeof(struct fz_tcp_channel_s));
        // if (NULL == buffer) RETURN_DEFER(0);
        // struct fz_tcp_channel_s *c_ptr = (struct fz_tcp_channel_s *)buffer;
        assert(0&&"Todo: Not yet implemented the TCP socket channel");
    } else {
        fz_log(FZ_ERROR, "Unsupported channel, ensure channel passed is supported");
        RETURN_DEFER(0);
    }
    defer:
        if (!result && NULL != buffer){free(buffer); buffer = NULL;}
        return result;
}


extern int fz_channel_init_v2(fz_channel_t *channel, int channel_desc, int mode, fz_channel_attr_t *channel_attr){
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
        buffer = calloc(1, sizeof(struct fz_tcp_channel_s));
        if (NULL == buffer) RETURN_DEFER(0);

        struct fz_tcp_channel_s *c_ptr = (struct fz_tcp_channel_s *)buffer;
        pthread_cond_init(&(c_ptr->done_cv), NULL);
        pthread_mutex_init(&(c_ptr->mtx), NULL);

        fz_channel_attr_t c_attr = {0};
        if (NULL == channel_attr) SET_TCP_CHANNEL_DEFAULTS(&c_attr);
        else c_attr = *channel_attr;
        if (0 == c_attr.address) c_attr.address = "127.0.0.1"; // host: localhost
        if (0 == c_attr.port) c_attr.port = 2000; // port: 2000
        struct sockaddr_in server = (struct sockaddr_in){
            .sin_addr.s_addr = inet_addr(c_attr.address),
            .sin_family = AF_INET,
            .sin_port = htons(c_attr.port),
        };

        c_ptr->client_d = -1;
        c_ptr->socket_d = -1;

        if (FZ_SENDER_MODE & mode){
            c_ptr->socket_d = socket(AF_INET, SOCK_STREAM, 0);
            if (0 > c_ptr->socket_d) {
                fz_log(FZ_ERROR, "Failed to create socket");
                RETURN_DEFER(0);
            }
            if (0 > connect(c_ptr->socket_d, (struct sockaddr *)&server, sizeof(server))) {
                fz_log(FZ_ERROR, "Failed to connect to server");
                close(c_ptr->socket_d);
                RETURN_DEFER(0);
            }
            fz_log(FZ_INFO, "Here is the socket/server file descriptor: %d", c_ptr->socket_d); 
        } else if (FZ_RECEIVER_MODE & mode){
            struct sockaddr_in client; 
            c_ptr->socket_d = socket(AF_INET, SOCK_STREAM, 0);
            if (c_ptr->socket_d < 0) {
                fz_log(FZ_ERROR, "Failed to create socket");
                RETURN_DEFER(0);
            }

            int opt = 1;
            if (0 > setsockopt(c_ptr->socket_d, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt))) {
                fz_log(FZ_WARNING, "Failed to set SO_REUSEADDR");
            }
            
            fz_log(FZ_INFO, "Here is the socket/client file descriptor: %d", c_ptr->socket_d); 
            if (0 > bind(c_ptr->socket_d, (struct sockaddr *)&server, sizeof(server))) {
                fz_log(FZ_ERROR, "Failed to bind");
                close(c_ptr->socket_d);
                RETURN_DEFER(0);
            }

            if (0 > listen(c_ptr->socket_d, 1)) {
                fz_log(FZ_ERROR, "Failed to listen");
                close(c_ptr->socket_d);
                RETURN_DEFER(0);
            }
            int n = sizeof(client);

            // I need to fix this part of the code, when it fails to connect
            // int flags = fcntl(c_ptr->socket_d, F_GETFL);
            // fcntl(c_ptr->socket_d, F_SETFL, flags | O_NONBLOCK);
            // struct pollfd poll_d;
            // poll_d.fd = c_ptr->socket_d;
            // poll_d.events = POLLIN;
            // int ready = poll(&poll_d, 1, 100);
            // if (0 > ready){
            //     fz_log(FZ_ERROR, "Failed to connect to server");
            //     close(c_ptr->socket_d);
            //     RETURN_DEFER(0);
            // }
            c_ptr->client_d = accept(c_ptr->socket_d, (struct sockaddr *)&client, (socklen_t *)&n);
            if (0 > c_ptr->client_d && EAGAIN == errno) {
                fz_log(FZ_ERROR, "Failed to accept connection");
                close(c_ptr->socket_d);
                RETURN_DEFER(0);
            }
            
            fz_log(FZ_INFO, "Accepted client connection on fd: %d", c_ptr->client_d);
        } else {
            fz_log(FZ_ERROR, "Invalid mode for TCP channel");
            RETURN_DEFER(0);
        }
        if (-1 == c_ptr->socket_d && (FZ_SENDER_MODE & mode)){
            fz_log(FZ_INFO, "Failed to establish sender channel");
            RETURN_DEFER(0);
        } else if (-1 == c_ptr->client_d && (FZ_RECEIVER_MODE & mode)){
            fz_log(FZ_INFO, "Failed to establish receiver channel");
            RETURN_DEFER(0);
        }
        channel->channel_desc = buffer; 
    } else {
        fz_log(FZ_ERROR, "Unsupported channel type");
        RETURN_DEFER(0);
    }
    defer:
        if (!result && NULL != buffer){free(buffer); buffer = NULL;}
        return result;
}


extern void fz_channel_destroy(fz_channel_t *channel){
    if (FZ_FIFO & channel->type){
        struct fz_fifo_channel_s *c_ptr = (struct fz_fifo_channel_s *)channel->channel_desc;
        if (NULL != c_ptr){
            pthread_cond_destroy(&(c_ptr->done_cv));
            pthread_mutex_destroy(&(c_ptr->mtx));
            if (-1 != c_ptr->request_d || -1 != c_ptr->response_d){
                if (-1 != c_ptr->request_d) close(c_ptr->request_d);
                if (-1 != c_ptr->response_d) close(c_ptr->response_d);
            }
        }
    } else if (FZ_TCP_SOCKET & channel->type){
        struct fz_tcp_channel_s *c_ptr = (struct fz_tcp_channel_s *)channel->channel_desc;
        if (NULL != c_ptr){
            pthread_cond_destroy(&(c_ptr->done_cv));
            pthread_mutex_destroy(&(c_ptr->mtx));
            if (-1 != c_ptr->socket_d) close(c_ptr->socket_d);
            if (-1 != c_ptr->client_d) close(c_ptr->client_d);
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


extern int fz_parse_config_file(fz_config_t *config,  const char *config_file_path){
    int result = 1;
    FILE *fd = NULL;
    size_t file_size = 0;
    struct stat file_meta;
    char *buffer = NULL;

    fd = fopen(config_file_path, "r");
    if (NULL == fd){
        fz_log(FZ_ERROR, "Unable to open file `%s`", config_file_path);
        RETURN_DEFER(0);
    }

    if (0 == stat(config_file_path, &file_meta)) file_size = file_meta.st_size;
    else {
        fz_log(FZ_ERROR, "Issue encountered while reading file `%s`", config_file_path);
        RETURN_DEFER(0);
    }
    if (0 == file_size){
        fz_log(FZ_ERROR, "Config file should not be empty `%s`", config_file_path);
        RETURN_DEFER(0);
    }

    buffer = calloc(file_size + 1, sizeof(*buffer));
    if (NULL == buffer) {
        fz_log(FZ_ERROR, "Out of memory error in %s", __func__);
        RETURN_DEFER(0);
    }

    fread(buffer, 1, file_size, fd);
    if (!fz_deserialize_config(buffer, config)){
        fz_log(FZ_ERROR, "Issue occurred while attempting to deserialze config file");
        RETURN_DEFER(0);
    }
 
    defer:
        if (NULL != fd) fclose(fd);
        if (NULL != buffer) free(buffer);
        return result;
}


static inline int fz_deserialize_config(const char *json, fz_config_t *config){
    int result = 1;
    struct json_value_s* root = NULL;
    struct json_object_s* config_json = NULL;

    fz_log(FZ_INFO, "Attempting to deserialize config file");

    root = json_parse(json, strlen(json));
    if (!root) RETURN_DEFER(0);

    int strategy = 0;
    char *metadata_loc = NULL;
    char *target_dir = NULL;
    char *database_path = NULL;
    size_t workers = 0;
    size_t chunk_size = 0;
    size_t prefetch_size = 0;
    size_t channel = FZ_FIFO;
    config_json = (struct json_object_s*)root->payload;
    if (!config_json || !config_json->length) RETURN_DEFER(0);
    struct json_object_element_s *elem = NULL;
    for (size_t i = 0; i < config_json->length; i++){
        elem = (0 == i)? config_json->start : elem->next;
        if (NULL == elem) RETURN_DEFER(0);
        if (0 == strcmp(elem->name->string, "strategy")){
            struct json_number_s *val = (struct json_number_s *)elem->value->payload;
            strategy = (int)atoi(val->number);
        } else if (0 == strcmp(elem->name->string, "metadata_loc")){
            struct json_string_s *val = (struct json_string_s *)elem->value->payload;
            metadata_loc = calloc(val->string_size + 1, sizeof(char));
            if (NULL == metadata_loc) RETURN_DEFER(0);
            memcpy(metadata_loc, val->string, val->string_size);
        } else if (0 == strcmp(elem->name->string, "target_dir")){
            struct json_string_s *val = (struct json_string_s *)elem->value->payload;
            target_dir = calloc(val->string_size + 1, sizeof(char));
            if (NULL == target_dir) RETURN_DEFER(0);
            memcpy(target_dir, val->string, val->string_size);
        } else if (0 == strcmp(elem->name->string, "database_path")){
            struct json_string_s *val = (struct json_string_s *)elem->value->payload;
            database_path = calloc(val->string_size + 1, sizeof(char));
            if (NULL == database_path) RETURN_DEFER(0);
            memcpy(database_path, val->string, val->string_size);
        } else if (0 == strcmp(elem->name->string, "workers")){
            struct json_number_s *val = (struct json_number_s *)elem->value->payload;
            workers = (size_t)strtoul(val->number, NULL, 10);
        } else if (0 == strcmp(elem->name->string, "chunk_size")){
            struct json_number_s *val = (struct json_number_s *)elem->value->payload;
            chunk_size = (size_t)strtoul(val->number, NULL, 10);
        } else if (0 == strcmp(elem->name->string, "prefetch_size")){
            struct json_number_s *val = (struct json_number_s *)elem->value->payload;
            prefetch_size = (size_t)strtoul(val->number, NULL, 10);
        } else if (0 == strcmp(elem->name->string, "channel")){
            struct json_string_s *val = (struct json_string_s *)elem->value->payload;
            if (0 == strcmp(val->string, "tcp_socket")) channel = FZ_TCP_SOCKET;
            else if (0 == strcmp(val->string, "fifo")) channel = FZ_FIFO;
        }
    }
    config->strategy = strategy;
    config->metadata_loc = metadata_loc;
    config->target_dir = target_dir;
    config->database_path = database_path;
    config->workers = workers;
    config->chunk_size = chunk_size;
    config->prefetch_size = prefetch_size;
    config->channel = channel;

    defer:
        if (NULL != root) free(root);
        if (!result){
            if (NULL != metadata_loc) {free(metadata_loc); metadata_loc = NULL;}
            if (NULL != target_dir) {free(target_dir); target_dir = NULL;}
            if (NULL != database_path) {free(database_path); database_path = NULL;}
        }
        return result;
}


extern void fz_config_file_destroy(fz_config_t *config){
    if (NULL != config->metadata_loc) {free(config->metadata_loc); config->metadata_loc = NULL;}
    if (NULL != config->target_dir) {free(config->target_dir); config->target_dir = NULL;}
    if (NULL != config->database_path) {free(config->database_path); config->database_path = NULL;}
    config->strategy = 0;
    config->workers = 0;
    config->chunk_size = 0;
    config->prefetch_size = 0;
    config->channel = 0;
}