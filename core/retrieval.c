#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sqlite3.h>

#include "core.h"


static inline int fetch_chunk_from_file_cutpoint(fz_ctx_t *ctx, fz_hex_digest_t chnk_checksum);
static inline int fetch_chunk_from_source(fz_ctx_desc_t ctx, fz_hex_digest_t chnk_checksum, size_t chunk_index, fz_dyn_queue_t *download_queue);
static inline int fetch_chunk_from_blob_store(fz_ctx_t *ctx, fz_hex_digest_t chnk_checksum, char *scratchpad, size_t scratchpad_size); /* LRU cache for the chunks */
static inline int get_filename(const char *file_path, char **file_name);
static inline int download_chunks(fz_ctx_t *ctx, fz_dyn_queue_t *download_queue, fz_channel_t *channel);
static void* download_chunks_(void *arg);

/* Single threaded download */
static inline int download_chunks_st(fz_ctx_t *ctx, fz_dyn_queue_t *download_queue, fz_channel_t *channel, fz_file_manifest_t *mnfst);


/* The file retrieval step is a all-or-nothing step i.e. for all the file to be successfully retrieved 
all the chunks that make up the file must exist */ 
extern int fz_retrieve_file(fz_ctx_t *ctx, fz_file_manifest_t *mnfst, fz_channel_t *channel, char *file_name){
    int result = 1;
    FILE *fh = NULL;
    char *buffer = NULL;
    FILE *dest_fh = NULL;
    fz_dyn_queue_t dq = {0};
    size_t flag = 0;
    struct cutpoint_map_s *cutpoint_map = NULL;
    struct missing_chunks_map_s *missing_chunks = NULL;

    hmdefault(missing_chunks, 1);
    for (size_t i = 0; i < mnfst->chunk_seq.chunk_seq_len; i++){
        hmput(missing_chunks, mnfst->chunk_seq.chunk_checksum[i], 1);
    }

    if(!fz_dyn_queue_init(&dq, RESERVED)){
        fz_log(FZ_ERROR, "Out of memory ah error!");
        RETURN_DEFER(0);
    }

    /* Todo: revisit this multithreaded fetch */
    if (!fz_fetch_file_st(ctx, mnfst, channel, &dq, &cutpoint_map, &missing_chunks)){
        fz_log(FZ_ERROR, "Something went wrong trying to scavenge for chunks");
        RETURN_DEFER(0);
    }

    fz_log(FZ_INFO, "File from cutpoint successful");
    /* Todo: revisit this multithreaded download */
    if (!download_chunks_st(ctx, &dq, channel, mnfst)){
        fz_log(FZ_ERROR, "Something went wrong while trying to download missing chunk");
        RETURN_DEFER(0);
    }

    dest_fh = fopen(file_name, "a+");
    if (NULL == dest_fh) {
        fz_log(FZ_INFO, "Destination handle fail");
        RETURN_DEFER(0);
    }

    // truncate file
    if (-1 == ftruncate(fileno(dest_fh), 0)) RETURN_DEFER(0);
    char receiver_chnk_loc[RESERVED];
    size_t max_alloc = 0;
    for (size_t i = 0; i < mnfst->chunk_seq.chunk_seq_len; i++){
        size_t min = (mnfst->file_size - (mnfst->chunk_seq.chunk_size[i] * i)) < mnfst->chunk_seq.chunk_size[i]? 
            (mnfst->file_size - (mnfst->chunk_seq.chunk_size[i] * i)) : mnfst->chunk_seq.chunk_size[i];
        if (max_alloc < min){
            max_alloc = min;
            buffer = realloc(buffer, max_alloc * sizeof(char));
            memset(buffer, 0, max_alloc * sizeof(char));
        }
        if (NULL == buffer) RETURN_DEFER(0);
        snprintf(receiver_chnk_loc, RESERVED, "%s%016llx", ctx->metadata_loc, mnfst->chunk_seq.chunk_checksum[i]);

        fh = fopen(receiver_chnk_loc, "r");
        if (NULL == fh) RETURN_DEFER(0);
        fread(buffer, 1, min, fh);
        fwrite(buffer, 1, min, dest_fh);
        fclose(fh);

        memset(buffer, 0, max_alloc * sizeof(char));
    }

    fz_hex_digest_t digest = 0;
    xxhash_hexdigest_from_file(dest_fh, &digest);
    fz_log(FZ_INFO, "Calculating the file checksum");
    if (mnfst->file_checksum != digest) {
        RETURN_DEFER(0);
    }

    size_t count = 0;
    for (size_t i = 0; i < hmlenu(missing_chunks); i++){
        fz_hex_digest_t key = missing_chunks[i].key;
        if (1 == hmget(missing_chunks, key)){
            // fz_log(FZ_INFO, "chunk checksum: %llu", key);
            count++;
        }
    }
    fz_log(FZ_INFO, "Here are the missing chunks size(%lu): ", count);
    defer:
        /* Notify sender that the files have been sent successfully 
        Todo: have different code to indicate the result file transfer i.e., FZ_TRANSFER_SUCCESS = 1 etc.
        This will improve visibilty of the file transfer process to the sender */
        flag = 1;
        SEND_CONN_FLAG(flag); /* Non-zero indicates close connection: This is not a very good idea */

        if (NULL != fh) fclose(fh);
        if (NULL != buffer) free(buffer);
        if (NULL != dest_fh) fclose(dest_fh);
        if (NULL != missing_chunks) hmfree(missing_chunks);
        if (NULL != cutpoint_map){
            for (size_t i = 0; i < shlenu(cutpoint_map); i++){
                fz_cutpoint_list_destroy(cutpoint_map[i].value);
            }
            shfree(cutpoint_map);
        }
        fz_dyn_enqueue_destroy(&dq);
        return result;
}


extern int fz_fetch_file_st(fz_ctx_t *ctx, fz_file_manifest_t *mnfst, fz_channel_t *channel, fz_dyn_queue_t *download_queue, struct cutpoint_map_s **cutpoint_map, struct missing_chunks_map_s **missing_chunks){
    (void)channel;
    int result = 1;
    char *scratchpad = NULL;
    size_t scratchpad_size = RESERVED;
    fz_chunk_seq_t *chunk_seq = NULL;
    size_t *missing_index = NULL;
    fz_chunk_t *chunk_list = NULL;
    size_t chunk_size = 0;

    scratchpad = calloc(scratchpad_size, sizeof(char));
    if (NULL == scratchpad) RETURN_DEFER(0);

    chunk_seq = calloc(mnfst->chunk_seq.chunk_seq_len, sizeof(fz_chunk_seq_t));
    missing_index = calloc(mnfst->chunk_seq.chunk_seq_len, sizeof(size_t));
    if (NULL == chunk_seq) RETURN_DEFER(0);
    if (NULL == missing_index) RETURN_DEFER(0);
    for (size_t i = 0; i < mnfst->chunk_seq.chunk_seq_len; i++){
        if (fetch_chunk_from_blob_store(ctx, mnfst->chunk_seq.chunk_checksum[i], scratchpad, scratchpad_size)){
            hmput(*missing_chunks, mnfst->chunk_seq.chunk_checksum[i], 0);
        }
    }
    if (!fz_query_required_chunk_list(ctx, mnfst, &chunk_list, &chunk_size, missing_chunks)) RETURN_DEFER(0);
    if (!fz_fetch_chunks_from_file_cutpoint(ctx, mnfst, chunk_list, chunk_size, cutpoint_map, missing_chunks)) RETURN_DEFER(0);
    for (size_t i = 0; i < mnfst->chunk_seq.chunk_seq_len; i++){
        if (0 == hmget(*missing_chunks, mnfst->chunk_seq.chunk_checksum[i])) continue;
        else if (!fetch_chunk_from_source((fz_ctx_desc_t)ctx, mnfst->chunk_seq.chunk_checksum[i], i, download_queue)) RETURN_DEFER(0);
    }
    defer:
        if (NULL != scratchpad) free(scratchpad);
        if (NULL != chunk_seq) free(chunk_seq);
        if (NULL != missing_index) free(missing_index);
        if (NULL != chunk_list) {
            for(size_t i = 0; i < chunk_size; i++){
                if (NULL != chunk_list[i].src_file_path) free((char *)chunk_list[i].src_file_path);
            }
            free(chunk_list);
        }
        return result;
}


extern int fz_fetch_file(fz_ctx_t *ctx, fz_file_manifest_t *mnfst, fz_channel_t *channel, fz_dyn_queue_t *download_queue){
    pthread_mutex_t t_mtx;
    pthread_cond_t is_done;
    int failed = 0;
    size_t failed_tasks = 0;
    ssize_t remaining_tasks = mnfst->chunk_seq.chunk_seq_len;
    int result = 1;
    pthread_t *workers = NULL;
    char *scratchpad = NULL;

    pthread_mutex_init(&t_mtx, NULL);
    pthread_cond_init(&is_done, NULL);
    workers = malloc(sizeof(pthread_t) * ctx->max_threads);
    if(NULL == workers) RETURN_DEFER(0);
    scratchpad = calloc(RESERVED, sizeof(char));
    if(NULL == scratchpad) RETURN_DEFER(0);
    for (size_t i = 0; i < ctx->max_threads; i++){
        struct thread_arg t_arg = {
            .wq = &(ctx->wq), 
            .done_cv = &is_done, 
            .mtx = &t_mtx, 
            .failed_tasks = &failed_tasks, 
            .remaining_tasks = &remaining_tasks,
            .download_queue = download_queue};
        pthread_create(&workers[i], NULL, fz_fetch_chunk, (void *)&t_arg);
    }
    // fz_log(FZ_INFO, "Chunk count: %lu", mnfst->chunk_seq.chunk_seq_len);
    for (size_t i = 0; i < mnfst->chunk_seq.chunk_seq_len; i++){
        if (!fetch_chunk_from_blob_store(ctx, mnfst->chunk_seq.chunk_checksum[i], scratchpad, RESERVED)){
            // fz_log(FZ_ERROR, "Cannot find chunk `%016llx` in blob store", mnfst->chunk_seq.chunk_checksum[i]);
            // fz_log(FZ_INFO, "Scavenging for the chunk with the checksum `%016llx`", mnfst->chunk_seq.chunk_checksum[i]);
            pthread_mutex_lock(&t_mtx);
            if (0 < failed_tasks) failed = 1;
            pthread_mutex_unlock(&t_mtx);
            if (failed) {
                fz_log(FZ_ERROR, "Error occurred while scavenging for chunks"); break;
            }
            fz_enqueue(&(ctx->wq), (fz_chunk_request_t){
                .dest_id = (uintptr_t)ctx,
                .chunk_meta = (fz_chunk_t){
                    .chunk_checksum = mnfst->chunk_seq.chunk_checksum[i],
                    .chunk_size = mnfst->chunk_seq.chunk_size[i],
                    .cutpoint = mnfst->chunk_seq.cutpoint[i],
                    .chunk_index = i,
                }});            
        } else {
            pthread_mutex_lock(&t_mtx);
            remaining_tasks -= 1;
            pthread_mutex_unlock(&t_mtx);
        }
    }
    pthread_mutex_lock(&t_mtx);
    while(0 < remaining_tasks && 0 >= failed_tasks){
        pthread_cond_wait(&is_done, &t_mtx);
    }
    pthread_mutex_unlock(&t_mtx);

    for (size_t i = 0; i < ctx->max_threads; i++){
        pthread_join(workers[i], NULL);
    }

    if (0 < failed_tasks) RETURN_DEFER(0);
    defer:
        pthread_mutex_destroy(&t_mtx);
        pthread_cond_destroy(&is_done);
        if (NULL != workers) free(workers);
        if (NULL != scratchpad) free(scratchpad);
        return result;
}


extern void* fz_fetch_chunk(void *arg){
    struct thread_arg t_arg = *(struct thread_arg *) arg;
    fz_ringbuffer_t *t_cntl = (fz_ringbuffer_t *)t_arg.wq;
    while(1){
        fz_chunk_request_t val;
        pthread_mutex_lock(t_arg.mtx);
        size_t failed_tasks = *(t_arg.failed_tasks);
        ssize_t remaining_tasks = *(t_arg.remaining_tasks);
        pthread_mutex_unlock(t_arg.mtx);
        /* If any threads fails all other threads should fail */
        if (0 < failed_tasks || 0 >= remaining_tasks) return NULL;
        if (fz_dequeue(t_cntl, &val)){
            if(!fetch_chunk_from_source(
                (fz_ctx_desc_t)val.dest_id, 
                val.chunk_meta.chunk_checksum, 
                val.chunk_meta.chunk_index, 
                t_arg.download_queue)
            ){
                // fz_log(FZ_ERROR, "Could not download chunk `%016llx` from source", val.chunk_meta.chunk_checksum);
                /* Potentially try again... */
                pthread_mutex_lock(t_arg.mtx);
                *(t_arg.failed_tasks) += 1;
                pthread_cond_signal(t_arg.done_cv);
                pthread_mutex_unlock(t_arg.mtx);
            } else {
                /* Signal to the caller thread that this task has been completed */ 
                pthread_mutex_lock(t_arg.mtx);
                *(t_arg.remaining_tasks) -= 1;
                pthread_cond_signal(t_arg.done_cv);
                pthread_mutex_unlock(t_arg.mtx);
                // fz_log(FZ_INFO, "Finished download for `%016llx`", val.chunk_meta.chunk_checksum);
            }
        }
    }
    return NULL;
}


extern int fz_fetch_chunks_from_file_cutpoint(
    fz_ctx_t *ctx, 
    fz_file_manifest_t *mnfst, 
    fz_chunk_t *chunk_buffer, 
    size_t nchunk, 
    struct cutpoint_map_s **cutpoint_map,
    struct missing_chunks_map_s **missing_chunks
){
    int result = 1;
    char *buffer = NULL;
    char *chunk_loc_buffer = NULL;
    /* This is wasteful, use a resizable arena allocator */
    shdefault(*cutpoint_map, NULL);
    for (size_t i = 0; i < nchunk; i++){
        fz_cutpoint_list_t *val_buffer =(fz_cutpoint_list_t *)shget(*cutpoint_map, chunk_buffer[i].src_file_path);
        if (NULL == val_buffer) {
            val_buffer = calloc(1, sizeof(fz_cutpoint_list_t));
            if (NULL == val_buffer) RETURN_DEFER(0);
            val_buffer->buffer = calloc(mnfst->chunk_seq.chunk_seq_len, sizeof(fz_hex_digest_t));
            val_buffer->chunk_size = calloc(mnfst->chunk_seq.chunk_seq_len, sizeof(size_t));
            val_buffer->cutpoint = calloc(mnfst->chunk_seq.chunk_seq_len, sizeof(size_t));
            if (NULL == val_buffer->buffer || NULL == val_buffer->chunk_size || NULL == val_buffer->cutpoint) {
                free(val_buffer); RETURN_DEFER(0);
            }
        }
        val_buffer->buffer[val_buffer->cutpoint_len] = chunk_buffer[i].chunk_checksum;
        val_buffer->chunk_size[val_buffer->cutpoint_len] = chunk_buffer[i].chunk_size;
        val_buffer->cutpoint[val_buffer->cutpoint_len] = chunk_buffer[i].cutpoint;
        hmput(*missing_chunks, chunk_buffer[i].chunk_checksum, 0);
        val_buffer->cutpoint_len++;
        shput(*cutpoint_map, chunk_buffer[i].src_file_path, val_buffer);
    }

    size_t max_alloc = 0;
    fz_hex_digest_t digest = 0;
    char temp[17] = {0};

    chunk_loc_buffer = (char *)calloc(strlen(ctx->metadata_loc) + 17, sizeof(char));
    if (NULL == chunk_loc_buffer) RETURN_DEFER(0);
    strncat(chunk_loc_buffer, ctx->metadata_loc, sizeof(ctx->metadata_loc));

    for (size_t i = 0; i < shlenu(*cutpoint_map); i++){
        char *scvg_file_path = (*cutpoint_map)[i].key;
        fz_cutpoint_list_t *val_buffer = (*cutpoint_map)[i].value;
        // fz_log(FZ_INFO, "Open file `%s`", scvg_file_path);
        FILE *fh = fopen(scvg_file_path, "rb");
        if (NULL == fh) RETURN_DEFER(0);
        for (size_t j = 0; j < val_buffer->cutpoint_len; j++){
            if (fseek(fh, val_buffer->cutpoint[j], SEEK_SET) < 0) RETURN_DEFER(0);
            if (max_alloc < val_buffer->chunk_size[j]){
                buffer = realloc(buffer, val_buffer->chunk_size[j]);
                if (NULL == buffer) RETURN_DEFER(0);
                max_alloc = val_buffer->chunk_size[j];
            }

            memset(buffer, 0, max_alloc);
            fread(buffer, 1, val_buffer->chunk_size[j], fh);
            size_t min = val_buffer->chunk_size[j];
            xxhash_hexdigest(buffer, val_buffer->chunk_size[j], &digest);
            if (digest != val_buffer->buffer[j]){
                hmput(*missing_chunks, digest, 1);
                fclose(fh); RETURN_DEFER(0);
            } else {
                snprintf(temp, 17, "%016llx", digest);
                memcpy(&chunk_loc_buffer[strlen(ctx->metadata_loc)], temp, 17);
                // fz_log(FZ_INFO, "Chunk location: %s", chunk_loc_buffer);
                FILE *d_fh = fopen(chunk_loc_buffer, "wb");
                if (NULL == d_fh) RETURN_DEFER(0);
                fwrite(buffer, 1, min, d_fh);
                fclose(d_fh);
            }
        }
        // fz_log(FZ_INFO, "Close file `%s`", scvg_file_path);
        snprintf(temp, 17, "%016llx", digest);
        fclose(fh);
    }
    defer:
        if (!result && NULL != *cutpoint_map){
            for (size_t i = 0; i < shlenu(*cutpoint_map); i++){
                fz_cutpoint_list_destroy((*cutpoint_map)[i].value);
            }
            shfree(*cutpoint_map); *cutpoint_map = NULL;
        }
        if (NULL != buffer) free(buffer);
        if (NULL != chunk_loc_buffer) free(chunk_loc_buffer);
        return result;
}


/* For now this function is a stub that just copies chunks to the target directory */ 
static inline int fetch_chunk_from_source(fz_ctx_desc_t ctx_id, fz_hex_digest_t chnk_checksum, size_t chunk_index, fz_dyn_queue_t *download_queue){
    int result = 1;
    fz_ctx_t *ctx = (fz_ctx_t *)ctx_id;
    (void)ctx;

    /* Begin: This whole section is atomic */
    fz_chunk_response_t response = (fz_chunk_response_t){
        .checksum = chnk_checksum,
        .chunk_index = chunk_index,
    };
    /* Enqueue chunk downloads */
    if (!fz_dyn_enqueue(download_queue, response)) RETURN_DEFER(0);
    /* end */

    defer:
        return result;
}


static inline int fetch_chunk_from_file_cutpoint(fz_ctx_t *ctx, fz_hex_digest_t chnk_checksum){
    (void)ctx;
    (void)chnk_checksum;
    return 0;
}


static inline int fetch_chunk_from_blob_store(fz_ctx_t *ctx, fz_hex_digest_t chnk_checksum, char *scratchpad, size_t scratchpad_size){
    int result = 1;
    FILE *fh = NULL;
    char *chnk_loc = scratchpad;
    memset(chnk_loc, 0, scratchpad_size);
    if (NULL == chnk_loc) RETURN_DEFER(0);

    /* This will be replaced with a cache query */
    snprintf(chnk_loc, RESERVED, "%s%016llx", ctx->metadata_loc, chnk_checksum);

    fh = fopen(chnk_loc, "rb");
    if (NULL == fh) RETURN_DEFER(0);
    
    fz_hex_digest_t digest = 0;
    xxhash_hexdigest_from_file(fh, &digest);

    if (chnk_checksum != digest){
        fz_log(FZ_ERROR, "Corrupted chunk data, expected `%016llx` got `%016llx`", chnk_checksum, digest);
        RETURN_DEFER(0);
    }
    defer:
        if (NULL != fh) fclose(fh);
        return result;
}


/* Potential buffer overflow */
static int get_filename(const char *file_path, char **file_name){
    int result = 1;
    size_t file_path_len = strlen(file_path);
    int count = 0;
    for (int i = file_path_len; i >= 0; i--){
        if ('/' == file_path[i]) {
            break;
        } else {
            count += 1;
        }
    }

    char *buffer = calloc((size_t)count, sizeof(char));
    if (NULL == buffer) RETURN_DEFER(0);
    memcpy(buffer, (char *)(file_path + (uintptr_t)(file_path_len - count + 1)), count);
    *file_name = buffer;
    defer:
        if (!result && NULL != buffer) free(buffer);
        return result;
}


extern int fz_serialize_response(fz_chunk_response_t *response, char **json, size_t *json_size){
    int result = 1;
    char *buffer = NULL;

    buffer = calloc(XSMALL_RESERVED, sizeof(char));
    if (NULL == buffer) RETURN_DEFER(0);

    snprintf(buffer, XSMALL_RESERVED, "{\"chunk_checksum\":\"%016llx\",\"chunk_index\":%lu}", response->checksum, response->chunk_index);
    *json = buffer;
    *json_size = XSMALL_RESERVED;
    defer:
        if (!result) free(buffer);
        return result;
}


extern int fz_deserialize_response(char *json, fz_chunk_response_t *response){
    int result = 1;
    struct json_value_s* root = NULL;
    struct json_object_s* response_json = NULL;

    root = json_parse(json, strlen(json));
    if (!root) RETURN_DEFER(0);

    response_json = (struct json_object_s*)root->payload;
    if (!response_json || !response_json->length) RETURN_DEFER(0);
    struct json_object_element_s *elem = NULL;
    for (size_t i = 0; i < response_json->length; i++){
        elem = (0 == i)? response_json->start : elem->next;
        if (NULL == elem) RETURN_DEFER(0);
        if (0 == strcmp(elem->name->string, "chunk_checksum")){
            struct json_number_s *val = (struct json_number_s *)elem->value->payload;
            response->checksum = (fz_hex_digest_t)strtoull(val->number, NULL, 16);
        } else if (0 == strcmp(elem->name->string, "chunk_index")){
            struct json_number_s *val = (struct json_number_s *)elem->value->payload;
            response->chunk_index = (size_t)strtoul(val->number, NULL, 10);
        } 
    }
    defer:
        if (!root) free(root);
        return result;
}


static inline int download_chunks(fz_ctx_t *ctx, fz_dyn_queue_t *download_queue, fz_channel_t *channel){
    int result = 1;
    int failed = 0;
    pthread_mutex_t mtx;
    pthread_cond_t done_cv;
    pthread_mutex_init(&mtx, NULL);
    pthread_cond_init(&done_cv, NULL);
    pthread_t *workers = calloc(ctx->max_threads, sizeof(pthread_t));
    if (NULL == workers) RETURN_DEFER(0);
    for (size_t i = 0; i < ctx->max_threads; i++){
        pthread_create(
            &workers[i], 
            NULL, 
            download_chunks_, 
            (void *)&(struct download_thread_arg){
                .failed = &failed,
                .channel = NULL,
                .done_cv = &done_cv,
                .download_queue = download_queue,
                .mtx = &mtx});
    }
    for (size_t i = 0; i < ctx->max_threads; i++){
        pthread_join(workers[i], NULL);
    }
    if (failed) RETURN_DEFER(0);
    defer:
        if (NULL != workers) free(workers);
        pthread_cond_destroy(&done_cv);
        pthread_mutex_destroy(&mtx);
        return result;
}


static void* download_chunks_(void *arg){
    struct download_thread_arg *t_arg = (struct download_thread_arg *)arg;
    fz_dyn_queue_t *dq = (fz_dyn_queue_t *)t_arg->download_queue;
    while(1){
        pthread_mutex_lock(t_arg->mtx);
        int failed = *(t_arg->failed);
        pthread_mutex_unlock(t_arg->mtx);
        fz_chunk_response_t val = {0};
        if (fz_dyn_queue_empty(dq) || failed){
            break;
        } else {
            if (fz_dyn_dequeue(dq, &val)){
                fz_log(FZ_INFO, "Downloading the chunk...");

                fz_log(FZ_ERROR, "Could not download chunk...");
                pthread_mutex_lock(t_arg->mtx);
                *(t_arg->failed) = 1;
                pthread_mutex_unlock(t_arg->mtx);
            }
        }
    }
    return NULL;
}


static inline int download_chunks_st(fz_ctx_t *ctx, fz_dyn_queue_t *download_queue, fz_channel_t *channel, fz_file_manifest_t *mnfst){
    int result = 1;
    char *json = NULL;
    size_t content_size = 0;
    char number_as_str[XXSMALL_RESERVED] = {0};
    char *content_buffer = NULL;
    FILE *chnk_fh = NULL;
    char *scratchpad = NULL;
    size_t scratchpad_size = LARGE_RESERVED;
    size_t chunk_max_alloc = 0;
    size_t count = 0;

    scratchpad = calloc(scratchpad_size, sizeof(char));
    if (NULL == scratchpad) RETURN_DEFER(0);

    while(!fz_dyn_queue_empty(download_queue)){
        do {
            char number_as_str[XXSMALL_RESERVED] = {0};
            snprintf(number_as_str, XXSMALL_RESERVED, "%lu", 0lu);
            if (!fz_channel_write_response(channel, number_as_str, XXSMALL_RESERVED)){
                fz_log(FZ_ERROR, "Something went wrong: %s", json);
                RETURN_DEFER(0);
            }
        } while(0);
        fz_chunk_response_t val = {0};
        if (fz_dyn_dequeue(download_queue, &val)){
            if (!fz_serialize_response(&val, &json, &content_size)){
                fz_log(FZ_ERROR, "Something wrong trying to serialize response");
                RETURN_DEFER(0);
            }
            if (0 == content_size) RETURN_DEFER(0);
            snprintf(number_as_str, XXSMALL_RESERVED, "%lu", content_size);
            if (!fz_channel_write_response(channel, number_as_str, XXSMALL_RESERVED)){
                fz_log(FZ_ERROR, "Something wrong trying to writing the response");
                RETURN_DEFER(0);
            }
            if (!fz_channel_write_response(channel, json, content_size)) {
                fz_log(FZ_ERROR, "Something went wrong: %s", json);
                RETURN_DEFER(0);
            }

            size_t chunk_size = mnfst->chunk_seq.chunk_size[val.chunk_index];
            if (chunk_max_alloc < chunk_size){
                content_buffer = realloc(content_buffer, chunk_size);
                if (NULL == content_buffer) RETURN_DEFER(0);
                chunk_max_alloc = chunk_size;
            }

            if (!fz_channel_read_request(channel, content_buffer, chunk_size, scratchpad, scratchpad_size)) RETURN_DEFER(0);
            char temp_loc[XXSMALL_RESERVED] = {0};
            snprintf(temp_loc, XXSMALL_RESERVED, "%s%016llx", ctx->metadata_loc, val.checksum);
            chnk_fh = fopen(temp_loc, "wb");
            if (NULL == chnk_fh) RETURN_DEFER(0);
            fwrite(content_buffer, 1, chunk_size, chnk_fh);
            fclose(chnk_fh);
        } else {
            assert(0&&"Unreachable!");
            break;
        }
        count++;
    }
    fz_log(FZ_INFO, "Downloaded %lu missing chunk(s) from sender", count);
    defer:
        if (NULL == json) free(json);
        if (NULL != scratchpad) free(scratchpad);
        return result;
}
