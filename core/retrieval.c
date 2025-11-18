#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sqlite3.h>

#include "core.h"


// static inline int fetch_chunk_from_file_cutpoint(fz_ctx_t *ctx, fz_hex_digest_t chnk_checksum);
static inline int fetch_chunk_from_source(fz_ctx_t *ctx, fz_hex_digest_t chnk_checksum, size_t chunk_index, fz_dyn_queue_t *download_queue);
static inline int fetch_chunk_from_blob_store(fz_ctx_t *ctx, fz_hex_digest_t chnk_checksum, char *scratchpad, size_t scratchpad_size); /* LRU cache for the chunks */
// static inline int get_filename(const char *file_path, char **file_name);
// static inline int download_chunks(fz_ctx_t *ctx, fz_dyn_queue_t *download_queue, fz_channel_t *channel);
// static void* download_chunks_(void *arg);

/* Single threaded download */
static inline int download_chunks_st(fz_ctx_t *ctx, fz_dyn_queue_t *download_queue, fz_channel_t *channel, fz_file_manifest_t *mnfst);


/* The file retrieval step is a all-or-nothing step i.e., for all the file to be successfully retrieved all the chunks that make up the file must exist */ 
extern int fz_retrieve_file(fz_ctx_t *ctx, fz_file_manifest_t *mnfst, fz_channel_t *channel, char *file_name){
    int result = 1;
    FILE *fh = NULL;
    char *buffer = NULL;
    FILE *dest_fh = NULL;
    fz_dyn_queue_t dq = {0};
    struct cutpoint_map_s *cutpoint_map = NULL;
    struct missing_chunks_map_s *missing_chunks = NULL;
    char *temp_file_path = NULL;

    hmdefault(missing_chunks, 1);
    for (size_t i = 0; i < mnfst->chunk_seq.chunk_seq_len; i++){
        hmput(missing_chunks, mnfst->chunk_seq.chunk_checksum[i], 1);
    }

    if(!fz_dyn_queue_init(&dq, RESERVED)){
        fz_log(FZ_ERROR, "Out of memory ah error!");
        RETURN_DEFER(0);
    }

    /* Todo: revisit this multithreaded fetch */
    if (!fz_fetch_file_st(ctx, mnfst, channel, &dq, &cutpoint_map, &missing_chunks, file_name)){
        fz_log(FZ_ERROR, "Something went wrong trying to scavenge for chunks");
        RETURN_DEFER(0);
    }

    fz_log(FZ_INFO, "File from cutpoint successful");
    /* Todo: revisit this multithreaded download */
    if (!download_chunks_st(ctx, &dq, channel, mnfst)){
        fz_log(FZ_ERROR, "Something went wrong while trying to download missing chunk");
        RETURN_DEFER(0);
    }

    size_t temp_file_path_len = strlen(ctx->target_dir) + strlen("filezap__") + HEX_DIGIT_SIZE;
    temp_file_path = calloc(temp_file_path_len + 1, sizeof(char));
    if (NULL == temp_file_path) RETURN_DEFER(0);
    snprintf(temp_file_path, temp_file_path_len + 1, "%sfilezap__%16llx", ctx->target_dir, mnfst->file_checksum); 

    dest_fh = fopen(temp_file_path, "w+b");
    if (NULL == dest_fh) {
        fz_log(FZ_INFO, "Destination handle failed");
        RETURN_DEFER(0);
    }
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

        fh = fopen(receiver_chnk_loc, "rb");
        if (NULL == fh) RETURN_DEFER(0);
        fread(buffer, 1, min, fh);
        fwrite(buffer, 1, min, dest_fh);
        fclose(fh);
        memset(buffer, 0, max_alloc * sizeof(char));
    }

    fz_hex_digest_t digest = 0;
    xxhash_hexdigest_from_file(dest_fh, &digest); /* this guy is reading the destination file `dest_fh` */
    fz_log(FZ_INFO, "Calculating the file checksum");
    if (mnfst->file_checksum != digest) {
        fz_log(FZ_INFO, "Retrieval error, corrupted file");
        RETURN_DEFER(0);
    }
    if (0 != rename(temp_file_path, file_name)) {
        fz_log(FZ_ERROR, "Failed to rename file to %s", file_name);
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
        if (NULL != fh) fclose(fh);
        if (NULL != buffer) free(buffer);
        if (NULL != dest_fh) fclose(dest_fh);
        if (NULL != missing_chunks) hmfree(missing_chunks);
        if (NULL != cutpoint_map){shfree(cutpoint_map);}
        if (NULL != temp_file_path) free(temp_file_path);
        fz_dyn_queue_destroy(&dq);
        return result;
}


extern int fz_fetch_file_st(fz_ctx_t *ctx, fz_file_manifest_t *mnfst, fz_channel_t *channel, fz_dyn_queue_t *download_queue, struct cutpoint_map_s **cutpoint_map, struct missing_chunks_map_s **missing_chunks, char *dest_file_path){
    (void)channel;
    int result = 1;
    char *scratchpad = NULL;
    size_t scratchpad_size = RESERVED;
    fz_chunk_seq_t *chunk_seq = NULL;
    fz_chunk_t *chunk_list = NULL;
    size_t chunk_size = 0;

    scratchpad = calloc(scratchpad_size, sizeof(char));
    if (NULL == scratchpad) RETURN_DEFER(0);

    chunk_seq = calloc(mnfst->chunk_seq.chunk_seq_len, sizeof(fz_chunk_seq_t));
    if (NULL == chunk_seq) RETURN_DEFER(0);
    for (size_t i = 0; i < mnfst->chunk_seq.chunk_seq_len; i++){
        if (fetch_chunk_from_blob_store(ctx, mnfst->chunk_seq.chunk_checksum[i], scratchpad, scratchpad_size)){
            hmput(*missing_chunks, mnfst->chunk_seq.chunk_checksum[i], 0);
        }
    }
    
    if (fz_query_required_chunk_list(ctx, mnfst, &chunk_list, &chunk_size, missing_chunks)){
        if (!fz_fetch_chunks_from_file_cutpoint(ctx, mnfst, chunk_list, chunk_size, cutpoint_map, missing_chunks, dest_file_path)){
            fz_log(FZ_ERROR, "Error occurred while trying to fetch chunk from file(s)");
        }
    } else fz_log(FZ_ERROR, "Error occurred while querying for necessary chunk(s) from chunk table");

    for (size_t i = 0; i < mnfst->chunk_seq.chunk_seq_len; i++){
        if (0 == hmget(*missing_chunks, mnfst->chunk_seq.chunk_checksum[i])) continue;
        else if (!fetch_chunk_from_source(ctx, mnfst->chunk_seq.chunk_checksum[i], i, download_queue)) RETURN_DEFER(0);
    }
    defer:
        if (NULL != scratchpad) free(scratchpad);
        if (NULL != chunk_seq) {free(chunk_seq); chunk_seq = NULL;}
        if (NULL != chunk_list) {
            for(size_t i = 0; i < chunk_size; i++){
                if (NULL != chunk_list[i].src_file_path) {free((char *)chunk_list[i].src_file_path); chunk_list[i].src_file_path = NULL;}
            }
            free(chunk_list); chunk_list = NULL;
        }
        return result;
}


extern int fz_fetch_chunks_from_file_cutpoint(
    fz_ctx_t *ctx, 
    fz_file_manifest_t *mnfst, 
    fz_chunk_t *chunk_buffer, 
    size_t nchunk, 
    struct cutpoint_map_s **cutpoint_map,
    struct missing_chunks_map_s **missing_chunks,
    char *dest_file_path
){
    int result = 1;
    char *buffer = NULL;
    char *chunk_loc_buffer = NULL;
    /* This is wasteful, use a resizable arena allocator; create a map from file to the chunk cutpoint */
    for (size_t i = 0; i < nchunk; i++){
        fz_cutpoint_list_t *val_buffer = (fz_cutpoint_list_t *)shget(*cutpoint_map, chunk_buffer[i].src_file_path);
        if (NULL == val_buffer) {
            val_buffer = calloc(1, sizeof(fz_cutpoint_list_t));
            if (NULL == val_buffer) RETURN_DEFER(0);
            val_buffer->buffer = calloc(mnfst->chunk_seq.chunk_seq_len, sizeof(fz_hex_digest_t));
            val_buffer->chunk_size = calloc(mnfst->chunk_seq.chunk_seq_len, sizeof(size_t));
            val_buffer->cutpoint = calloc(mnfst->chunk_seq.chunk_seq_len, sizeof(size_t));
            if (NULL == val_buffer->buffer || NULL == val_buffer->chunk_size || NULL == val_buffer->cutpoint) {
                if (NULL != val_buffer->buffer) {free(val_buffer->buffer); val_buffer->buffer= NULL;}
                if (NULL != val_buffer->chunk_size) {free(val_buffer->chunk_size); val_buffer->chunk_size = NULL;}
                if (NULL != val_buffer->cutpoint) {free(val_buffer->cutpoint); val_buffer->cutpoint = NULL;}
                free(val_buffer); RETURN_DEFER(0);
            }
        }
        if (val_buffer->cutpoint_len >= mnfst->chunk_seq.chunk_seq_len) continue;
        val_buffer->buffer[val_buffer->cutpoint_len] = chunk_buffer[i].chunk_checksum;
        val_buffer->chunk_size[val_buffer->cutpoint_len] = chunk_buffer[i].chunk_size;
        val_buffer->cutpoint[val_buffer->cutpoint_len] = chunk_buffer[i].cutpoint;
        hmput(*missing_chunks, chunk_buffer[i].chunk_checksum, 1);
        val_buffer->cutpoint_len++;

        shput(*cutpoint_map, chunk_buffer[i].src_file_path, val_buffer);
    }

    size_t max_alloc = 0;
    fz_hex_digest_t digest = 0;
    char temp[HEX_DIGIT_SIZE] = {0};

    chunk_loc_buffer = (char *)calloc(strlen(ctx->metadata_loc) + HEX_DIGIT_SIZE, sizeof(char));
    if (NULL == chunk_loc_buffer) RETURN_DEFER(0);
    strncat(chunk_loc_buffer, ctx->metadata_loc, sizeof(ctx->metadata_loc));

    for (size_t i = 0; i < shlenu(*cutpoint_map); i++){
        char *scvg_file_path = (*cutpoint_map)[i].key;
        fz_cutpoint_list_t *val_buffer = (*cutpoint_map)[i].value;
        // fz_log(FZ_INFO, "Open file `%s`", scvg_file_path);
        FILE *fh = fopen(scvg_file_path, "rb");
        if (NULL == fh) continue; /* If it fails to open the file move to next file */
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
            if (digest == val_buffer->buffer[j]){
                snprintf(temp, 17, "%016llx", digest);
                memcpy(&chunk_loc_buffer[strlen(ctx->metadata_loc)], temp, 17);
                // fz_log(FZ_INFO, "Chunk location: %s", chunk_loc_buffer);
                FILE *d_fh = fopen(chunk_loc_buffer, "wb");
                if (NULL == d_fh) RETURN_DEFER(0);
                fwrite(buffer, 1, min, d_fh);
                fclose(d_fh);
                hmput(*missing_chunks, digest, 0);
            }
        }
        // fz_log(FZ_INFO, "Close file `%s`", scvg_file_path);
        snprintf(temp, HEX_DIGIT_SIZE, "%016llx", digest);
        fclose(fh);
    }
    defer:
        if (NULL != buffer) free(buffer);
        if (NULL != chunk_loc_buffer) free(chunk_loc_buffer);
        return result;
}


/* For now this function is a stub that just copies chunks to the target directory */ 
static inline int fetch_chunk_from_source(fz_ctx_t *ctx, fz_hex_digest_t chnk_checksum, size_t chunk_index, fz_dyn_queue_t *download_queue){
    int result = 1;
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
        }
        count++;
    }
    fz_log(FZ_INFO, "Downloaded %lu missing chunk(s) from sender", count);
    defer:
        if (NULL == json) free(json);
        if (NULL != scratchpad) free(scratchpad);
        return result;
}
