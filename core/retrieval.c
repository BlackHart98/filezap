#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sqlite3.h>

#include "core.h"


static int fetch_chunk_from_source(fz_ctx_t *ctx, fz_hex_digest_t chnk_checksum, size_t chunk_index, fz_dyn_queue_t *download_queue);
static int fetch_chunk_from_blob_store(fz_ctx_t *ctx, fz_hex_digest_t chnk_checksum, char *scratchpad, size_t scratchpad_size);

/* Single threaded download */
static int download_chunks_st(context_t *context, fz_ctx_t *ctx, fz_dyn_queue_t *download_queue, fz_channel_t *channel, fz_file_manifest_t *mnfst);


/* The file retrieval step is a all-or-nothing step i.e., for all the file to be successfully retrieved all the chunks that make up the file must exist */ 
extern int fz_retrieve_file(context_t *context, fz_ctx_t *ctx, fz_file_manifest_t *mnfst, fz_channel_t *channel, char *file_name){
    int result = 1;
    FILE *fh = NULL;
    FILE *dest_fh = NULL;
    fz_dyn_queue_t dq = {0};
    struct cutpoint_map_s *cutpoint_map = NULL;
    struct missing_chunks_map_s *missing_chunks = NULL;

    size_t temp_file_path_len = strlen(ctx->target_dir) + strlen("filezap__") + HEX_DIGIT_SIZE;
    slice_t temp_file_path = arena_allocator_alloc(&(context->temp_allocator), char, temp_file_path_len + 1);
    if (NULL == temp_file_path.ptr) {
        fz_log(FZ_ERROR, "Out of memory error in %s", __func__);
        RETURN_DEFER(0);
    }
    snprintf(temp_file_path.ptr, temp_file_path_len + 1, "%sfilezap__%16llx", ctx->target_dir, mnfst->file_checksum);

    hmdefault(missing_chunks, 1);
    for (size_t i = 0; i < mnfst->chunk_seq.chunk_seq_len; i++){
        hmput(missing_chunks, mnfst->chunk_seq.chunk_checksum[i], 1);
    }
    fz_log(FZ_INFO, "missing_chunks after hmput loop: %p", (void *)missing_chunks);

    if (!fz_dyn_queue_init(&dq, RESERVED)){
        fz_log(FZ_ERROR, "Out of memory ah error!");
        RETURN_DEFER(0);
    }
    
    if (!fz_fetch_file_st(context, ctx, mnfst, channel, &dq, &cutpoint_map, missing_chunks, file_name)){
        fz_log(FZ_ERROR, "Something went wrong trying to scavenge for chunks");
        RETURN_DEFER(0);
    }
    fz_log(FZ_INFO, "File from cutpoint successful");

    if (!download_chunks_st(context, ctx, &dq, channel, mnfst)){
        fz_log(FZ_ERROR, "Something went wrong while trying to download missing chunk");
        RETURN_DEFER(0);
    }

    dest_fh = fopen(temp_file_path.ptr, "w+b");
    if (NULL == dest_fh) {
        fz_log(FZ_INFO, "Destination handle failed");
        RETURN_DEFER(0);
    }

    /* Pre-allocate buffer at max chunk size to avoid per-iteration resize */
    size_t max_chunk_size = 0;
    for (size_t i = 0; i < mnfst->chunk_seq.chunk_seq_len; i++){
        if (max_chunk_size < mnfst->chunk_seq.chunk_size[i])
            max_chunk_size = mnfst->chunk_seq.chunk_size[i];
    }
    slice_t buffer = arena_allocator_alloc(&(context->temp_allocator), char, max_chunk_size);
    if (NULL == buffer.ptr) {
        fz_log(FZ_ERROR, "Out of memory error in %s", __func__);
        RETURN_DEFER(0);
    }
    char receiver_chnk_loc[RESERVED];

    for (size_t i = 0; i < mnfst->chunk_seq.chunk_seq_len; i++){
        memset(buffer.ptr, 0, buffer.len_in_bytes);
        size_t remaining = mnfst->file_size - mnfst->chunk_seq.cutpoint[i];
        size_t min = (remaining < mnfst->chunk_seq.chunk_size[i])? remaining : mnfst->chunk_seq.chunk_size[i];

        snprintf(receiver_chnk_loc, RESERVED, "%s%016llx", ctx->metadata_loc, mnfst->chunk_seq.chunk_checksum[i]);

        fh = fopen(receiver_chnk_loc, "rb");
        if (NULL == fh) RETURN_DEFER(0);
        fread(buffer.ptr, 1, min, fh);
        fwrite(buffer.ptr, 1, min, dest_fh);
        fclose(fh); fh = NULL;
    }

    fz_hex_digest_t digest = 0;
    xxhash_hexdigest_from_file(dest_fh, &digest);
    fz_log(FZ_INFO, "Calculating the file checksum");
    if (mnfst->file_checksum != digest) {
        fz_log(FZ_INFO, "Retrieval error, corrupted file");
        RETURN_DEFER(0);
    }
    if (0 != rename(temp_file_path.ptr, file_name)) {
        fz_log(FZ_ERROR, "Failed to rename file to %s", file_name);
        RETURN_DEFER(0);
    }

    size_t count = 0;
    for (size_t i = 0; i < hmlenu(missing_chunks); i++){
        fz_hex_digest_t key = missing_chunks[i].key;
        if (1 == hmget(missing_chunks, key)) count++;
    }
    fz_log(FZ_INFO, "Here are the missing chunks size(%lu): ", count);
    fz_log(FZ_INFO, "Size of missing_chunks_map_s (%lu): ", sizeof(struct missing_chunks_map_s));

    defer:
        if (NULL != fh)      fclose(fh);
        if (NULL != dest_fh) fclose(dest_fh);
        if (NULL != missing_chunks) hmfree(missing_chunks);
        if (NULL != cutpoint_map) shfree(cutpoint_map);
        fz_dyn_queue_destroy(&dq);
        return result;
}

extern int fz_fetch_file_st(
    context_t *context,
    fz_ctx_t *ctx,
    fz_file_manifest_t *mnfst,
    fz_channel_t *channel,
    fz_dyn_queue_t *download_queue,
    struct cutpoint_map_s **cutpoint_map,
    struct missing_chunks_map_s *missing_chunks,
    char *dest_file_path
){
    (void)channel;
    int result = 1;
    fz_chunk_t *chunk_list = NULL;
    size_t chunk_size = 0;
    
    slice_t scratchpad = arena_allocator_alloc(&(context->temp_allocator), char, RESERVED);
    if (NULL == scratchpad.ptr) RETURN_DEFER(0);

    slice_t chunk_seq = arena_allocator_alloc(&(context->temp_allocator), fz_chunk_seq_t, mnfst->chunk_seq.chunk_seq_len);
    if (NULL == chunk_seq.ptr) RETURN_DEFER(0);

    for (size_t i = 0; i < mnfst->chunk_seq.chunk_seq_len; i++){
        if (fetch_chunk_from_blob_store(ctx, mnfst->chunk_seq.chunk_checksum[i], scratchpad.ptr, scratchpad.len_in_bytes)){
            hmput(missing_chunks, mnfst->chunk_seq.chunk_checksum[i], 0);
        }
    }

    if (fz_query_required_chunk_list(context, ctx, mnfst, &chunk_list, &chunk_size, missing_chunks)){
        if (!fz_fetch_chunks_from_file_cutpoint(context, ctx, mnfst, chunk_list, chunk_size, cutpoint_map, missing_chunks, dest_file_path)){
            fz_log(FZ_ERROR, "Error occurred while trying to fetch chunk from file(s)");
        }
    } else fz_log(FZ_ERROR, "Error occurred while querying for necessary chunk(s) from chunk table");

    for (size_t i = 0; i < mnfst->chunk_seq.chunk_seq_len; i++){
        if (0 == hmget(missing_chunks, mnfst->chunk_seq.chunk_checksum[i])) continue;
        else if (!fetch_chunk_from_source(ctx, mnfst->chunk_seq.chunk_checksum[i], i, download_queue)) RETURN_DEFER(0);
    }
    defer:
        return result;
}

extern int fz_fetch_chunks_from_file_cutpoint(
    context_t *context,
    fz_ctx_t *ctx, 
    fz_file_manifest_t *mnfst, 
    fz_chunk_t *chunk_buffer, 
    size_t nchunk, 
    struct cutpoint_map_s **cutpoint_map,
    struct missing_chunks_map_s *missing_chunks,
    char *dest_file_path
){
    int result = 1;
    string_t chunk_loc_buffer = string_lib_init_capacity(&(context->temp_allocator), strlen(ctx->metadata_loc) + HEX_DIGIT_SIZE);
    /* This is wasteful, use a resizable arena allocator; create a map from file to the chunk cutpoint */
    for (size_t i = 0; i < nchunk; i++){
        fz_cutpoint_list_t *val_buffer = (fz_cutpoint_list_t *)shget(*cutpoint_map, chunk_buffer[i].src_file_path);
        if (NULL == val_buffer) {
            val_buffer = (fz_cutpoint_list_t *)arena_allocator_alloc_item(&(context->temp_allocator), fz_cutpoint_list_t);
            if (NULL == val_buffer) RETURN_DEFER(0);
            val_buffer->cutpoint_len = 0;
            slice_t buffer_slice = arena_allocator_alloc(&(context->temp_allocator), fz_hex_digest_t, mnfst->chunk_seq.chunk_seq_len);
            slice_t chunk_size_slice = arena_allocator_alloc(&(context->temp_allocator), size_t, mnfst->chunk_seq.chunk_seq_len);
            slice_t cutpoint_slice = arena_allocator_alloc(&(context->temp_allocator), size_t, mnfst->chunk_seq.chunk_seq_len);
            if (NULL == buffer_slice.ptr || NULL == chunk_size_slice.ptr || NULL == cutpoint_slice.ptr) RETURN_DEFER(0);

            val_buffer->buffer = buffer_slice.ptr;
            val_buffer->chunk_size = chunk_size_slice.ptr;
            val_buffer->cutpoint = cutpoint_slice.ptr;
        }
        if (val_buffer->cutpoint_len >= mnfst->chunk_seq.chunk_seq_len) continue;
        val_buffer->buffer[val_buffer->cutpoint_len] = chunk_buffer[i].chunk_checksum;
        val_buffer->chunk_size[val_buffer->cutpoint_len] = chunk_buffer[i].chunk_size;
        val_buffer->cutpoint[val_buffer->cutpoint_len] = chunk_buffer[i].cutpoint;
        hmput(missing_chunks, chunk_buffer[i].chunk_checksum, 1);
        val_buffer->cutpoint_len++;

        shput(*cutpoint_map, chunk_buffer[i].src_file_path, val_buffer);
    }

    size_t max_alloc = 0;
    fz_hex_digest_t digest = 0;
    char temp[HEX_DIGIT_SIZE] = {0};

    int ret = string_lib_append_strlit(&(context->temp_allocator), &chunk_loc_buffer, ctx->metadata_loc);
    if (0 != ret) RETURN_DEFER(0);

    // Precompute buffer allocation
    for (size_t i = 0; i < shlenu(*cutpoint_map); i++){
        fz_cutpoint_list_t *val_buffer = (*cutpoint_map)[i].value;
        for (size_t j = 0; j < val_buffer->cutpoint_len; j++){;
            if (max_alloc < val_buffer->chunk_size[j]) max_alloc = val_buffer->chunk_size[j];
        }
    }
    slice_t buffer = arena_allocator_alloc(&(context->temp_allocator), char, max_alloc);
    if (NULL == buffer.ptr) RETURN_DEFER(0);

    slice_t temp_loc_slice = arena_allocator_alloc(&(context->temp_allocator), char, strlen(ctx->metadata_loc) + HEX_DIGIT_SIZE + 1);
    if (NULL == temp_loc_slice.ptr) RETURN_DEFER(0);

    for (size_t i = 0; i < shlenu(*cutpoint_map); i++){
        char *scvg_file_path = (*cutpoint_map)[i].key;
        fz_cutpoint_list_t *val_buffer = (*cutpoint_map)[i].value;
        FILE *fh = fopen(scvg_file_path, "rb");
        if (NULL == fh) continue; /* If it fails to open the file move to next file */
        for (size_t j = 0; j < val_buffer->cutpoint_len; j++){
            if (fseek(fh, val_buffer->cutpoint[j], SEEK_SET) < 0) RETURN_DEFER(0);

            memset(buffer.ptr, 0, max_alloc);
            fread(buffer.ptr, 1, val_buffer->chunk_size[j], fh);
            size_t min = val_buffer->chunk_size[j];
            xxhash_hexdigest(buffer.ptr, val_buffer->chunk_size[j], &digest);
            if (digest == val_buffer->buffer[j]){
                snprintf(temp, HEX_DIGIT_SIZE, "%016llx", digest);
                int ret = string_lib_append_strlit(&(context->temp_allocator), &chunk_loc_buffer, temp);
                if (0 != ret) RETURN_DEFER(0);
                slice_t loc_slice = string_lib_cstring_in_slice(&chunk_loc_buffer, &temp_loc_slice);
                FILE *d_fh = fopen(loc_slice.ptr, "wb");
                if (NULL == d_fh) RETURN_DEFER(0);
                fwrite(buffer.ptr, 1, min, d_fh);
                fclose(d_fh);
                hmput(missing_chunks, digest, 0);
                chunk_loc_buffer = string_lib_shrink_len(&chunk_loc_buffer, HEX_DIGIT_SIZE);
            }
        }
        snprintf(temp, HEX_DIGIT_SIZE, "%016llx", digest);
        fclose(fh);
    }
    defer:
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


extern int fz_serialize_response(context_t *context, fz_chunk_response_t *response, string_t *json_str){
    int result = 1;
    char temp[XSMALL_RESERVED] = {0};

    snprintf(temp, XSMALL_RESERVED, "{\"chunk_checksum\":\"%016llx\",\"chunk_index\":%lu}", response->checksum, response->chunk_index);
    int ret = string_lib_append_strlit(&(context->temp_allocator), json_str, temp);
    if (0 != ret) RETURN_DEFER(0);
    defer:
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
        if (NULL != root) free(root);
        return result;
}


static int download_chunks_st(context_t *context, fz_ctx_t *ctx, fz_dyn_queue_t *download_queue, fz_channel_t *channel, fz_file_manifest_t *mnfst){
    int result = 1;
    char number_as_str[XXSMALL_RESERVED] = {0};
    FILE *chnk_fh = NULL;
    size_t chunk_max_alloc = 0;
    size_t count = 0;

    slice_t scratchpad = arena_allocator_alloc(&(context->temp_allocator), char, LARGE_RESERVED);
    if (NULL == scratchpad.ptr) {
        fz_log(FZ_ERROR, "Out of memory error in %s", __func__);
        RETURN_DEFER(0);
    }

    fz_log(FZ_INFO, "chunk max is: %zu, queue_size: %zu!", chunk_max_alloc, download_queue->rear);

    fz_chunk_response_t *res = (fz_chunk_response_t *)download_queue->buffer;
    for (size_t i = 0; i < download_queue->rear; i++){
        if (chunk_max_alloc < mnfst->chunk_seq.chunk_size[res[i].chunk_index]) 
            chunk_max_alloc = mnfst->chunk_seq.chunk_size[res[i].chunk_index];
    }
    fz_log(FZ_INFO, "chunk max is: %zu?", chunk_max_alloc);
    slice_t content_buffer = arena_allocator_alloc(&(context->temp_allocator), char, chunk_max_alloc);
    if (NULL == content_buffer.ptr) {
        fz_log(FZ_ERROR, "Out of memory error in %s", __func__);
        RETURN_DEFER(0);
    }
    string_t json_str = string_lib_init_capacity(&(context->temp_allocator), XSMALL_RESERVED);
    slice_t str_slice = arena_allocator_alloc(&(context->temp_allocator), char, XSMALL_RESERVED + 1);
    if (NULL == json_str.ptr || NULL == str_slice.ptr){
        fz_log(FZ_ERROR, "Out of memory error in %s", __func__);
         RETURN_DEFER(0);
    }
    while(!fz_dyn_queue_empty(download_queue)){
        do {
            char number_as_str[XXSMALL_RESERVED] = {0};
            snprintf(number_as_str, XXSMALL_RESERVED, "%lu", 0lu);
            if (!fz_channel_write_response(channel, number_as_str, XXSMALL_RESERVED)){
                fz_log(FZ_ERROR, "Something went wrong");
                RETURN_DEFER(0);
            }
        } while(0);
        fz_chunk_response_t val = {0};
        if (fz_dyn_dequeue(download_queue, &val)){
            memset(str_slice.ptr, 0, str_slice.len_in_bytes);
            if (!fz_serialize_response(context, &val, &json_str)){
                fz_log(FZ_ERROR, "Something wrong trying to serialize response");
                RETURN_DEFER(0);
            }
            if (0 == json_str.len) {
                fz_log(FZ_ERROR, "JSON string");
                RETURN_DEFER(0);
            }
            slice_t temp_slice = string_lib_cstring_in_slice(&json_str, &str_slice);
            snprintf(number_as_str, XXSMALL_RESERVED, "%lu", temp_slice.len_in_bytes);
            if (!fz_channel_write_response(channel, number_as_str, XXSMALL_RESERVED)){
                fz_log(FZ_ERROR, "Something wrong trying to writing the response");
                RETURN_DEFER(0);
            }
            if (!fz_channel_write_response(channel, temp_slice.ptr, strlen(temp_slice.ptr))) {
                fz_log(FZ_ERROR, "Something went wrong: %s", temp_slice.ptr);
                RETURN_DEFER(0);
            }

            size_t chunk_size = mnfst->chunk_seq.chunk_size[val.chunk_index];

            if (!fz_channel_read_request(channel, content_buffer.ptr, chunk_size, scratchpad.ptr, scratchpad.len_in_bytes)) RETURN_DEFER(0);
            char temp_loc[XXSMALL_RESERVED] = {0};
            snprintf(temp_loc, XXSMALL_RESERVED, "%s%016llx", ctx->metadata_loc, val.checksum);
            chnk_fh = fopen(temp_loc, "wb");
            if (NULL == chnk_fh) RETURN_DEFER(0);
            fwrite(content_buffer.ptr, 1, chunk_size, chnk_fh);
            fclose(chnk_fh);
            json_str = string_lib_shrink_len(&json_str, 0); //reset string
        } else {
            assert(0&&"Unreachable!");
        }
        count++;
    }
    fz_log(FZ_INFO, "Downloaded %lu missing chunk(s) from sender", count);
    defer:
        return result;
}
