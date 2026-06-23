#include <stdlib.h>
#include "core.h"
#include "json.h"


static inline int get_filename(const char *file_path, char **file_name);
static int get_filename_v2(arena_allocator_t *allocator, const char *file_path, char **file_name);

/* This is better version of the original send_file, there is not physical copy deposits in the sender cache folder */
extern int fz_send_file(context_t *context, fz_ctx_t *ctx, fz_channel_t *channel, const char *src_file_path)
{
    int result = 1;
    fz_file_manifest_t mnfst = {0};
    size_t chunk_max_alloc = 0;
    slice_t content_buf = {0};
    char number_as_str[XXSMALL_RESERVED] = {0};
    char flag_str[XXSMALL_RESERVED] = {0};

    /* Owned by fz_serialize_manifest (malloc internally) — cannot arena-ify
     * without changing that function's signature.                           */
    char *manifest_buf = NULL;

    /* Fixed scratch buffer — allocated once, never resized. */
    slice_t scratchpad = arena_allocator_alloc(&(context->temp_allocator), char, LARGE_RESERVED);
    if (NULL == scratchpad.ptr) {
        fz_log(FZ_ERROR, "Out of memory error in %s", __func__);
        RETURN_DEFER(0);
    }

    if (!fz_chunk_file(ctx, &mnfst, src_file_path)) {
        fz_log(FZ_ERROR, "%s: Failed to chunk file `%s`", __func__, src_file_path);
        RETURN_DEFER(0);
    }

    size_t content_size = 0;
    if (!fz_serialize_manifest(&mnfst, &manifest_buf, &content_size)) {
        fz_log(FZ_ERROR, "Failed to serialize manifest file");
        RETURN_DEFER(0);
    }
    if (0 == content_size || MAX_MANIFEST_SIZE < content_size) {
        fz_log(FZ_ERROR, "Manifest content_size violates 0 < size < MAX_MANIFEST_SIZE (64MB): %lu", content_size / (KB(1) * KB(1)));
        RETURN_DEFER(0);
    }

    snprintf(number_as_str, XXSMALL_RESERVED, "%lu", content_size);
    fz_log(FZ_INFO, "sender: content size: %lukb", content_size / 1024);
    fz_log(FZ_INFO, "Number as string: %s, Actual number: %lu", number_as_str, content_size);

    if (!fz_channel_write_request(channel, number_as_str, XXSMALL_RESERVED, scratchpad.ptr, scratchpad.len_in_bytes)) {
        fz_log(FZ_ERROR, "Failed to send content size to destination");
        RETURN_DEFER(0);
    }
    // fz_log(FZ_INFO, "Number: %s ... %zu", number_as_str, content_size);
    if (!fz_channel_write_request(channel, manifest_buf, content_size, scratchpad.ptr, scratchpad.len_in_bytes)) {
        fz_log(FZ_ERROR, "Failed to send serialized manifest to destination");
        RETURN_DEFER(0);
    }
    // fz_log(FZ_INFO, "- Number: %s ... %zu", number_as_str, content_size);
    /* content_buf starts empty; sized on first chunk response. */
    FILE *src_fh = fopen(src_file_path, "rb");
    if (NULL == src_fh) {
        fz_log(FZ_ERROR, "Failed to open source file `%s` for read", src_file_path);
        RETURN_DEFER(0);
    }

    // Check any download will be needed
    if (!fz_channel_read_response(channel, flag_str, XXSMALL_RESERVED, scratchpad.ptr, scratchpad.len_in_bytes)) {
        fz_log(FZ_ERROR, "Failed to read control flag");
        RETURN_DEFER(0);
    }
    size_t no_download_flag = strtoul(flag_str, NULL, 10);
    if (1 == no_download_flag) {
        fz_log(FZ_INFO, "No need for download");
        RETURN_DEFER(1);
    }

    /* Read chunk request size. */
    if (!fz_channel_read_response(channel, number_as_str, XXSMALL_RESERVED, scratchpad.ptr, scratchpad.len_in_bytes)) {
        RETURN_DEFER(0);
    }
    content_size = strtoul(number_as_str, NULL, 10);
    fz_log(FZ_INFO, "Content size: %zu", content_size);
    if (0 < content_size && MAX_MANIFEST_SIZE > content_size){
        // fz_log(FZ_INFO, "Got into the conditional block");
        slice_t str_slice = arena_allocator_alloc(&(context->temp_allocator), char, content_size);
        if (NULL == str_slice.ptr) {
            fz_log(FZ_ERROR, "Out of memory error in %s", __func__);
            RETURN_DEFER(0);
        }
        if (!fz_channel_read_response(channel, str_slice.ptr, str_slice.len_in_bytes, scratchpad.ptr, scratchpad.len_in_bytes)) {
            RETURN_DEFER(0);
        }
        // fz_log(FZ_INFO, "Recieved missing chunks: %s", str_slice.ptr);
        array_list_t missing_chunk_list = array_list_init_capacity(&(context->temp_allocator), fz_chunk_response_t, XSMALL_RESERVED);
        if (!fz_deserialize_response(&(context->temp_allocator), str_slice.ptr, &missing_chunk_list)) RETURN_DEFER(0);
        fz_chunk_response_t *ptr = (fz_chunk_response_t *)missing_chunk_list.ptr;

        // Precompute the largest reserve allocation
        for (size_t i = 0; i < missing_chunk_list.len; i++){
            if (chunk_max_alloc < mnfst.chunk_seq.chunk_size[i]) chunk_max_alloc = mnfst.chunk_seq.chunk_size[i];
        }
        content_buf = arena_allocator_alloc(&(context->temp_allocator), char, chunk_max_alloc);
        if (NULL == content_buf.ptr){
            fz_log(FZ_ERROR, "Out of memory error in %s", __func__);
            RETURN_DEFER(0);
        }
        for (size_t i = 0; i < missing_chunk_list.len; i++){
            size_t chunk_index = ptr[i].chunk_index;
            size_t cutpoint = mnfst.chunk_seq.cutpoint[chunk_index];
            size_t chunk_size = mnfst.chunk_seq.chunk_size[chunk_index];

            snprintf(number_as_str, XXSMALL_RESERVED, "%lu", chunk_index);
            if (!fz_channel_write_request(channel, number_as_str, XXSMALL_RESERVED, scratchpad.ptr, scratchpad.len_in_bytes)) {
                RETURN_DEFER(0);
            }
            if (fseek(src_fh, (long)cutpoint, SEEK_SET) < 0) RETURN_DEFER(0);

            size_t nread = fread(content_buf.ptr, 1, chunk_size, src_fh);   /* read exactly chunk_size, not chunk_max_alloc */
            if (nread != chunk_size) {
                fz_log(FZ_ERROR, "Short read on chunk %zu: expected %zu, got %zu", chunk_index, chunk_size, nread);
                RETURN_DEFER(0);
            }
            if (!fz_channel_write_request(channel, content_buf.ptr, chunk_size, scratchpad.ptr, scratchpad.len_in_bytes)) {
                fz_log(FZ_ERROR, "Failed to send chunk to destination");
                RETURN_DEFER(0);
            }
        }
    }
    if (MAX_MANIFEST_SIZE < content_size) {
        fz_log(FZ_ERROR, "Manifest content_size violates size < MAX_MANIFEST_SIZE (64MB): %lumb", content_size / (KB(1) * KB(1)));
        RETURN_DEFER(0);
    }
    fz_log(FZ_INFO, "Closing connection");

    defer:
        fz_log(FZ_INFO, "Closed connection");
        if (NULL != src_fh)    fclose(src_fh);
        if (NULL != manifest_buf) free(manifest_buf);       /* malloc-owned, not arena */
        arena_allocator_reset(&(context->temp_allocator));  /* clears scratchpad, response_buf, content_buf in one shot */
        fz_file_manifest_destroy(&mnfst);
        return result;
}


/*
Spawn a process for recieiving the file
- wait for the request from the sender, by pooling the fifo queue
- pop the manifest off the fifo
- try to retrieve the chunks in the manifest file:
    if the chunk is on the queue pop it and do something with it, when the total chunks need is complete generate the file and validate the file checksum the send the appropriate signal to the sender process via the fifo */
extern int fz_receive_file(context_t *context, fz_ctx_t *ctx, fz_channel_t *channel){    
    int result = 1;
    fz_file_manifest_t mnfst = {0};

    char *file_name = NULL;
    char number_as_str[XXSMALL_RESERVED] = {0};
    size_t flag = 0;

    slice_t scratchpad = arena_allocator_alloc(&(context->temp_allocator), char, LARGE_RESERVED);
    if (NULL == scratchpad.ptr) {
        fz_log(FZ_ERROR, "Out of memory error in %s", __func__);
        RETURN_DEFER(0);
    }

    if (!fz_channel_read_request(channel, number_as_str, XXSMALL_RESERVED, scratchpad.ptr, scratchpad.len_in_bytes)) RETURN_DEFER(0);
    size_t content_size = strtoul(number_as_str, NULL, 10);
    if (0 == content_size || MAX_MANIFEST_SIZE < content_size) RETURN_DEFER(0);

    fz_log(FZ_INFO, "Received manifest json content size: %zukb", content_size/1024);
    slice_t buffer = arena_allocator_alloc(&(context->temp_allocator), char, content_size);
    if (NULL == buffer.ptr) RETURN_DEFER(0);

    if (!fz_channel_read_request(channel, buffer.ptr, buffer.len_in_bytes, scratchpad.ptr, scratchpad.len_in_bytes)) RETURN_DEFER(0);
    if (!fz_deserialize_manifest(buffer.ptr, &mnfst)) RETURN_DEFER(0);
    if (!get_filename_v2(&(context->temp_allocator), mnfst.file_name, &file_name)) RETURN_DEFER(0);

    slice_t file_path_buffer = arena_allocator_alloc(&(context->temp_allocator), char, RESERVED);
    if (NULL == file_path_buffer.ptr) RETURN_DEFER(0);

    /* Todo: Use an actual string object */
    snprintf(file_path_buffer.ptr, RESERVED, "%s%s", ctx->target_dir, file_name);
    fz_log(FZ_INFO, "File path: %s", file_path_buffer);

    if (!fz_retrieve_file(context, ctx, &mnfst, channel, file_path_buffer.ptr)) RETURN_DEFER(0);
    fz_log(FZ_INFO, "Receive file name: %s", file_path_buffer.ptr);

    /* Commit new chunk metadata, for now this is just a stub, I have to move this out of here */
    if (!fz_commit_chunk_metadata(ctx, &mnfst, file_path_buffer.ptr)) RETURN_DEFER(0);
    defer:
        /* Notify sender that the files have been sent successfully 
        Todo: have different code to indicate the result file transfer i.e., FZ_TRANSFER_SUCCESS = 1 etc.
        This will improve visibilty of the file transfer process to the sender */
        fz_file_manifest_destroy(&mnfst);
        arena_allocator_reset(&(context->temp_allocator));
        return result;
}


extern int fz_serialize_manifest(fz_file_manifest_t *mnfst, char **json, size_t *json_size){
    int result = 1;
    char *chunk_seq = NULL;
    size_t alloc_size = 0;
    size_t capacity = KB(64);
    chunk_seq = (char *)calloc(capacity, sizeof(char));
    if (NULL == chunk_seq) {fz_log(FZ_ERROR, "Out of memory error in %s", __func__); RETURN_DEFER(0);}
    char temp_[KB(4)] = {0};

    fz_log(FZ_INFO, "Manifest length: %zu", mnfst->chunk_seq.chunk_seq_len);

    for (size_t i = 0; i < mnfst->chunk_seq.chunk_seq_len; i++){
        if (i == (mnfst->chunk_seq.chunk_seq_len - 1)){
            snprintf(
                temp_, 
                sizeof(temp_), 
                "{\"chunk_checksum\":\"%016llx\",\"cutpoint\":%lu,\"chunk_size\":%lu}",
                mnfst->chunk_seq.chunk_checksum[i], 
                mnfst->chunk_seq.cutpoint[i],
                mnfst->chunk_seq.chunk_size[i]);
        } else {
            snprintf(
                temp_, 
                sizeof(temp_), 
                "{\"chunk_checksum\":\"%016llx\",\"cutpoint\":%lu,\"chunk_size\":%lu},",
                mnfst->chunk_seq.chunk_checksum[i], 
                mnfst->chunk_seq.cutpoint[i],
                mnfst->chunk_seq.chunk_size[i]);
        }

        /* Todo: use an actual allocator to manage this */
        alloc_size += (strlen(temp_) + 1);
        if (alloc_size > capacity){
            size_t prev_capacity = capacity;
            capacity = alloc_size * 2;
            chunk_seq = (char *)realloc(chunk_seq, capacity);
            if (NULL == chunk_seq) {fz_log(FZ_ERROR, "Out of memory error in %s", __func__); RETURN_DEFER(0);}
            memset(chunk_seq + prev_capacity, 0, (capacity - prev_capacity));
        }
        strncat(chunk_seq, temp_, strlen(temp_));
        memset(temp_, 0, sizeof(temp_));
    }

    // fz_log(FZ_INFO, "JSON: [%s], size: %luKB", chunk_seq, capacity / 1024);
    *json_size = capacity * 2;
    *json = calloc(*json_size, sizeof(char)); /* replace this with realloc */
    if (NULL == *json) {fz_log(FZ_ERROR, "Out of memory error in %s", __func__); RETURN_DEFER(0);}

    memset(temp_, 0, sizeof(temp_));
    snprintf(
        temp_, 
        sizeof(temp_), 
        "{\"file_name\":\"%s\",\"file_checksum\":\"%016llx\",\"file_size\":%lu,\"source_id\":%lu,\"chunk_seq_len\":%lu, \"chunk_seq\":",
        mnfst->file_name, 
        mnfst->file_checksum,
        mnfst->file_size,
        mnfst->source_id,
        mnfst->chunk_seq.chunk_seq_len);
    strncat(*json, temp_, strlen(temp_));
    strncat(*json, "[", 1);
    strncat(*json, chunk_seq, strlen(chunk_seq));
    strncat(*json, "]}", 2);
    defer:
        if (NULL != chunk_seq) free(chunk_seq);
        if (!result && NULL != *json){
            free(*json); *json = NULL;
        }
        return result;
}


extern int fz_deserialize_manifest(const char *json, fz_file_manifest_t *mnfst){
    int result = 1;
    char *file_name = NULL;
    struct json_value_s* root = NULL;
    struct json_object_s* file_manifest_json = NULL;
    struct json_array_s* chunk_seq_json = NULL;

    root = json_parse(json, strlen(json));
    if (!root) RETURN_DEFER(0);

    file_manifest_json = (struct json_object_s*)root->payload;
    if (!file_manifest_json || !file_manifest_json->length) RETURN_DEFER(0);
    
    fz_hex_digest_t digest = 0;
    size_t chunk_seq_len = 0;
    size_t file_size = 0;
    fz_ctx_desc_t source_id = 0;
    struct json_object_element_s *elem = NULL;
    for (size_t i = 0; i < file_manifest_json->length; i++){
        elem = (0 == i)? file_manifest_json->start : elem->next;
        if (NULL == elem) RETURN_DEFER(0);
        if (0 == strcmp(elem->name->string, "file_name")){
            struct json_string_s *val = (struct json_string_s *)elem->value->payload;
            file_name = calloc(val->string_size + 1, sizeof(char));
            if (NULL == file_name) RETURN_DEFER(0);
            memcpy(file_name, val->string, val->string_size);
        } else if (0 == strcmp(elem->name->string, "file_checksum")){
            struct json_number_s *val = (struct json_number_s *)elem->value->payload;
            digest = (fz_hex_digest_t)strtoull(val->number, NULL, 16);
        } else if (0 == strcmp(elem->name->string, "file_size")){
            struct json_number_s *val = (struct json_number_s *)elem->value->payload;
            file_size = (size_t)strtoul(val->number, NULL, 10);
        } else if (0 == strcmp(elem->name->string, "source_id")){
            struct json_number_s *val = (struct json_number_s *)elem->value->payload;
            source_id = (size_t)strtoul(val->number, NULL, 10);
        } else if (0 == strcmp(elem->name->string, "chunk_seq_len")){
            struct json_number_s *val = (struct json_number_s *)elem->value->payload;
            chunk_seq_len = (size_t)strtoul(val->number, NULL, 10);
        } else if (0 == strcmp(elem->name->string, "chunk_seq")){
            struct json_array_s *val = (struct json_array_s *)elem->value->payload;
            chunk_seq_json = val;
        }
    }
    mnfst->chunk_seq.chunk_checksum = (fz_hex_digest_t *)calloc(chunk_seq_len, sizeof(fz_hex_digest_t));
    mnfst->chunk_seq.cutpoint = (size_t *)calloc(chunk_seq_len, sizeof(size_t));
    mnfst->chunk_seq.chunk_size = (size_t *)calloc(chunk_seq_len, sizeof(size_t));

    if (NULL == mnfst->chunk_seq.chunk_checksum || NULL == mnfst->chunk_seq.cutpoint || NULL == mnfst->chunk_seq.chunk_size){
        RETURN_DEFER(0);
    }

    struct json_array_element_s *c_elem = NULL;
    for (size_t i = 0; i < chunk_seq_len; i++){
        c_elem = (0 == i)? chunk_seq_json->start : c_elem->next;
        struct json_object_s *item = (struct json_object_s *) c_elem->value->payload;
        for (size_t j = 0; j < item->length; j++){
            elem = (0 == j)? item->start : elem->next;
            if (0 == strcmp(elem->name->string, "chunk_checksum")){
                struct json_number_s *val = (struct json_number_s *)elem->value->payload;
                mnfst->chunk_seq.chunk_checksum[i] = (fz_hex_digest_t)strtoull(val->number, NULL, 16);
            } else if (0 == strcmp(elem->name->string, "cutpoint")){
                struct json_number_s *val = (struct json_number_s *)elem->value->payload;
                mnfst->chunk_seq.cutpoint[i] = (size_t)strtoul(val->number, NULL, 10);
            } else if (0 == strcmp(elem->name->string, "chunk_size")){
                struct json_number_s *val = (struct json_number_s *)elem->value->payload;
                source_id = (size_t)strtoul(val->number, NULL, 10);
                mnfst->chunk_seq.chunk_size[i] =  (size_t)strtoul(val->number, NULL, 10);
            }
        }
    }

    mnfst->file_name = file_name;
    mnfst->file_size = file_size;
    mnfst->file_checksum = digest;
    mnfst->source_id = source_id;
    mnfst->chunk_seq.chunk_seq_len = chunk_seq_len;

    defer:
        if (!result && NULL != file_name) {free(file_name); file_name = NULL;}
        if (NULL != root) free(root);
        return result;
}


/* Add a scratchpad for reader */
extern int fz_channel_write_request(fz_channel_t *channel, char *buffer, size_t data_size, char *scratchpad, size_t scratchpad_size){
    int result = 1;
    int request_d = -1;
    if (FZ_FIFO & channel->type){
        struct fz_fifo_channel_s *c_ptr = (struct fz_fifo_channel_s *)channel->channel_desc;
        pthread_mutex_lock(&(c_ptr->mtx));
        request_d = c_ptr->request_d;

        size_t total_written = 0;
        while (total_written < data_size) {
            size_t remaining = data_size - total_written;
            size_t want = LARGE_RESERVED > remaining ? remaining : LARGE_RESERVED;
            ssize_t n = write(request_d, buffer + total_written, want);
            if (n < 0) {
                fz_log(FZ_ERROR, "write() failed at offset %zu/%zu, errno=%d (%s)", total_written, data_size, errno, strerror(errno));
                pthread_mutex_unlock(&(c_ptr->mtx)); RETURN_DEFER(0);
            }
            total_written += (size_t)n;
        }
        pthread_mutex_unlock(&(c_ptr->mtx));
    } else if (FZ_TCP_SOCKET & channel->type) {
        // fz_log(FZ_INFO, "Writing to TCP server");
        struct fz_tcp_channel_s *c_ptr = (struct fz_tcp_channel_s *)channel->channel_desc;
        pthread_mutex_lock(&(c_ptr->mtx));
        request_d = c_ptr->socket_d;
        
        size_t total_sent = 0;
        while (total_sent < data_size) {
            ssize_t n = send(request_d, buffer + total_sent, data_size - total_sent, 0);
            if (0 >= n) {
                fz_log(FZ_ERROR, "Failed to write to TCP server");
                pthread_mutex_unlock(&(c_ptr->mtx)); 
                RETURN_DEFER(0);
            }
            total_sent += n;
        }
        pthread_mutex_unlock(&(c_ptr->mtx));
    }
    defer:
        return result;
}


extern int fz_channel_read_request(fz_channel_t *channel, char *buffer, size_t data_size, char *scratchpad, size_t scratchpad_size){
    int result = 1;
    int request_d = -1;
    char *temp_ = NULL;
    memset(buffer, 0, data_size);
    memset(scratchpad, 0, scratchpad_size);
    if (FZ_FIFO & channel->type){
        temp_ = scratchpad;
        if (NULL == temp_) RETURN_DEFER(0);
        struct fz_fifo_channel_s *c_ptr = (struct fz_fifo_channel_s *)channel->channel_desc;
        pthread_mutex_lock(&(c_ptr->mtx));
        request_d = c_ptr->request_d;

        size_t total_read = 0;
        while (total_read < data_size) {
            size_t remaining = data_size - total_read;
            size_t want = scratchpad_size > remaining ? remaining : scratchpad_size;
            ssize_t n = read(request_d, temp_, want);
            if (n < 0) {
                pthread_mutex_unlock(&(c_ptr->mtx)); RETURN_DEFER(0);
            }
            if (0 == n) {
                fz_log(FZ_ERROR, "Unexpected EOF on FIFO read, got %zu/%zu bytes", total_read, data_size);
                pthread_mutex_unlock(&(c_ptr->mtx)); RETURN_DEFER(0);
            }
            memcpy(buffer + total_read, temp_, (size_t)n);
            total_read += (size_t)n;
        }
        pthread_mutex_unlock(&(c_ptr->mtx));
    } else if (FZ_TCP_SOCKET & channel->type) {
        // fz_log(FZ_INFO, "Reading from TCP client");
        struct fz_tcp_channel_s *c_ptr = (struct fz_tcp_channel_s *)channel->channel_desc;
        pthread_mutex_lock(&(c_ptr->mtx));
        request_d = c_ptr->client_d;
        
        size_t total_read = 0;
        while (total_read < data_size) {
            ssize_t n = recv(request_d, buffer + total_read, data_size - total_read, 0);
            if (n <= 0) {
                fz_log(FZ_ERROR, "Failed to read from TCP client");
                pthread_mutex_unlock(&(c_ptr->mtx)); 
                RETURN_DEFER(0);
            }
            total_read += n;
        }
        pthread_mutex_unlock(&(c_ptr->mtx));
    }
    defer:
        return result;
}


extern int fz_channel_read_response(fz_channel_t *channel, char *buffer, size_t data_size, char *scratchpad, size_t scratchpad_size){
    int result = 1;
    int response_d = -1;
    char *temp_ = NULL;
    memset(buffer, 0, data_size);
    memset(scratchpad, 0, scratchpad_size);
    if (FZ_FIFO & channel->type){
        temp_ = scratchpad;
        if (NULL == temp_) RETURN_DEFER(0);
        struct fz_fifo_channel_s *c_ptr = (struct fz_fifo_channel_s *)channel->channel_desc;
        pthread_mutex_lock(&(c_ptr->mtx));
        response_d = c_ptr->response_d;
        for (size_t i = 0; i < data_size; i += scratchpad_size){
            size_t min = scratchpad_size > (data_size - i)? (data_size - i) : scratchpad_size;
            if (-1 == read(response_d, temp_, min)){
                pthread_mutex_unlock(&(c_ptr->mtx)); RETURN_DEFER(0);
            }
            if (NULL == temp_){pthread_mutex_unlock(&(c_ptr->mtx)); RETURN_DEFER(0);}
            memcpy(buffer + i, temp_, min);
            memset(temp_, 0, scratchpad_size);
        }
        pthread_mutex_unlock(&(c_ptr->mtx));
    } else if (FZ_TCP_SOCKET & channel->type) {
        struct fz_tcp_channel_s *c_ptr = (struct fz_tcp_channel_s *)channel->channel_desc;
        pthread_mutex_lock(&(c_ptr->mtx));
        response_d = c_ptr->socket_d;
        if (-1 == recv(response_d, buffer, data_size, 0)){
            fz_log(FZ_ERROR, "Oh no! failed to read from TCP server");
            pthread_mutex_unlock(&(c_ptr->mtx)); RETURN_DEFER(0);
        }
        pthread_mutex_unlock(&(c_ptr->mtx));
    }
    defer:
        return result;
}


extern int fz_channel_write_response(fz_channel_t *channel, char *buffer, size_t data_size){
    int result = 1;
    int response_d = -1;
    if (FZ_FIFO & channel->type){
        struct fz_fifo_channel_s *c_ptr = (struct fz_fifo_channel_s *)channel->channel_desc;
        pthread_mutex_lock(&(c_ptr->mtx));
        response_d = c_ptr->response_d;
        for (size_t i = 0; i < data_size; i += LARGE_RESERVED){
            size_t min = LARGE_RESERVED > (data_size - i)? (data_size - i) : LARGE_RESERVED;
            if (-1 == write(response_d, buffer + i, min)){
                pthread_mutex_unlock(&(c_ptr->mtx)); RETURN_DEFER(0);
            }
        }
        pthread_mutex_unlock(&(c_ptr->mtx));
    } else if (FZ_TCP_SOCKET & channel->type) {
        struct fz_tcp_channel_s *c_ptr = (struct fz_tcp_channel_s *)channel->channel_desc;
        pthread_mutex_lock(&(c_ptr->mtx));
        response_d = c_ptr->client_d;
        if (-1 == send(response_d, buffer, data_size, 0)){
            fz_log(FZ_ERROR, "Oh no! failed to write to TCP client");
            pthread_mutex_unlock(&(c_ptr->mtx)); RETURN_DEFER(0);
        }
        pthread_mutex_unlock(&(c_ptr->mtx));
    }

    defer:
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
        if (!result && NULL != buffer) {free(buffer); buffer = NULL;}
        return result;
}


static int get_filename_v2(arena_allocator_t *allocator, const char *file_path, char **file_name){
    int result = 1;
    string_t str = string_lib_init_slice(allocator, make_const_slice(file_path));
    if (NULL == str.ptr) RETURN_DEFER(0);

    array_list_t list = string_lib_split_string(allocator, &str, make_const_slice("/"));
    slice_t *ptr = (slice_t *)list.ptr;
    slice_t last_item = ptr[list.len - 1];
    int ret = string_lib_slice_to_cstring(allocator, last_item, file_name);
    if (0 != ret) RETURN_DEFER(0);
    defer:
        return result;
}