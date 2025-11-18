#include <stdlib.h>
#include "core.h"
#include "json.h"


static inline int get_filename(const char *file_path, char **file_name);

/* This is better version of the original send_file, there is not physical copy deposits in the sender cache folder */
extern int fz_send_file(fz_ctx_t *ctx, fz_channel_t *channel, const char *src_file_path){
    int result = 1;
    fz_file_manifest_t mnfst = {0};

    /* JSON serialized result */
    char *buffer = NULL;

    char *response_buffer = NULL;
    char *scratchpad = NULL;
    size_t scratchpad_size = LARGE_RESERVED;
    char *content_buffer = NULL;
    size_t alloc_size = XSMALL_RESERVED;
    FILE *src_fh = NULL;

    scratchpad = calloc(scratchpad_size, sizeof(char));
    if (NULL == scratchpad) {
        fz_log(FZ_ERROR, "Out of memory error in %s", __func__);
        RETURN_DEFER(0);
    }

    if (!fz_chunk_file(ctx, &mnfst, src_file_path)){
        fz_log(FZ_ERROR, "%s: Failed to chunk file `%s`", __func__, src_file_path);
        RETURN_DEFER(0);
    }

    size_t content_size = 0;
    if (!fz_serialize_manifest(&mnfst, &buffer, &content_size)) {
        fz_log(FZ_ERROR, "Failed to serialize manifest file");
        RETURN_DEFER(0);
    }
    if (0 == content_size || MAX_MANIFEST_SIZE < content_size) {
        fz_log(FZ_ERROR, "Content size of the manifest file violates the accepted boundary 0 < content_size < MAX_MANIFEST_SIZE (64MB): %lu", content_size / (KB(1) * KB(1)));
        RETURN_DEFER(0);
    }

    char number_as_str[XXSMALL_RESERVED] = {0};
    snprintf(number_as_str, XXSMALL_RESERVED, "%lu", content_size);

    if (!fz_channel_write_request(channel, number_as_str, XXSMALL_RESERVED)) {
        fz_log(FZ_ERROR, "Failed to send content size data to destination");
        RETURN_DEFER(0);
    }
    if (!fz_channel_write_request(channel, buffer, content_size)) {
        fz_log(FZ_ERROR, "Failed to send serialized manifest data to destination");
        RETURN_DEFER(0);
    }
    
    /* Waiting for response from the reciever, todo: remove unnecessary memset */
    size_t flag = 0;
    size_t chunk_max_alloc = 0;
    response_buffer = calloc(alloc_size, sizeof(char));
    if (NULL == response_buffer) {
        fz_log(FZ_ERROR, "Out of memory error in %s", __func__);
        RETURN_DEFER(0);
    }
    src_fh = fopen(src_file_path, "rb");
    if (NULL == src_fh) {
        fz_log(FZ_ERROR, "Failed to open source file `%s` for read", src_file_path);
        RETURN_DEFER(0);
    }
    while (1){
        /* Flag to control connection */
        do {
            char number_as_str[XXSMALL_RESERVED] = {0};
            if (!fz_channel_read_response(channel, number_as_str, XXSMALL_RESERVED, scratchpad, scratchpad_size)){
                fz_log(FZ_ERROR, "Somthing went wrong while trying read control flag data");
                RETURN_DEFER(0);
            }
            flag = strtoul(number_as_str, NULL, 10);
        } while(0);
        if (flag) break;

        if (!fz_channel_read_response(channel, number_as_str, XXSMALL_RESERVED, scratchpad, scratchpad_size)){
            RETURN_DEFER(0);
        }
        content_size = strtoul(number_as_str, NULL, 10);
        if (0 == content_size || MAX_MANIFEST_SIZE < content_size) {
            fz_log(FZ_ERROR, "Content size of the chunk violates the accepted boundary 0 < content_size < MAX_MANIFEST_SIZE (64MB): %lu", content_size / (KB(1) * KB(1)));
            RETURN_DEFER(0);
        }
        if (alloc_size < content_size){
            response_buffer = realloc(response_buffer, content_size);
            if (NULL == response_buffer) {fz_log(FZ_ERROR, "Out of memory error in %s", __func__); RETURN_DEFER(0);}
            alloc_size = content_size;
        }
        if (!fz_channel_read_response(channel, response_buffer, alloc_size, scratchpad, scratchpad_size)){
            RETURN_DEFER(0);
        }

        fz_chunk_response_t val = {0};
        if (!fz_deserialize_response(response_buffer, &val)) RETURN_DEFER(0);

        size_t chunk_size = mnfst.chunk_seq.chunk_size[val.chunk_index];
        if (chunk_max_alloc < chunk_size){
            content_buffer = realloc(content_buffer, chunk_size);
            if (NULL == content_buffer) {fz_log(FZ_ERROR, "Out of memory error in %s", __func__); RETURN_DEFER(0);}
            chunk_max_alloc = chunk_size;
        }
        memset(content_buffer, 0, chunk_max_alloc);
        size_t cutpoint = mnfst.chunk_seq.cutpoint[val.chunk_index];
        if (fseek(src_fh, cutpoint, SEEK_SET) < 0) RETURN_DEFER(0);
        fread(content_buffer, 1, chunk_size, src_fh);

        if (!fz_channel_write_request(channel, content_buffer, chunk_size)){
            fz_log(FZ_ERROR, "Failed to send chunk to destination");
            RETURN_DEFER(0);
        }

    }
    fz_log(FZ_INFO, "Closing connection");
    defer:
        fz_log(FZ_INFO, "Closed connection");
        if (NULL != src_fh) fclose(src_fh);
        if (NULL != response_buffer) free(response_buffer);
        if (NULL != content_buffer) free(content_buffer);
        if (NULL != buffer) free(buffer);
        if (NULL != scratchpad) free(scratchpad);
        fz_file_manifest_destroy(&mnfst);
        return result;
}


/*
Spawn a process for recieiving the file
- wait for the request from the sender, by pooling the fifo queue
- pop the manifest off the fifo
- try to retrieve the chunks in the manifest file:
    if the chunk is on the queue pop it and do something with it, when the total chunks need is complete generate the file and validate the file checksum the send the appropriate signal to the sender process via the fifo */
extern int fz_receive_file(fz_ctx_t *ctx, fz_channel_t *channel){    
    int result = 1;
    fz_file_manifest_t mnfst = {0};

    /* JSON serialized result */
    char *buffer = NULL;
    
    char *file_name = NULL;
    char *file_path_buffer = NULL;
    char number_as_str[XXSMALL_RESERVED] = {0};
    char *scratchpad = NULL;
    size_t scratchpad_size = LARGE_RESERVED;
    size_t flag = 0;

    scratchpad = calloc(scratchpad_size, sizeof(char));
    if (NULL == scratchpad) RETURN_DEFER(0);

    if (!fz_channel_read_request(channel, number_as_str, XXSMALL_RESERVED, scratchpad, scratchpad_size)) RETURN_DEFER(0);
    size_t content_size = strtoul(number_as_str, NULL, 10);
    if (0 == content_size || MAX_MANIFEST_SIZE < content_size) RETURN_DEFER(0);

    fz_log(FZ_INFO, "Received manifest json content size: %lukb", content_size/1024);
    buffer = calloc(content_size, sizeof(char));
    if (NULL == buffer) RETURN_DEFER(0);
    
    if (!fz_channel_read_request(channel, buffer, content_size, scratchpad, scratchpad_size)) RETURN_DEFER(0);
    if (!fz_deserialize_manifest(buffer, &mnfst)) RETURN_DEFER(0);
    if (!get_filename(mnfst.file_name, &file_name)) RETURN_DEFER(0);

    file_path_buffer = calloc(RESERVED, sizeof(char));
    if (NULL == file_path_buffer) RETURN_DEFER(0);

    /* Todo: Use an actual string object */
    snprintf(file_path_buffer, RESERVED, "%s%s", ctx->target_dir, file_name);
    fz_log(FZ_INFO, "File path: %s", file_path_buffer);

    if (!fz_retrieve_file(ctx, &mnfst, channel, file_path_buffer)) RETURN_DEFER(0);
    fz_log(FZ_INFO, "Receive file name: %s", file_path_buffer);

    /* Commit new chunk metadata, for now this is just a stub, I have to move thi out of here */
    if (!fz_commit_chunk_metadata(ctx, &mnfst, file_path_buffer)) RETURN_DEFER(0);
    defer:
        /* Notify sender that the files have been sent successfully 
        Todo: have different code to indicate the result file transfer i.e., FZ_TRANSFER_SUCCESS = 1 etc.
        This will improve visibilty of the file transfer process to the sender */
        flag = 1;
        SEND_CONN_FLAG(flag); /* Non-zero indicates close connection: This is not a very good idea */

        if (NULL != buffer) free(buffer);
        if (NULL != scratchpad) free(scratchpad);
        if (NULL != file_name) free(file_name);
        if (NULL != file_path_buffer) free(file_path_buffer);
        fz_file_manifest_destroy(&mnfst);
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
extern int fz_channel_write_request(fz_channel_t *channel, char *buffer, size_t data_size){
    int result = 1;
    int request_d = -1;
    if (FZ_FIFO & channel->type){
        struct fz_fifo_channel_s *c_ptr = (struct fz_fifo_channel_s *)channel->channel_desc;
        pthread_mutex_lock(&(c_ptr->mtx));
        request_d =  c_ptr->request_d;
        for (size_t i = 0; i < data_size; i += LARGE_RESERVED){
            size_t min = LARGE_RESERVED > (data_size - i)? (data_size - i) : LARGE_RESERVED;
            if (-1 == write(request_d, buffer + i, min)){
                if (NULL == buffer) fz_log(FZ_INFO, "Prepare...");
                fz_log(FZ_INFO, "I think it's a maxxed out issue: %lu", i);
                pthread_mutex_unlock(&(c_ptr->mtx)); RETURN_DEFER(0);
            }
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
        for (size_t i = 0; i < data_size; i += scratchpad_size){
            size_t min = scratchpad_size > (data_size - i)? (data_size - i) : scratchpad_size;
            if (-1 == read(request_d, temp_, min)){
                pthread_mutex_unlock(&(c_ptr->mtx)); RETURN_DEFER(0);
            }
            memcpy(buffer + i, temp_, min);
            memset(temp_, 0, scratchpad_size);
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
            memcpy(buffer + i, temp_, min);
            memset(temp_, 0, scratchpad_size);
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
        if (!result && NULL != buffer) free(buffer);
        return result;
}