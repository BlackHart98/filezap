#include <stdio.h>
#include <string.h>
#include "core.h"


#include "../hash/xxhash.h"

#define CHUNK_LIST_RESERVED KB(64)
#define CHUNK_CHECKSUM_SEED 0x000000
#define FILE_CHECKSUM_SEED 0x123456


static inline int fz_chunking_fixed_size(fz_ctx_t *ctx, fz_file_manifest_t *file_mnfst, FILE *input_fd, size_t file_size, const char* src_file_path);
static inline int fz_seeding_fixed_size(fz_ctx_t *ctx, fz_file_manifest_t *file_mnfst, FILE *input_fd, size_t file_size, const char* src_file_path);


extern int fz_chunk_file(fz_ctx_t *ctx, fz_file_manifest_t *file_mnfst, const char* src_file_path){
    int result = 1;
    FILE *fd = fopen(src_file_path, "rb"); 
    if (NULL == fd){
        fz_log(FZ_ERROR, "Unable to open file `%s`", src_file_path);
        RETURN_DEFER(0);
    }

    /* Check file size */
    size_t file_size = 0;

    struct stat file_meta;
    if (0 == stat(src_file_path, &file_meta)){
        file_size = file_meta.st_size;
    } else {
        fz_log(FZ_ERROR, "[Error] Issue encountered while reading file `%s`", src_file_path);
        RETURN_DEFER(0);
    }

    if (0 == file_size){
        fz_log(FZ_ERROR, "[Error] Cannot chunk empty file `%s`", src_file_path);
        RETURN_DEFER(0);
    }

    if (FZ_FIXED_SIZED_CHUNK & ctx->chunk_strategy){
        if (!fz_chunking_fixed_size(ctx, file_mnfst, fd, file_size, src_file_path)){
            RETURN_DEFER(0);
        }
    } else {
        fz_log(FZ_ERROR, "Invalid chunking strategy");
        RETURN_DEFER(0);
    }
    defer:
        if (NULL != fd) fclose(fd);
        if (!result) fz_file_manifest_destroy(file_mnfst);
        return result;
}

/* 
The goal of this function is to cut down on storage use and focus more being able 
preprocess the files before hand
*/
extern int fz_seed_local_files(fz_ctx_t *ctx, const char *dir){
    /* 
    open db connection 
    for file in directory do
        seed file
        if seed file fails, log error
        else continue
    close db connection
    */
    int result = 1;
    defer:
        return result;
}


/*
using the chunk strategy in the context, get checksum seq, chunk cutpoints, and file asssociations
put result in a reference table in-memory/database
if chunking fails return 0 else return 1
*/
extern int fz_seed_local_file(fz_ctx_t *ctx, const char *src_file_path, int db_conn){
    FILE *fd = NULL; 
    fz_file_manifest_t file_mnfst = {0};
    size_t file_size = 0;
    int result = 1;
    struct stat file_meta;
    char *buffer = NULL;
    size_t buf_len = 0;
    fz_chunk_t *chunk_buffer = NULL;

    if (!fz_file_manifest_init(&file_mnfst)){
        fz_log(FZ_ERROR, "Unable to initialize manifest file");
        RETURN_DEFER(0);
    }

    fd = fopen(src_file_path, "rb"); 
    if (NULL == fd){
        fz_log(FZ_ERROR, "Unable to open file `%s`", src_file_path);
        RETURN_DEFER(0);
    }

    if (0 == stat(src_file_path, &file_meta)){
        file_size = file_meta.st_size;
    } else {
        fz_log(FZ_ERROR, "Issue encountered while reading file `%s`", src_file_path);
        RETURN_DEFER(0);
    }

    if (0 == file_size){
        fz_log(FZ_ERROR, "Cannot chunk empty file `%s`", src_file_path);
        RETURN_DEFER(0);
    }

    if (FZ_FIXED_SIZED_CHUNK & ctx->chunk_strategy){
        if (!fz_seeding_fixed_size(ctx, &file_mnfst, fd, file_size, src_file_path)){
            fz_log(FZ_ERROR, "Chunking failed");
            RETURN_DEFER(0);
        }
    } else {
        fz_log(FZ_ERROR, "Invalid chunking strategy");
        RETURN_DEFER(0);
    }
    fz_log(FZ_INFO, "File checksum: %016llx", file_mnfst.file_checksum);

    if (0 == file_mnfst.chunk_seq.chunk_seq_len) RETURN_DEFER(0);

    chunk_buffer = calloc(file_mnfst.chunk_seq.chunk_seq_len, sizeof(fz_chunk_t));
    if (NULL == chunk_buffer) RETURN_DEFER(0);

    // if (!fz_file_manifest_to_chunk_list(&file_mnfst, chunk_buffer, )) RETURN_DEFER(0);
    // if (!fz_fetch_chunks_from_file_cutpoint(ctx, &file_mnfst, chunk_buffer, file_mnfst.chunk_seq.chunk_seq_len)) RETURN_DEFER(0);
    defer:
        if (NULL != fd) fclose(fd);
        if (NULL != chunk_buffer) free(chunk_buffer);
        fz_file_manifest_destroy(&file_mnfst);
        return result;
}


extern inline int fz_file_manifest_to_chunk_list(fz_file_manifest_t *mnfst, fz_chunk_t *chunk_list, char *dest_file_path){
    for (size_t i = 0; i < mnfst->chunk_seq.chunk_seq_len; i++){
        chunk_list[i].chunk_checksum = mnfst->chunk_seq.chunk_checksum[i];
        chunk_list[i].chunk_size = mnfst->chunk_seq.chunk_size[i];
        chunk_list[i].cutpoint = mnfst->chunk_seq.cutpoint[i];
        chunk_list[i].src_file_path = mnfst->file_name;
    }
    return 1;
}


static inline int fz_chunking_fixed_size(fz_ctx_t *ctx, fz_file_manifest_t *file_mnfst, FILE *input_fd, size_t file_size, const char* src_file_path){
    /* Chunking section here needs to be isolated */
    int result = 1;
    size_t chunk_size = ctx->ctx_attrs.chunk_size;
    size_t chunk_seq_len = 0;
    fz_hex_digest_t digest = 0;
    char *block = NULL;
    char *chunk_loc_buffer = NULL;
    char *file_name = NULL;
    
    block = (char *)calloc(ctx->ctx_attrs.in_mem_buffer, sizeof(char));
    if (NULL == block) RETURN_DEFER(0);

    chunk_loc_buffer = (char *)calloc(strlen(ctx->metadata_loc) + 17, sizeof(char));
    if (NULL == chunk_loc_buffer) RETURN_DEFER(0);

    strncat(chunk_loc_buffer, ctx->metadata_loc, sizeof(ctx->metadata_loc));

    size_t allocation_size = (size_t)((file_size / chunk_size) + chunk_size);

    file_mnfst->chunk_seq.chunk_checksum = (fz_hex_digest_t *)calloc(allocation_size, sizeof(fz_hex_digest_t));
    file_mnfst->chunk_seq.cutpoint = (size_t *)calloc(allocation_size, sizeof(size_t));
    file_mnfst->chunk_seq.chunk_size = (size_t *)calloc(allocation_size, sizeof(size_t));

    if (NULL == file_mnfst->chunk_seq.chunk_checksum 
        || NULL == file_mnfst->chunk_seq.cutpoint 
        || NULL == file_mnfst->chunk_seq.chunk_size
    ){
        RETURN_DEFER(0);
    }

    size_t size_read;
    char temp[17] = {0};
    /* I will move this out of here */
    while((size_read = fread(block, 1, ctx->ctx_attrs.in_mem_buffer, input_fd)) > 0){
        for (size_t i = 0; i < size_read; i += chunk_size){
            size_t min = chunk_size;
            xxhash_hexdigest(block + i, min, &digest);
            file_mnfst->chunk_seq.chunk_checksum[chunk_seq_len] = digest;
            file_mnfst->chunk_seq.cutpoint[chunk_seq_len] = chunk_size * chunk_seq_len;
            file_mnfst->chunk_seq.chunk_size[chunk_seq_len] = chunk_size;

            snprintf(temp, 17, "%016llx", digest);
            memcpy(&chunk_loc_buffer[strlen(ctx->metadata_loc)], temp, 17);
            FILE *fh = fopen(chunk_loc_buffer, "wb");

            if (NULL == fh) RETURN_DEFER(0);
            fwrite(block + i, 1, min, fh);
            fclose(fh);
            chunk_seq_len++;
        }
        memset(block, 0, ctx->ctx_attrs.in_mem_buffer);
    }
    if (!xxhash_hexdigest_from_file(input_fd, &digest)) RETURN_DEFER(0);

    file_mnfst->file_checksum = digest;
    file_mnfst->source_id = ctx->ctx_id;
    file_mnfst->chunk_seq.chunk_seq_len = chunk_seq_len;

    file_name = calloc(strlen(src_file_path) + 1, sizeof(char));
    if (NULL == file_name) RETURN_DEFER(0);

    memcpy(file_name, src_file_path, strlen(src_file_path) + 1);
    // fz_log(FZ_INFO, "Source file path: %s, %s", src_file_path, file_name);

    file_mnfst->file_name = file_name;
    file_mnfst->file_size = file_size;
    defer:
        if (NULL != block) free(block);
        if (NULL != chunk_loc_buffer) free(chunk_loc_buffer);
        if (!result) fz_file_manifest_destroy(file_mnfst);
        return result;
}


static inline int fz_seeding_fixed_size(fz_ctx_t *ctx, fz_file_manifest_t *file_mnfst, FILE *input_fd, size_t file_size, const char* src_file_path){
    /* Chunking section here needs to be isolated */
    int result = 1;
    size_t chunk_size = ctx->ctx_attrs.chunk_size;
    size_t chunk_seq_len = 0;
    fz_hex_digest_t digest = 0;
    char *block = NULL;
    char *file_name = NULL;
    
    block = (char *)calloc(ctx->ctx_attrs.in_mem_buffer, sizeof(char));
    if (NULL == block) RETURN_DEFER(0);

    size_t allocation_size = (size_t)((file_size / chunk_size) + chunk_size);

    file_mnfst->chunk_seq.chunk_checksum = (fz_hex_digest_t *)calloc(allocation_size, sizeof(fz_hex_digest_t));
    file_mnfst->chunk_seq.cutpoint = (size_t *)calloc(allocation_size, sizeof(size_t));
    file_mnfst->chunk_seq.chunk_size = (size_t *)calloc(allocation_size, sizeof(size_t));

    if (NULL == file_mnfst->chunk_seq.chunk_checksum || NULL == file_mnfst->chunk_seq.cutpoint || NULL == file_mnfst->chunk_seq.chunk_size){
        RETURN_DEFER(0);
    }

    size_t size_read;
    /* I will move this out of here */
    while((size_read = fread(block, 1, ctx->ctx_attrs.in_mem_buffer, input_fd)) > 0){
        for (size_t i = 0; i < size_read; i += chunk_size){
            size_t min = (file_size - i) < size_read? size_read : chunk_size;
            xxhash_hexdigest(block + i, min, &digest);
            file_mnfst->chunk_seq.chunk_checksum[chunk_seq_len] = digest;
            file_mnfst->chunk_seq.cutpoint[chunk_seq_len] = chunk_size * chunk_seq_len;
            file_mnfst->chunk_seq.chunk_size[chunk_seq_len] = chunk_size;
            chunk_seq_len++;
        }
    }
    if (!xxhash_hexdigest_from_file(input_fd, &digest)) RETURN_DEFER(0);

    file_mnfst->file_checksum = digest;
    file_mnfst->source_id = ctx->ctx_id;
    file_mnfst->chunk_seq.chunk_seq_len = chunk_seq_len;

    file_name = calloc(strlen(src_file_path) + 1, sizeof(char));
    if (NULL == file_name) RETURN_DEFER(0);

    memcpy(file_name, src_file_path, strlen(src_file_path) + 1);

    // file_mnfst->file_name = src_file_path;
    file_mnfst->file_size = file_size;

    defer:
        if (NULL != block) free(block);
        if(!result) fz_file_manifest_destroy(file_mnfst);
        return result;
}


extern inline void xxhash_hexdigest(char *buffer, size_t stream_len, fz_hex_digest_t *digest){
    XXH64_hash_t hex = XXH3_64bits(buffer, stream_len);
    *digest = hex;
}


extern inline int xxhash_hexdigest_from_file(FILE *fd, fz_hex_digest_t *digest){
    int result = 1;
    fseek(fd, 0, SEEK_SET);
    XXH3_state_t* state = XXH3_createState();
    if (NULL == state) RETURN_DEFER(0);
    XXH3_64bits_reset(state);
    char buffer[KB(16)];
    size_t count;
    while ((count = fread(buffer, 1, sizeof(buffer), fd)) > 0) {
        XXH3_64bits_update(state, buffer, count);
    }
    XXH64_hash_t hex = XXH3_64bits_digest(state);
    *digest = hex;
    defer:
        if (NULL != state) XXH3_freeState(state);
        return result;
}


extern inline int xxhash_hexdigest_from_file_prime(fz_hex_digest_t *digest, fz_hex_digest_t *digest_list, size_t digest_len){
    int result = 1;
    char *buffer = calloc((digest_len * 16) + 1, sizeof(char));
    if (NULL == buffer) RETURN_DEFER(0);
    char temp[17] = {0};
    for (size_t i = 0; i < digest_len; i++){
        snprintf(temp, 17, "%016llx", digest_list[i]);
        strncat(buffer, temp, 16);
    }
    printf("File hash stream: buffer");
    XXH64_hash_t hex = XXH3_64bits(buffer, strlen(buffer));
    *digest = hex;
    defer:
        if (NULL != buffer) free(buffer);
        return result;
}