#include <stdio.h>
#include <string.h>
#include "core.h"


#include "../hash/xxhash.h"

#define CHUNK_LIST_RESERVED KB(64)
#define CHUNK_CHECKSUM_SEED 0x000000
#define FILE_CHECKSUM_SEED 0x123456


static inline int fz_chunking_fixed_size(fz_ctx_t *ctx, fz_file_manifest_t *file_mnfst, FILE *input_fd, size_t file_size, const char* src_file_path);
static inline int fz_chunking_variable_size(fz_ctx_t *ctx, fz_file_manifest_t *file_mnfst, FILE *input_fd, size_t file_size, const char* src_file_path);


extern int fz_chunk_file(fz_ctx_t *ctx, fz_file_manifest_t *file_mnfst, const char* src_file_path){
    int result = 1;
    FILE *fd = NULL; 
    size_t file_size = 0;
    struct stat file_meta;

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
    switch (ctx->chunk_strategy) {
        case FZ_FIXED_SIZED_CHUNK:
            if (!fz_chunking_fixed_size(ctx, file_mnfst, fd, file_size, src_file_path)){
                fz_log(FZ_ERROR, "Fixed sized chunking failed"); RETURN_DEFER(0);
            }
            break;
        case FZ_GEAR_CDC_CHUNK:
            assert(0&&"Todo: Gear CDC chunk not yet implemented!");
            break;
        default:
            fz_log(FZ_ERROR, "Invalid chunking strategy");
            RETURN_DEFER(0);
            break;
    }
    fz_log(FZ_INFO, "File checksum: %016llx", file_mnfst->file_checksum);
    if (0 == file_mnfst->chunk_seq.chunk_seq_len) {
        fz_log(FZ_ERROR, "Fatal error, file manifest chunk sequence chunk length is 0");
        RETURN_DEFER(0);
    }

    defer:
        if (NULL != fd) fclose(fd);
        if (!result) fz_file_manifest_destroy(file_mnfst);
        return result;
}


extern inline int fz_file_manifest_to_chunk_list(fz_file_manifest_t *mnfst, fz_chunk_t *chunk_list){
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
    char *file_name = NULL;
    
    block = (char *)calloc(ctx->ctx_attrs.in_mem_buffer, sizeof(char));
    if (NULL == block) {
        fz_log(FZ_ERROR, "Out of memory error in %s", __func__);
        RETURN_DEFER(0);
    }
    size_t allocation_size = (size_t)((file_size / chunk_size) + chunk_size);

    file_mnfst->chunk_seq.chunk_checksum = (fz_hex_digest_t *)calloc(allocation_size, sizeof(fz_hex_digest_t));
    file_mnfst->chunk_seq.cutpoint = (size_t *)calloc(allocation_size, sizeof(size_t));
    file_mnfst->chunk_seq.chunk_size = (size_t *)calloc(allocation_size, sizeof(size_t));

    if (NULL == file_mnfst->chunk_seq.chunk_checksum || NULL == file_mnfst->chunk_seq.cutpoint || NULL == file_mnfst->chunk_seq.chunk_size){
        fz_log(FZ_ERROR, "Out of memory error in %s", __func__);
        RETURN_DEFER(0);
    }
    size_t size_read = 0;
    while((size_read = fread(block, 1, ctx->ctx_attrs.in_mem_buffer, input_fd)) > 0){
        for (size_t i = 0; i < size_read; i += chunk_size){
            size_t min = chunk_size;
            xxhash_hexdigest(block + i, min, &digest);
            file_mnfst->chunk_seq.chunk_checksum[chunk_seq_len] = digest;
            file_mnfst->chunk_seq.cutpoint[chunk_seq_len] = chunk_size * chunk_seq_len;
            file_mnfst->chunk_seq.chunk_size[chunk_seq_len] = chunk_size;
            chunk_seq_len++;
        }
        memset(block, 0, ctx->ctx_attrs.in_mem_buffer);
    }
    if (!xxhash_hexdigest_from_file(input_fd, &digest)) {
        fz_log(FZ_ERROR, "Something went wrong while trying to generate file hash");
        RETURN_DEFER(0);
    }

    file_mnfst->file_checksum = digest;
    file_mnfst->chunk_seq.chunk_seq_len = chunk_seq_len;

    file_name = calloc(strlen(src_file_path) + 1, sizeof(char));
    if (NULL == file_name) {
        fz_log(FZ_ERROR, "Out of memory error in %s", __func__);
        RETURN_DEFER(0);
    }

    memcpy(file_name, src_file_path, strlen(src_file_path) + 1);
    file_mnfst->file_name = file_name;
    file_mnfst->file_size = file_size;

    defer:
        if (NULL != block) free(block);
        if (!result) fz_file_manifest_destroy(file_mnfst);
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
    if (NULL == state) {
        fz_log(FZ_ERROR, "Out of memory error in %s", __func__);
        RETURN_DEFER(0);
    }
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
    if (NULL == buffer) {
        fz_log(FZ_ERROR, "Out of memory error in %s", __func__);
        RETURN_DEFER(0);
    }
    char temp[17] = {0};
    for (size_t i = 0; i < digest_len; i++){
        snprintf(temp, HEX_DIGIT_SIZE, "%016llx", digest_list[i]);
        strncat(buffer, temp, 16);
    }
    printf("File hash stream: buffer");
    XXH64_hash_t hex = XXH3_64bits(buffer, strlen(buffer));
    *digest = hex;
    defer:
        if (NULL != buffer) free(buffer);
        return result;
}