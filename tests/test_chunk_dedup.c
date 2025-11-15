#include <stdio.h>

#define XXH_STATIC_LINKING_ONLY
#define XXH_IMPLEMENTATION
#define JSN_IMPLEMENTATION
#define STB_DS_IMPLEMENTATION
#include "core.h"
#include <sqlite3.h>

static inline size_t calculate_chunk_size(fz_file_manifest_t *mnfst);

int main(int argc, char *argv[]){
    (void)argc;
    (void)argv;

    int result = 0;
    fz_ctx_t my_ctx = {0};
    fz_file_manifest_t mnfst = {0};

    char *input_file = "examples/src/Free Nationals - Beauty & Essex (feat. Daniel Caesar & Unknown Mortal Orchestra)(1).mp4";

    if (!fz_ctx_init(&my_ctx, (fz_ctx_desc_t)&my_ctx, FZ_FIXED_SIZED_CHUNK, "tmp/", "examples/src/", "filezap.db", NULL, NULL)){
        fz_log(FZ_ERROR, "%s: Failed to initialize file zap sender context", __func__);
        RETURN_DEFER(1);
    }
    if (!fz_chunk_file(&my_ctx, &mnfst, input_file)){
        fz_log(FZ_ERROR, "%s: Failed to chunk file `%s`", __func__, input_file);
        RETURN_DEFER(1);
    }
    fz_log(FZ_INFO, "File chunked successfully");
    fz_log(FZ_INFO, "File manifest file: %s", mnfst.file_name);

    size_t total_chunk_size = calculate_chunk_size(&mnfst);
    fz_log(FZ_INFO, "The total sum of the chunk of the file is %lu", total_chunk_size);
    fz_log(FZ_INFO, "The actual file size: %lu", mnfst.file_size);
    
    ssize_t diff_size = (ssize_t)(mnfst.file_size - total_chunk_size);
    
    if (0 > diff_size) fz_log(FZ_INFO, "The total chunk size is larger by %ld byte(s)", (-1 * diff_size));
    else if (0 == diff_size) fz_log(FZ_INFO, "The total chunk size is equal to the file size");
    else fz_log(FZ_INFO, "The chunk is less by %ld byte(s)", diff_size);
    defer:
        fz_ctx_destroy(&my_ctx);
        fz_file_manifest_destroy(&mnfst);
        return result;
}


static inline size_t calculate_chunk_size(fz_file_manifest_t *mnfst){
    size_t result = 0;
    struct{fz_hex_digest_t key; uint8_t value;} *seen_chunk_map = NULL;
    hmdefault(seen_chunk_map, 0);
    for (size_t i = 0; i < mnfst->chunk_seq.chunk_seq_len; i++){
        if (0 == hmget(seen_chunk_map, mnfst->chunk_seq.chunk_checksum[i])){
            result += mnfst->chunk_seq.chunk_size[i];
            hmput(seen_chunk_map, mnfst->chunk_seq.chunk_checksum[i], 1);
        }
    }
    if (NULL != seen_chunk_map) hmfree(seen_chunk_map);
    return result;
}