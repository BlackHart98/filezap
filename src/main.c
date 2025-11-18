#include <stdio.h>

#define XXH_STATIC_LINKING_ONLY
#define XXH_IMPLEMENTATION
#define JSN_IMPLEMENTATION
#define STB_DS_IMPLEMENTATION
#include "core.h"
#include <sqlite3.h>


int main(int argc, char *argv[]){
    (void)argc;
    (void)argv;
    // char *input_file = "examples/src/Free Nationals - Beauty & Essex (feat. Daniel Caesar & Unknown Mortal Orchestra)(1).mp4";
    char *input_file = "examples/src/Endless, a film by Frank Ocean.mp4";

    /* Sender and receiver device */
    fz_ctx_t snd_fz = {0}, recv_fz = {0};
    fz_chunk_t *chunk_list = NULL;
    fz_file_manifest_t mnfst = {0};
    int result = 0;
    char *buffer = NULL;
    char *chunk_loc_buffer = NULL;

    struct cutpoint_map_s *cutpoint_map = NULL;
    struct missing_chunks_map_s *missing_chunks = NULL;

    if (!fz_ctx_init(&snd_fz, FZ_FIXED_SIZED_CHUNK, "tmp/", "examples/src/", "filezap.db", NULL, NULL)){
        fz_log(FZ_ERROR, "%s: Failed to initialize file zap sender context", __func__);
        RETURN_DEFER(1);
    }
    /* Chunking phase */ 
    if (!fz_chunk_file(&snd_fz, &mnfst, input_file)){
        fz_log(FZ_ERROR, "%s: Failed to chunk file `%s`", __func__, input_file);
        RETURN_DEFER(1);
    }
    fz_log(FZ_INFO, "File chunked successfully");
    fz_log(FZ_INFO, "File manifest file: %s", mnfst.file_name);

    /* Clean up */
    defer:
        if (NULL != chunk_list) free(chunk_list);
        if (NULL != buffer) free(buffer);
        if (NULL != chunk_loc_buffer) free(chunk_loc_buffer);
        if (NULL != missing_chunks) hmfree(missing_chunks);
        if (NULL != cutpoint_map){
            for (size_t i = 0; i < shlenu(cutpoint_map); i++){
                fz_cutpoint_list_destroy(cutpoint_map[i].value);
            }
            shfree(cutpoint_map);
        }
        fz_ctx_destroy(&snd_fz);
        fz_ctx_destroy(&recv_fz);
        fz_file_manifest_destroy(&mnfst);
        return result;
}