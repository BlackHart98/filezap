#include "core.h"

extern int fz_janitor_clean_up(fz_ctx_t *ctx){
    int result = 1;
    fz_hex_digest_t *unused_checksum = NULL;
    size_t buffer_len = 0;
    char *temp_buffer = NULL;
    size_t temp_buffer_len = strlen(ctx->metadata_loc) + HEX_DIGIT_SIZE + 1;
    temp_buffer = calloc(temp_buffer_len, sizeof(char));
    if (NULL == temp_buffer) RETURN_DEFER(0);

    if (!fz_query_unused_chunk(ctx, &unused_checksum, &buffer_len)){
        fz_log(FZ_ERROR, "Could not fetch unused chunks");
        RETURN_DEFER(0);
    }
    if (0 != buffer_len){
        fz_log(FZ_INFO, "Trashing unused chunk reference...");
        for (size_t i = 0; i < buffer_len; i++){
            snprintf(temp_buffer, temp_buffer_len, "%s%016llx", ctx->metadata_loc, unused_checksum[i]);
            fz_log(FZ_INFO, "Deleting %s chunk", temp_buffer);
            remove(temp_buffer);
        }
    }

    if (!fz_commit_janitor_change(ctx, unused_checksum, buffer_len)){
        fz_log(FZ_ERROR, "Could not commit janitor changes");
    }
    defer:
        if (NULL != unused_checksum) free(unused_checksum);
        if (NULL != temp_buffer) free(temp_buffer);
        return result;
}