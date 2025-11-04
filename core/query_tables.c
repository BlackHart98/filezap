#include "core.h"

extern int fz_query_required_chunk_list(fz_ctx_t *ctx, fz_file_manifest_t *mnfst, fz_chunk_t **chunk_buffer, size_t *nchunk){
    int result = 1;
    fz_chunk_t *buffer = NULL;
    size_t local_nchunk = 0;
    sqlite3_stmt *insert = NULL;
    sqlite3_stmt *stmt = NULL;
    int ret;

    /* SQL templates */
    const char *create_tbl_sql = "CREATE TEMP TABLE IF NOT EXISTS temp_manifest_chunks (chunk_checksum INTEGER);";
    const char *insert_sql = "INSERT INTO temp_manifest_chunks (chunk_checksum) VALUES (?);";
    const char *clear_temp_sql = "DELETE FROM temp_manifest_chunks;";
    const char *sql =
        "SELECT f.id, f.chunk_checksum, f.cutpoint, f.chunk_size, f.file_path "
        "FROM filezap_chunks f "
        "JOIN temp_manifest_chunks t ON f.chunk_checksum = t.chunk_checksum;";

    ret = sqlite3_exec(ctx->db, create_tbl_sql, 0, NULL, NULL);
    if (SQLITE_OK != ret) {
        fz_log(FZ_INFO, "Something went wrong while creating temporary table");
        RETURN_DEFER(0);
    }

    ret = sqlite3_exec(ctx->db, clear_temp_sql, NULL, NULL, NULL);
    if (SQLITE_OK != ret) {
        fz_log(FZ_INFO, "Failed to clear temp table: %s", sqlite3_errmsg(ctx->db));
        RETURN_DEFER(0);
    }

    sqlite3_prepare_v2(ctx->db, insert_sql, -1, &insert, NULL);
    for (size_t i = 0; i < mnfst->chunk_seq.chunk_seq_len; i++){
        sqlite3_bind_int64(insert, 1, mnfst->chunk_seq.chunk_checksum[i]);
        sqlite3_step(insert);
        sqlite3_reset(insert);
    }

    ret = sqlite3_prepare_v2(ctx->db, sql, -1, &stmt, NULL);
    if (SQLITE_OK != ret) RETURN_DEFER(0);

    buffer = calloc(mnfst->chunk_seq.chunk_seq_len, sizeof(fz_chunk_t));
    if (NULL == buffer) RETURN_DEFER(0);
    /* I need to fix this part */
    while (SQLITE_ROW == (ret = sqlite3_step(stmt))){
        fz_hex_digest_t chunk_checksum = (fz_hex_digest_t)sqlite3_column_int64(stmt, 1);
        size_t cutpoint = (size_t)sqlite3_column_int64(stmt, 2);
        size_t chunk_size = (size_t)sqlite3_column_int64(stmt, 3);
        const char *file_path = (char *)sqlite3_column_text(stmt, 4);

        if (local_nchunk >= mnfst->chunk_seq.chunk_seq_len) {
            fz_log(FZ_INFO, "More rows returned than expected; ignoring extras");
            break;
        }

        buffer[local_nchunk].chunk_checksum = chunk_checksum;
        buffer[local_nchunk].cutpoint = cutpoint;
        buffer[local_nchunk].chunk_size = chunk_size;
        buffer[local_nchunk].src_file_path = calloc(strlen(file_path) + 1, sizeof(char));
        if (NULL == buffer[local_nchunk].src_file_path) RETURN_DEFER(0);
        strncat(buffer[local_nchunk].src_file_path, file_path, strlen(file_path));
        local_nchunk++;
    }
    if (SQLITE_DONE != ret) RETURN_DEFER(0);
    *nchunk = local_nchunk;
    *chunk_buffer = buffer;
    fz_log(FZ_INFO, "chunk size: %lu", local_nchunk);
    defer:
        if (NULL != insert) sqlite3_finalize(insert);
        if (NULL != stmt) sqlite3_finalize(stmt);
        if (!result && NULL != buffer) {
            for(size_t i = 0; i < local_nchunk; i++){
                if (NULL != buffer[i].src_file_path) free((char *)buffer[i].src_file_path);
            }
            free(buffer);
        }
        return result;
}