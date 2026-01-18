#include "core.h"
#include <sys/stat.h>

/* This is a big issue I need to tackle */
extern int fz_query_required_chunk_list(fz_ctx_t *ctx, fz_file_manifest_t *mnfst, fz_chunk_t **chunk_buffer, size_t *nchunk, struct missing_chunks_map_s **missing_chunks){
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
        "FROM filezap_chunks AS f "
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
        if (0 == hmget(*missing_chunks, mnfst->chunk_seq.chunk_checksum[i])) continue;
        else {
            sqlite3_bind_int64(insert, 1, mnfst->chunk_seq.chunk_checksum[i]);
            sqlite3_step(insert);
            sqlite3_reset(insert);
        }
    }

    ret = sqlite3_prepare_v2(ctx->db, sql, -1, &stmt, NULL);
    if (SQLITE_OK != ret) RETURN_DEFER(0);

    buffer = calloc(mnfst->chunk_seq.chunk_seq_len, sizeof(fz_chunk_t));
    if (NULL == buffer) RETURN_DEFER(0);
    size_t max_alloc = mnfst->chunk_seq.chunk_seq_len;
    /* I need to fix this part */
    while (SQLITE_ROW == (ret = sqlite3_step(stmt))){
        fz_hex_digest_t chunk_checksum = (fz_hex_digest_t)sqlite3_column_int64(stmt, 1);
        size_t cutpoint = (size_t)sqlite3_column_int64(stmt, 2);
        size_t chunk_size = (size_t)sqlite3_column_int64(stmt, 3);
        const char *file_path = (char *)sqlite3_column_text(stmt, 4);

        /* resize the buffer if allocated space exceeded */
        if (local_nchunk >= max_alloc) {
            max_alloc *= 2;
            buffer = realloc(buffer, max_alloc * sizeof(fz_chunk_t));
            if (NULL == buffer) RETURN_DEFER(0);
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
    fz_log(FZ_INFO, "Found chunk size: %lu", local_nchunk);
    defer:
        if (NULL != insert) sqlite3_finalize(insert);
        if (NULL != stmt) sqlite3_finalize(stmt);
        if (!result && NULL != buffer) {
            for (size_t i = 0; i < local_nchunk; i++){
                if (NULL != buffer[i].src_file_path) {free((char *)buffer[i].src_file_path); buffer[i].src_file_path = NULL;}
            }
            free(buffer); buffer = NULL;
        }
        return result;
}


extern int fz_commit_chunk_metadata(fz_ctx_t *ctx, fz_file_manifest_t *mnfst, char *dest_file_path){
    int result = 1;
    int ret;
    sqlite3_stmt *insert = NULL;
    struct{fz_hex_digest_t key; uint8_t value;} *seen_chunk_map = NULL;
    const char *temp_table = 
        "CREATE TEMP TABLE temp_filezap_chunks("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "chunk_checksum INTEGER NOT NULL,"
            "cutpoint INTEGER NOT NULL,"
            "chunk_size INTEGER NOT NULL,"
            "file_path TEXT NOT NULL"
        ");";
    const char *insert_into_temp_filezap = "INSERT INTO temp_filezap_chunks (chunk_checksum, cutpoint, chunk_size, file_path) VALUES (?,?,?,?);";
    const char *unique_entries = 
        "CREATE TEMP TABLE unique_filezap_chunks AS "
        "SELECT t.chunk_checksum, t.cutpoint, t.chunk_size, t.file_path FROM temp_filezap_chunks AS t "
        "EXCEPT "
        "SELECT f.chunk_checksum, f.cutpoint, f.chunk_size, f.file_path FROM filezap_chunks AS f "
        ";";
    const char *insert_into_filezap = 
        "INSERT INTO filezap_chunks (chunk_checksum, cutpoint, chunk_size, file_path) "
        "SELECT t.chunk_checksum, t.cutpoint, t.chunk_size, t.file_path FROM unique_filezap_chunks AS t "
        ";";

    fz_log(FZ_INFO, "Committing chunk metadata");
    sqlite3_exec(ctx->db, "BEGIN TRANSACTION;", NULL, NULL, NULL);
    ret = sqlite3_exec(ctx->db, temp_table, NULL, NULL, NULL);
    if (SQLITE_OK != ret) {
        fz_log(FZ_INFO, "Something went wrong while creating temporary table `temp_filezap_chunks`");
        RETURN_DEFER(0);
    }
    hmdefault(seen_chunk_map, 0);
    sqlite3_prepare_v2(ctx->db, insert_into_temp_filezap, -1, &insert, NULL);
    for (size_t i = 0; i < mnfst->chunk_seq.chunk_seq_len; i++){
        if (1 == hmget(seen_chunk_map, mnfst->chunk_seq.chunk_checksum[i])) continue;
        sqlite3_bind_int64(insert, 1, mnfst->chunk_seq.chunk_checksum[i]);
        sqlite3_bind_int64(insert, 2, mnfst->chunk_seq.cutpoint[i]);
        sqlite3_bind_int64(insert, 3, mnfst->chunk_seq.chunk_size[i]);
        sqlite3_bind_text(insert, 4, dest_file_path, -1, SQLITE_TRANSIENT);
        if (SQLITE_DONE != sqlite3_step(insert)) {
            fz_log(FZ_ERROR, "Insert failed for chunk %zu: %s", i, sqlite3_errmsg(ctx->db));
            sqlite3_exec(ctx->db, "ROLLBACK;", NULL, NULL, NULL);
            RETURN_DEFER(0);
        }
        sqlite3_reset(insert);
        hmput(seen_chunk_map, mnfst->chunk_seq.chunk_checksum[i], 1);
    }
    ret = sqlite3_exec(ctx->db, unique_entries, NULL, NULL, NULL);
    if (SQLITE_OK != ret) {
        fz_log(FZ_INFO, "Something went wrong while creating unique entries table `filezap_chunks`");
        sqlite3_exec(ctx->db, "ROLLBACK;", NULL, NULL, NULL);
        RETURN_DEFER(0);
    }
    ret = sqlite3_exec(ctx->db, insert_into_filezap, NULL, NULL, NULL);
    if (SQLITE_OK != ret) {
        fz_log(FZ_INFO, "Something went wrong while inserting chunk metadata into `filezap_chunks`");
        sqlite3_exec(ctx->db, "ROLLBACK;", NULL, NULL, NULL);
        RETURN_DEFER(0);
    }
    sqlite3_exec(ctx->db, "COMMIT;", NULL, NULL, NULL);
    fz_log(FZ_INFO, "Chunk metadata committed successfully");
    defer:
        if (NULL != insert) sqlite3_finalize(insert);
        return result;
}


extern int fz_query_unused_chunk(fz_ctx_t *ctx, fz_hex_digest_t **unused_chunk_list, size_t *nchunk){
    int result = 1;
    int ret;
    sqlite3_stmt *stmt = NULL;
    struct{fz_hex_digest_t key; uint8_t value;} *chunk_is_ref = NULL;
    fz_hex_digest_t *buffer = NULL;
    const char *file_list_sql = "SELECT f.chunk_checksum, f.file_path FROM filezap_chunks AS f;";

    struct {
        fz_hex_digest_t *digest;
        char **file_path;
        // uint8_t *file_not_found;
    } chunk_checksum_file_path = {0};

    ret = sqlite3_prepare_v2(ctx->db, file_list_sql, -1, &stmt, NULL);
    if (SQLITE_OK != ret) {
        fz_log(FZ_ERROR, "Something went wrong here");
        RETURN_DEFER(0);
    }

    size_t max_alloc = RESERVED; // preallocate this memory size
    size_t local_size = 0;
    chunk_checksum_file_path.digest = calloc(max_alloc, sizeof(fz_hex_digest_t));
    chunk_checksum_file_path.file_path = calloc(max_alloc, sizeof(char *));
    if (NULL == chunk_checksum_file_path.digest || NULL == chunk_checksum_file_path.file_path) RETURN_DEFER(0);
    while (SQLITE_ROW == (ret = sqlite3_step(stmt))){
        fz_hex_digest_t chunk_checksum = (fz_hex_digest_t)sqlite3_column_int64(stmt, 0);
        char *file_path = (char *)sqlite3_column_text(stmt, 1);
        if (local_size >= max_alloc) {
            max_alloc *= 2;
            chunk_checksum_file_path.digest = calloc(max_alloc, sizeof(fz_hex_digest_t));
            chunk_checksum_file_path.file_path = calloc(max_alloc, sizeof(char *));
            if (NULL == chunk_checksum_file_path.digest || NULL == chunk_checksum_file_path.file_path) RETURN_DEFER(0);
        }
        chunk_checksum_file_path.digest[local_size] = chunk_checksum;
        chunk_checksum_file_path.file_path[local_size] = calloc(strlen(file_path) + 1, sizeof(char));
        if (NULL == chunk_checksum_file_path.file_path[local_size]) RETURN_DEFER(0);

        strncat(chunk_checksum_file_path.file_path[local_size], file_path, strlen(file_path));
        local_size++;
    }
    if (SQLITE_DONE != ret) RETURN_DEFER(0);

    hmdefault(chunk_is_ref, 0);
    for (size_t i = 0; i < local_size; i++){
        struct stat file_meta = {0};
        uint8_t temp_ = hmget(chunk_is_ref, chunk_checksum_file_path.digest[i]);
        if (0 == stat(chunk_checksum_file_path.file_path[i], &file_meta)){
            temp_ |= 1;
            hmput(chunk_is_ref, chunk_checksum_file_path.digest[i], temp_);
        } else {
            // fz_log(FZ_INFO, "File %s is no longer available", chunk_checksum_file_path.file_path[i]);
            hmput(chunk_is_ref, chunk_checksum_file_path.digest[i], temp_);
        }
    }

    size_t max_buffer_alloc = local_size;
    size_t buffer_len = 0;
    buffer = calloc(max_buffer_alloc, sizeof(fz_hex_digest_t));
    if (NULL == buffer) RETURN_DEFER(0);
    for (size_t i = 0; i < hmlenu(chunk_is_ref); i++){
        if (0 == hmget(chunk_is_ref, chunk_checksum_file_path.digest[i])){
            buffer[buffer_len] = chunk_checksum_file_path.digest[i];
            buffer_len++;
        }
    }

    *unused_chunk_list = buffer;
    *nchunk = buffer_len;
    fz_log(FZ_INFO, "Total number of unused chunks: %lu", buffer_len);
    defer:
        if (NULL != chunk_checksum_file_path.digest) free(chunk_checksum_file_path.digest);
        if (NULL != chunk_checksum_file_path.file_path) {
            for (size_t i = 0; i < local_size; i++){
                if (NULL != chunk_checksum_file_path.file_path[i]) free(chunk_checksum_file_path.file_path[i]);
            }
            free(chunk_checksum_file_path.file_path);
        }
        if (NULL != stmt) sqlite3_finalize(stmt);
        return result;
}


extern int fz_commit_janitor_change(fz_ctx_t *ctx, fz_hex_digest_t *unused_chunk_list, size_t nchunk){
    int result = 1;
    sqlite3_stmt *stmt = NULL; 
    int ret;

    const char *delete_chunk_sql = "DELETE FROM filezap_chunks WHERE chunk_checksum = ?;";
    ret = sqlite3_prepare_v2(ctx->db, delete_chunk_sql, -1, &stmt, NULL);
    if (SQLITE_OK != ret) {RETURN_DEFER(0);}
    for (size_t i = 0; i < nchunk; i++){
        sqlite3_bind_int64(stmt, 1, unused_chunk_list[i]);
        sqlite3_step(stmt);
        sqlite3_reset(stmt);
    }
    defer:
        if (NULL != stmt) sqlite3_finalize(stmt);
        return result;
}