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

    int result = 0;
    fz_ctx_t my_ctx = {0};

    if (!fz_ctx_init(&my_ctx, FZ_FIXED_SIZED_CHUNK, "dtmp/", "examples/dest/", "filezap.db", NULL, NULL)){
        fz_log(FZ_ERROR, "%s: Failed to initialize file zap sender context", __func__);
        RETURN_DEFER(1);
    }

    if (!fz_janitor_clean_up(&my_ctx)){
        fz_log(FZ_ERROR, "%s: Something went while trying to clean up unused chunks", __func__);
        RETURN_DEFER(1);
    }
    defer:
        fz_ctx_destroy(&my_ctx);
        return result;
}