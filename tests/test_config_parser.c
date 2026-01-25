#include <stdio.h>


#define XXH_STATIC_LINKING_ONLY
#define XXH_IMPLEMENTATION
#define STB_DS_IMPLEMENTATION
#include "core.h"


int main(int argc, char *argv[]){
    (void)argc;
    (void)argv;
    /* Receiver device */
    fz_ctx_t recv_fz = {0};
    int result = 0;
    fz_channel_t recv_channel = {0};
    fz_config_t config = {0};

    char *config_file_path = "config/dest/init.json";

    if (!fz_parse_config_file(&config, config_file_path)){
        fz_log(FZ_ERROR, "Failed to parse config file `%s`", config_file_path);
        RETURN_DEFER(1);
    }

    if (!fz_ctx_init(&recv_fz, config.strategy, config.metadata_loc, config.target_dir, config.database_path, NULL, NULL)){
        fz_log(FZ_ERROR, "%s: Failed to initialize file zap reciever context", __func__);
        RETURN_DEFER(1);
    }
    fz_log(FZ_INFO, "Receiver context initialized successfully");

    /* Clean up */
    defer:
        fz_channel_destroy(&recv_channel);
        fz_ctx_destroy(&recv_fz);
        fz_config_file_destroy(&config);
        return result;
}