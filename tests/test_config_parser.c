#include <stdio.h>


#define XXH_STATIC_LINKING_ONLY
#define XXH_IMPLEMENTATION
#define STB_DS_IMPLEMENTATION
#define WSA_IMPLEMENTATION
#define STRING_LIB_IMPLEMENTATION
#include "core.h"


int main(int argc, char *argv[]){
    (void)argc;
    (void)argv;
    /* Receiver device */
    fz_ctx_t recv_fz = {0};
    int result = 0;
    fz_channel_t recv_channel = {0};
    fz_config_t config = {0};

    // arena_allocator_t gpa = arena_allocator_init(c_allocator, MB(1), KB(2));
    // if (NULL == gpa.linkedlist) {
    //     fz_log(FZ_ERROR, "Failed to initialize arena allocator in %s", __func__);
    //     RETURN_DEFER(0);
    // }

    context_t context = context_init(MB(1), KB(512));
    if (NULL == context.allocator.linkedlist || NULL == context.temp_allocator.linkedlist){
        fz_log(FZ_ERROR, "Failed to initialize arena context in %s", __func__);
        RETURN_DEFER(1);
    }

    char *config_file_path = "config/dest/init.json";

    if (!fz_parse_config_file(&(context.allocator), &config, config_file_path)){
        fz_log(FZ_ERROR, "Failed to parse config file `%s`", config_file_path);
        RETURN_DEFER(1);
    }

    if (!fz_ctx_init(
        &recv_fz, 
        config.strategy, 
        config.metadata_loc, 
        config.target_dir, 
        config.database_path, NULL, NULL)
    ){
        fz_log(FZ_ERROR, "%s: Failed to initialize file zap reciever context", __func__);
        RETURN_DEFER(1);
    }
    fz_log(FZ_INFO, "Receiver context initialized successfully");


    /* Clean up */
    defer:
        fz_channel_destroy(&recv_channel);
        fz_ctx_destroy(&recv_fz);
        // arena_allocator_deinit(&gpa);
        context_deinit(&context);
        return result;
}