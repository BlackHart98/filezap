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

    context_t context = context_init(MB(1), KB(512));
    if (NULL == context.allocator.linkedlist || NULL == context.temp_allocator.linkedlist){
        fz_log(FZ_ERROR, "Failed to initialize arena context in %s", __func__);
        RETURN_DEFER(1);
    }

    if (!fz_ctx_init(&recv_fz, FZ_FIXED_SIZED_CHUNK, "dtmp/", "examples/dest/", "filezap.db", NULL, NULL)){
        fz_log(FZ_ERROR, "%s: Failed to initialize file zap reciever context", __func__);
        RETURN_DEFER(1);
    }
    fz_log(FZ_INFO, "Receiver context initialized successfully");

    if (!fz_channel_init_v2(&(context.allocator), &recv_channel, FZ_FIFO, FZ_RECEIVER_MODE, NULL)){
        fz_log(FZ_ERROR, "%s: Failed to initialize file zap reciever channel", __func__);
        RETURN_DEFER(1);
    }
    fz_log(FZ_INFO, "Receiver channel initialized successfully");
    if (!fz_receive_file(&context, &recv_fz, &recv_channel)){
        fz_log(FZ_ERROR, "%s: Error occurred while receiving file", __func__);
        RETURN_DEFER(1);
    }
    fz_log(FZ_INFO, "File retrieved successfully");

    /* Clean up */
    defer:
        fz_channel_destroy(&recv_channel);
        fz_ctx_destroy(&recv_fz);
        // arena_allocator_deinit(&gpa);
        context_deinit(&context);
        return result;
}