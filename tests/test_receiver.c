#include <stdio.h>


#define XXH_STATIC_LINKING_ONLY
#define XXH_IMPLEMENTATION
#define STB_DS_IMPLEMENTATION
#include "core.h"

/* global settings */
int glob_CHUNKING_STRATEGY;



int main(int argc, char *argv[]){
    (void)argc;
    (void)argv;

    glob_CHUNKING_STRATEGY = FZ_FIXED_SIZED_CHUNK;

    /* Receiver device */
    fz_ctx_t recv_fz = {0};
    int result = 0;
    fz_channel_t recv_channel = {0};

    if (!fz_ctx_init(&recv_fz, glob_CHUNKING_STRATEGY, "dtmp/", "examples/dest/", "filezap.db", NULL, NULL)){
        fz_log(FZ_ERROR, "%s: Failed to initialize file zap reciever context", __func__);
        RETURN_DEFER(1);
    }
    fz_log(FZ_INFO, "Receiver context initialized successfully");

    if (!fz_channel_init(&recv_channel, FZ_FIFO, FZ_RECEIVER_MODE)){
        fz_log(FZ_ERROR, "%s: Failed to initialize file zap reciever channel", __func__);
        RETURN_DEFER(1);
    }
    fz_log(FZ_INFO, "Receiver channel initialized successfully");
    if (!fz_receive_file(&recv_fz, &recv_channel)){
        fz_log(FZ_ERROR, "%s: Error occurred while receiving file", __func__);
        RETURN_DEFER(1);
    }
    fz_log(FZ_INFO, "File retrieved successfully");

    /* Clean up */
    defer:
        fz_channel_destroy(&recv_channel);
        fz_ctx_destroy(&recv_fz);
        return result;
}