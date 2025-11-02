#include <stdio.h>

#define XXH_STATIC_LINKING_ONLY
#define XXH_IMPLEMENTATION
#define STB_DS_IMPLEMENTATION
#include "core.h"

/* global settings */
int glob_CHUNKING_STRATEGY;



int main(int argc, char *argv[]){
    if (2 != argc){
        fz_log(FZ_INFO, "Usage #prog <file> ");
        return 1;
    }

    glob_CHUNKING_STRATEGY = FZ_FIXED_SIZED_CHUNK;
    const char *input_file = argv[1];
    // const char *input_file = "examples/src/Free Nationals - Beauty & Essex (feat. Daniel Caesar & Unknown Mortal Orchestra)(1).mp4";

    /* Sender device */
    fz_ctx_t snd_fz = {0};
    fz_channel_t snd_channel = {0};
    int result = 0;

    if (!fz_ctx_init(&snd_fz, (fz_ctx_desc_t)&snd_fz, glob_CHUNKING_STRATEGY, "tmp/", "examples/src/", "filezap.db", NULL, NULL)){
        fz_log(FZ_ERROR, "%s: Failed to initialize file zap sender context", __func__);
        RETURN_DEFER(1);
    }
    fz_log(FZ_INFO, "Sender context initialized successfully");

    if (!fz_channel_init(&snd_channel, FZ_FIFO, FZ_SENDER_MODE)){
        fz_log(FZ_ERROR, "%s: Failed to initialize file zap sender channel", __func__);
        RETURN_DEFER(1);
    }
    fz_log(FZ_INFO, "Sender context channel successfully");
    if (!fz_send_file(&snd_fz, &snd_channel, input_file)){
        fz_log(FZ_ERROR, "%s: Error occurred while sending file", __func__);
        RETURN_DEFER(1);
    }
    fz_log(FZ_INFO, "File sent successfully");
    /* Clean up */
    defer:
        fz_channel_destroy(&snd_channel);
        fz_ctx_destroy(&snd_fz);
        return result;
}