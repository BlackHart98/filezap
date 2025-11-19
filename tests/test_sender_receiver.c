#include <stdio.h>

#if !defined(_WIN32)
    #include <unistd.h>
    #include <fcntl.h>
#endif

#define XXH_STATIC_LINKING_ONLY
#define XXH_IMPLEMENTATION
#define STB_DS_IMPLEMENTATION
#include "core.h"


int main(int argc, char *argv[]){
    (void)argc;
    (void)argv;

    const char *input_file = "examples/src/Free Nationals - Beauty & Essex (feat. Daniel Caesar & Unknown Mortal Orchestra)(1).mp4";
    fz_ctx_t snd_fz = {0}, recv_fz = {0};
    fz_channel_t snd_channel = {0};
    fz_channel_t recv_channel = {0};
    int result = 0;

    pid_t child_process = fork(); 
    if (-1 == child_process){
        fz_log(FZ_ERROR, "Failed to create child process");
        RETURN_DEFER(1);
    } else if (0 == child_process){
        if (!fz_ctx_init(&snd_fz, FZ_FIXED_SIZED_CHUNK, "tmp/", "examples/src/", "filezap.db", NULL, NULL)){
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
    } else {
        if (!fz_ctx_init(&recv_fz, FZ_FIXED_SIZED_CHUNK, "dtmp/", "examples/dest/", "filezap.db", NULL, NULL)){
            fz_log(FZ_ERROR, "%s: Failed to initialize file zap reciever context", __func__);
            RETURN_DEFER(1);
        }
        fz_log(FZ_INFO, "Receiver context initialized successfully");
        if (!fz_channel_init(&recv_channel, FZ_FIFO, FZ_RECEIVER_MODE)){
            fz_log(FZ_ERROR, "%s: Failed to initialize file zap sender channel", __func__);
            RETURN_DEFER(1);
        }
        fz_log(FZ_INFO, "Receiver context channel successfully");
        if (!fz_receive_file(&recv_fz, &recv_channel)){
            fz_log(FZ_ERROR, "%s: Error occurred while receiving file", __func__);
            RETURN_DEFER(1);
        }
        fz_log(FZ_INFO, "File retrieved successfully");
    }

    /* Clean up */
    defer:
        fz_ctx_destroy(&snd_fz); fz_channel_destroy(&snd_channel);
        fz_ctx_destroy(&recv_fz); fz_channel_destroy(&recv_channel);
        return result;

}