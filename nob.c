#define NOB_IMPLEMENTATION
#define NOB_FETCH_IMPLEMENTATION
#define NOB_WARNING_DEPRECATED

#include "nob_fetch.h"

#define SRC_PATH "src/"
#define BUILD_PATH "build/"
#define TEST_PATH "tests/"


int main(int argc, char *argv[]){
    NOB_GO_REBUILD_URSELF_PLUS(argc, argv, "nob.h", "nob_fetch.h");

    nob_mkdir_if_not_exists(BUILD_PATH);

    Nob_Cmd cmd = {0};
    struct{
        const char *src_file;
        const char *target_file;
    } objects [] = {
        {.src_file = "core/core.c", .target_file = BUILD_PATH"core.o"},
        {.src_file = "core/chunking.c", .target_file = BUILD_PATH"chunking.o"},
        {.src_file = "core/retrieval.c", .target_file = BUILD_PATH"retrieval.o"},
        {.src_file = "core/sndr_recv.c", .target_file = BUILD_PATH"sndr_recv.o"},
    };

    for (int i = 0; i < NOB_ARRAY_LEN(objects); i++){
        nob_cc(&cmd);
        nob_cc_flags(&cmd);
        nob_cc_add_include(&cmd, "core");
        nob_cc_add_include(&cmd, "externals");
        nob_cmd_append(&cmd, "-c", "-O3");
        nob_cc_inputs(&cmd, objects[i].src_file);
        nob_cc_output(&cmd, objects[i].target_file);
        if (!nob_cmd_run(&cmd)) return 1;
    }


    nob_log(NOB_INFO, "--- Building tests ---");
    struct{
        const char *src_file;
        const char *target_file;
    } tests [] = {
        // {.src_file = TEST_PATH"test_filezap_v1.c", .target_file = BUILD_PATH"test_filezap_v1"},
        {.src_file = TEST_PATH"test_sender.c", .target_file = BUILD_PATH"test_sender"},
        // {.src_file = TEST_PATH"test_sender_arb.c", .target_file = BUILD_PATH"test_sender_arb"},
        {.src_file = TEST_PATH"test_receiver.c", .target_file = BUILD_PATH"test_receiver"},
        // {.src_file = TEST_PATH"test_xxhash.c", .target_file = BUILD_PATH"test_xxhash"},
    };
    for (int i = 0; i < NOB_ARRAY_LEN(tests); i++){
        nob_cc(&cmd);
        nob_cc_flags(&cmd);
        nob_cc_add_include(&cmd, "core");
        nob_cc_add_include(&cmd, "externals");
        for (int j = 0; j < NOB_ARRAY_LEN(objects); j++){
            nob_cmd_append(&cmd, objects[j].target_file);
        }
        nob_cmd_append(&cmd, "-fsanitize=address");
        nob_cmd_append(&cmd, "-lpthread");
        nob_cmd_append(&cmd, "-lsqlite3");
        nob_cc_inputs(&cmd, tests[i].src_file);
        nob_cc_output(&cmd, tests[i].target_file);
        if (!nob_cmd_run(&cmd)) return 1;
    }
    nob_log(NOB_INFO, "--- Tests build successful ---");

    nob_cc(&cmd);
    nob_cc_flags(&cmd);
    nob_cc_add_include(&cmd, "core");
    nob_cc_add_include(&cmd, "externals");
    for (int i = 0; i < NOB_ARRAY_LEN(objects); i++){
        nob_cmd_append(&cmd, objects[i].target_file);
    }
    nob_cmd_append(&cmd, "-fsanitize=address");
    nob_cmd_append(&cmd, "-lpthread");
    nob_cmd_append(&cmd, "-lsqlite3");
    // nob_cmd_append(&cmd, "-g"); 
    // nob_cmd_append(&cmd, "-O3");
    nob_cc_inputs(&cmd, SRC_PATH"main.c");
    nob_cc_output(&cmd, BUILD_PATH"filezap");
    if (!nob_cmd_run(&cmd)) return 1;
    nob_log(NOB_INFO, "--- Build successful ---");
    return 0;
}