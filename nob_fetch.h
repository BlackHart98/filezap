/* nob_fetch - v0.1.0 - Public Domain https://github.com/BlackHart98/nob.h/tree/private/nob_fetch_extends
    This library in an extension of the nob library [NoBuild](https://github.com/tsoding/nobuild) to support
    remote fetch of C source files, it takes a build it yourself approach. This project is still largely experimental

    # How to using it
    ```c
        #define NOB_FETCH_IMPLEMENTATION
        #include "nob_fetch.h"

        int main(int argc, char **argv)
        {
            NOB_GO_REBUILD_URSELF(argc, argv);

            // This is a self-hosted manifest repository
            static Nob_Repository remote_repo = {
                .host = NOB_GITHUB_HOST,
                .owner = "blackhart98",
                .repository = "jafar_local",
            };

            Nob_Cmd cmd = {0};
            nob_add_package(&cmd, remote_repo, "repository_index", "0.1.0");
            if(!nob_install_packages(&cmd)) return 1;

            nob_cc(&cmd);
            nob_cc_add_include(&cmd, "packages/some_lib/"); // packages will be automatically generated
            nob_cc_inputs(&cmd, "src/main.c"); //
            nob_cc_output(&cmd, "build/main");
            if(!nob_cmd_run(&cmd)) return 1;

            return 0;
        }
    ```

    ```console
    $ cc -o nob nob.c
    $ ./nob
    ```
    # For more information of how the self-hosted manifest repository works visit https://github.com/BlackHart98/jafar__local

*/

#ifndef NOB_FETCH_
#define NOB_FETCH_
#define NOB_IMPLEMENTATION
#define NOB_WARNING_DEPRECATED
#include "nob.h"


#define GITHUB_CONTENT_URL "https://raw.githubusercontent.com/"
#define GITHUB_URL "https://github.com/"
#define NOB_REMOTE_DEPENDENCIES "packages/"
#define NOB_EXTERNAL_SOURCES "externals/"
#define NOB_HOME_DIR "HOME"
#define MAX_BUFFER_SIZE 200

#define nob_install_packages(cmd, ...) nob_cmd_run(cmd, __VA_ARGS__)


#define nob_add_package(cmd, remote_repo, package_name, version)\
    do {\
        if (!nob__add_package(cmd, remote_repo, package_name, version, NULL)){\
            nob_log(NOB_ERROR, "Unable to install package %s", package_name);\
        }\
    }while(0)


#define nob_add_package_plus(cmd, remote_repo, package_name, version, vault)\
    do {\
        if (!nob__add_package(cmd, remote_repo, package_name, version, vault)){\
            nob_log(NOB_ERROR, "Unable to install package %s", package_name);\
        }\
    }while(0)


// NOB_REMOTE_FETCH
#define Nob_curl_request Nob_cmd;

typedef enum NOB_REMOTE_HOST {
    NOB_GITHUB_HOST = 0x1,
} NOB_REMOTE_HOST;

typedef enum NOB_INI_PARSE_STATE{
    NOB_INI_EMPTY_STATE = 0,
    NOB_INI_PACKAGE_STATE = 1,
    NOB_INI_PACKAGE_NAME_STATE = (1 << 1),
    NOB_INI_PACKAGE_URL_STATE = (1 << 2),
} NOB_INI_PARSE_STATE;


typedef struct Nob_Repository {
    NOB_REMOTE_HOST host; // the host site of the repository for now I only support Github
    const char *owner; // the user id of the repository
    const char  *repository; // ther repository identifier/name
} Nob_Repository;

typedef struct Nob_package_version{
    const char url[MAX_BUFFER_SIZE + 1];
    const char version[MAX_BUFFER_SIZE + 1];
    const char checksum[MAX_BUFFER_SIZE + 1];
}Nob_package_version;

typedef struct Nob_manifest_meta{
    const char package_name[MAX_BUFFER_SIZE + 1];
    const char url[MAX_BUFFER_SIZE + 1];
    // Nob_package_version *package_versions;
} Nob_manifest_meta;


NOBDEF bool nob__add_package(Nob_Cmd *cmd, Nob_Repository repo, const char *package, const char *version, const char *vault);
NOBDEF bool nob__fetch_src_from_package(
    Nob_Cmd *cmd, 
    Nob_Repository repo, 
    const char *package, 
    const char *version, 
    const char *vault, 
    const char *src_file
    // size_t num_of_files
    );
NOBDEF bool nob_parse_manifest_file(Nob_manifest_meta *mnfst, const char *mnfst_path);
NOBDEF bool nob__fetch_src_file(Nob_Cmd *cmd, const char *url, const char *src_file);


#endif // NOB_FETCH_

#ifdef NOB_FETCH_IMPLEMENTATION

NOBDEF bool nob__add_package(Nob_Cmd *cmd, Nob_Repository repo, const char *package, const char *version, const char *vault)
{
    nob_mkdir_if_not_exists(NOB_REMOTE_DEPENDENCIES);
    NOB_ASSERT((NULL != package));
    Nob_String_Builder package_sb = {0};
    nob_sb_append_cstr(&package_sb, package);
    bool result = true;
#if !defined(_WIN32)
    char *manifest_dir = NULL;
    if (NULL == vault){
        const char *home_dir = getenv(NOB_HOME_DIR);
        if (NULL == home_dir) {
            nob_log(NOB_ERROR, "Could not use the default vault, HOME varible not set `%s`", manifest_dir);
            nob_return_defer(false);
        }
        if (package_sb.count <= 0) nob_return_defer(false);
        manifest_dir = nob_temp_sprintf("%s/%s", home_dir, repo.repository);
    } else{
        manifest_dir = nob_temp_sprintf("%s%s", vault, repo.repository);
    }
    char *git_full_url = nob_temp_sprintf("%s%s/%s.git", GITHUB_URL, repo.owner, repo.repository);
    if (NOB_GITHUB_HOST == repo.host){
        if (!opendir(manifest_dir)) {
            nob_log(NOB_INFO, "Could not find Manifest repository `%s`", manifest_dir);
            nob_cmd_append(
                cmd, 
                "git", 
                "clone", 
                "--depth=1",
                "--silent",
                nob_temp_sprintf("%s%s/%s.git", GITHUB_URL, repo.owner, repo.repository),
                manifest_dir
            );
            if(!nob_cmd_run(cmd, .stderr_path = "/dev/null")) nob_return_defer(false);
        }
        Nob_manifest_meta mnfst = {0};
        const char *temp_ = nob_temp_sv_to_cstr(nob_sv_from_parts(package_sb.items, 1));
        if(!nob_parse_manifest_file(
            &mnfst, 
            nob_temp_sprintf("%s/%s%s/%s/%s.ini", manifest_dir, NOB_REMOTE_DEPENDENCIES, temp_, package, package))) nob_return_defer(false);
        nob_cmd_append(
            cmd, 
            "git", 
            "clone", 
            "--depth=1",
            "--silent",
            nob_temp_sprintf("%s", mnfst.url),
            nob_temp_sprintf("%s%s", NOB_REMOTE_DEPENDENCIES, mnfst.package_name));
    }
#endif
    defer:
        nob_temp_reset();
        nob_sb_free(package_sb);
        return result;
}

// this function checks fetches file
NOBDEF bool nob__fetch_src_file(Nob_Cmd *cmd, const char *url, const char *src_file)
{
    nob_mkdir_if_not_exists(NOB_EXTERNAL_SOURCES);
    NOB_ASSERT((NULL != url)&&(NULL != src_file));
    bool result = false;
#if !defined(_WIN32)
    nob_cmd_append(cmd, "curl", "-L", url, "--quiet" , "-o", nob_temp_sprintf(NOB_EXTERNAL_SOURCES"%s", src_file));
    if (nob_cmd_run(cmd))
        nob_return_defer(true);
#endif
    defer:
        nob_temp_reset();
        return result;
}


// Todo: I might use a complete .ini parser
NOBDEF bool nob_parse_manifest_file(Nob_manifest_meta *mnfst, const char *mnfst_path)
{
    nob_log(NOB_INFO, "Opening %s", mnfst_path);
    bool result = false;
    FILE *f = fopen(mnfst_path, "r");

    if (NULL == f) nob_return_defer(false);

    size_t line_no = 1;
    char buffer[MAX_BUFFER_SIZE + 1] = {0}; // I will have to use the heap instead of the stack
    int state = NOB_INI_EMPTY_STATE;
    while (NULL != fgets(buffer, MAX_BUFFER_SIZE, f)){
        Nob_String_View token = nob_sv_trim(nob_sv_from_cstr(buffer));
        if (nob_sv_starts_with(token, nob_sv_from_cstr("#")) || nob_sv_starts_with(token, nob_sv_from_cstr("\n")) || nob_sv_starts_with(token, nob_sv_from_cstr(";"))){
            continue;
        }
        else if (NOB_INI_EMPTY_STATE == state){
            if(0 != memcmp(nob_temp_sv_to_cstr(token), "[package]", strlen("[package]"))){
                nob_log(NOB_ERROR, "Parse error at %s:%lu, expected `[package]` .", mnfst_path, line_no);
                nob_return_defer(false);
            } else {
                state = NOB_INI_PACKAGE_STATE;
            }
        }
        else if (0 != (NOB_INI_PACKAGE_STATE & state)){
            if (nob_sv_starts_with(token, nob_sv_from_cstr("name=")) && (0 == (state & NOB_INI_PACKAGE_NAME_STATE))){
                Nob_String_View temp_ = nob_sv_chop_left(&token, strlen("name="));
                memcpy((char *)mnfst->package_name, temp_.data, token.count);
                state |= NOB_INI_PACKAGE_NAME_STATE;
            } else if (nob_sv_starts_with(token, nob_sv_from_cstr("url=")) && (0 ==(state & NOB_INI_PACKAGE_URL_STATE) )){
                Nob_String_View temp_ = nob_sv_chop_left(&token, strlen("url="));
                memcpy((char *)mnfst->url, temp_.data, token.count);
                state |= NOB_INI_PACKAGE_URL_STATE;
            } else {
                nob_log(NOB_ERROR, "Parse error at %s:%lu", mnfst_path, line_no);
                nob_return_defer(false);
            }
        } else {
            nob_log(NOB_ERROR, "Parse error at %s:%lu", mnfst_path, line_no);
        }
        line_no++;
    }
    if ((NOB_INI_PACKAGE_NAME_STATE | NOB_INI_PACKAGE_URL_STATE) & state) nob_return_defer(true);
    defer:
        fclose(f);
        return result;
}
#endif // NOB_FETCH_IMPLEMENTATION


/*
   Revision history:
*/

/*
   Version Conventions:

      We are following https://semver.org/ so the version has a format MAJOR.MINOR.PATCH:
      - Modifying comments does not update the version.
      - PATCH is incremented in case of a bug fix or refactoring without touching the API.
      - MINOR is incremented when new functions and/or types are added in a way that does
        not break any existing user code. We want to do this in the majority of the situation.
        If we want to delete a certain function or type in favor of another one we should
        just add the new function/type and deprecate the old one in a backward compatible way
        and let them co-exist for a while.
      - MAJOR update should be just a periodic cleanup of the DEPRECATED functions and types
        without really modifying any existing functionality.
      - Breaking backward compatibility in a MINOR release should be considered a bug and
        should be promptly fixed in the next PATCH release.

   Naming Conventions:

      - All the user facing names should be prefixed with `nob_` or `NOB_` depending on the case.
      - The prefixes of non-redefinable names should be strippable with NOB_STRIP_PREFIX (unless
        explicitly stated otherwise like in case of nob_log).
      - Internal functions should be prefixed with `nob__` (double underscore).
*/

/*
   ------------------------------------------------------------------------------
   This software is available under 2 licenses -- choose whichever you prefer.
   ------------------------------------------------------------------------------
   ALTERNATIVE A - MIT License
   Copyright (c) 2025 Pius Arhanbhunde
   Permission is hereby granted, free of charge, to any person obtaining a copy of
   this software and associated documentation files (the "Software"), to deal in
   the Software without restriction, including without limitation the rights to
   use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
   of the Software, and to permit persons to whom the Software is furnished to do
   so, subject to the following conditions:
   The above copyright notice and this permission notice shall be included in all
   copies or substantial portions of the Software.
   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
   IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
   FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
   AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
   LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
   OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
   SOFTWARE.
   ------------------------------------------------------------------------------
   ALTERNATIVE B - Public Domain (www.unlicense.org)
   This is free and unencumbered software released into the public domain.
   Anyone is free to copy, modify, publish, use, compile, sell, or distribute this
   software, either in source code form or as a compiled binary, for any purpose,
   commercial or non-commercial, and by any means.
   In jurisdictions that recognize copyright laws, the author or authors of this
   software dedicate any and all copyright interest in the software to the public
   domain. We make this dedication for the benefit of the public at large and to
   the detriment of our heirs and successors. We intend this dedication to be an
   overt act of relinquishment in perpetuity of all present and future rights to
   this software under copyright law.
   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
   IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
   FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
   AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
   ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
   WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
   ------------------------------------------------------------------------------
*/