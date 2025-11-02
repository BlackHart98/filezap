#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#define XXH_STATIC_LINKING_ONLY // expose unstable API
#define XXH_IMPLEMENTATION
#include "../hash/xxhash.h"
// Hashes argv[2] using the entropy from argv[1].
int main(int argc, char* argv[]){
    char secret[XXH3_SECRET_SIZE_MIN];
    if (argc != 3) return 1; 
    printf("Hey yo %s\n", argv[1]);
    XXH3_generateSecret(secret, sizeof(secret), argv[1], strlen(argv[1]));
    XXH64_hash_t h = XXH3_64bits_withSecret(
        argv[2], strlen(argv[2]),
        secret, sizeof(secret)
    );
    printf("%016llx\n", (unsigned long long) h);
}