# Hmmm...

- Ok, right now I have found out that one of the obvious bottleneck is the hashing algorithm, I will have to look for cheaper alternatives that does not sacrifce likelihood of collisions(I cannot afford any collision), I think it will improve the performance
- If XXHash solves my problem, I don't think I will need to cset the hash algorithm it's pointless
- Now I need to rationalize the database for storing thechunk metadata
- Keep the chunk in the table as a list of chunks separated by a hyphen
- I think the next thing will be to figure out a way to transfer the chunks with limited physical copies, by using a prefetch method. In this approach the manifest information is sent over to the receiver then based of the available chuck it sends a request for a limited list of chunks from the sender then if any of the steps fail the whole step should fail
- Now that I have been able to establish the interprocess communication, I will go ahead with the other stuff I intend to do, e.g., finding chunk in list of files from cutpoint
- Now I want to add the SQLite part
- I need to commit the result from the file transfer into sqlite database
- I also need to fix the checksum issue in the SQLite table
-  I need to write the defensive part to avoid re-download files that already exists
-  The query is trash I need to revisit it, also I have to make sure there is no instance of double free, use after free or leaks
- I need to handle the unnecessary overwrite due to file name, instead I will use a generate temporary name during the transfer and reconstruction phase, then as soon as that is done I will overwrite the file with the actual target/destination file path
- Querying database failure shouldn't make the entire transfer process fail neither should cache failure, failure should only occur when there's issue trying to retrieve file from sender
- I will have to look at the committing to chunk table code to ensure no duplicate data entry
```sql
BEGIN TRANSACTION;
CREATE TEMP TABLE temp_filezap_chunks (
    chunk_checksum INTEGER NOT NULL,
    cutpoint INTEGER NOT NULL,
    chunk_size INTEGER NOT NULL,
    file_path TEXT NOT NULL
);

INSERT INTO temp_filezap_chunks (chunk_checksum, cutpoint, chunk_size, file_path) VALUES (?, ?, ?, ?);

CREATE TEMP TABLE unique_filezap_chunks (chunk_checksum, cutpoint, chunk_size, file_path) AS 
SELECT t.chunk_checksum, t.cutpoint, t.chunk_size, t.file_path FROM temp_filezap_chunks AS t
EXCEPT
SELECT f.chunk_checksum, f.cutpoint, f.chunk_size, f.file_path FROM filezap_chunks AS f;

INSERT INTO filezap_chunks (chunk_checksum, cutpoint, chunk_size, file_path)
SELECT u.chunk_checksum, u.cutpoint, u.chunk_size, u.file_path FROM filezap_chunks AS u;
COMMIT;
```
- How do I make multiple sending and receive happen simultaneously? I am consider something that would invlove a queue a requested chunk queue and a received chunk queue and the worker on each end would query the queues to find out it there's any job to be done
- How do I solve the main problem, which is:
```
Let x[i] be a file in a list of files x[1], x[2], x[3], ..., x[n], there exists a chunk algorithm f (x[i]) that maps x[i] to a set of pair of chunk cutpoints c[j] and chunk size s[j] like so; {(c[1], s[1]), (c[2], s[2]), ..., (c[m], s[m])} such that:
sizeof(x[i]) >> sizeof(f(x[i])) (simply means the size of the file x[i] should be "substantially" smaller than the sum of the chunk file size, basically remove duplication)
and inverse(f)(f(x[i])) = x[j]; iff x[i] = x[j]

where inverse(f) is the reconstruction function

```
> I feel like this should exist, even if it is traditional compression, my use case might apply it in a novel way hopefully
> Is it even possible to reduce duplication if the chunk is only taking as variable the current file, hmmm...I will do my research to figure it out
- So I will spend most of my time on `tests/test_chunk_dedup.c` to make it less than the actual file size currently it is larger than the actual file by ~5kb, let's see what I will be able to come up with
- Ah ah, I found something pretty close to what I trying to work on Low Bandwidth Network File System
- I put the chunking step on hold to add the garbage collector to remove unused chunks in the metadata/blob store
- Implement socket channel communication, this will make it on less problem to worry about. I can also take that opportunity to learn about socket programming and use it for something practical
- I  will come back to figure out what is going on witht the socket channel, I have figured out that it is a race condition that's partly because I don't really know what I am doing yet
- Intermission, I have to reduce the amount of intermittent malloc, also fix ownership inconsistencies within all objects
- I fixed the race condition issue with the TCP channel, I have to still work on the ownership model to call it a day
- This lingering bomb has been sorted out
```c
/* Potential buffer overflow */
static int get_filename(const char *file_path, char **file_name){
    int result = 1;
    size_t file_path_len = strlen(file_path);
    int count = 0;
    for (int i = file_path_len; i >= 0; i--){
        if ('/' == file_path[i]) {
            break;
        } else {
            count += 1;
        }
    }

    char *buffer = calloc((size_t)count, sizeof(char));
    if (NULL == buffer) RETURN_DEFER(0);
    memcpy(buffer, (char *)(file_path + (uintptr_t)(file_path_len - count + 1)), count);
    *file_name = buffer;
    defer:
        if (!result && NULL != buffer) {free(buffer); buffer = NULL;}
        return result;
}
```
with the allocator
```c
static int get_filename_v2(arena_allocator_t *allocator, const char *file_path, char **file_name){
    int result = 1;
    string_t str = string_lib_init_slice(allocator, make_const_slice(file_path));
    if (NULL == str.ptr) RETURN_DEFER(0);

    array_list_t list = string_lib_split_string(allocator, &str, make_const_slice("/"));
    slice_t *ptr = (slice_t *)list.ptr;
    slice_t last_item = ptr[list.len - 1];
    int ret = string_lib_slice_to_cstring(allocator, last_item, file_name);
    if (0 != ret) RETURN_DEFER(0);
    defer:
        return result;
}
```
- There is a newly introduced bug fromread and writeing through both TCP and Pipe channel, I don't even know what to do to mitigate this mess
- The issue seems to strictly come from the Pipe, but the TCP has a different issue, the issue is the last chunk has extra padding to it
- The padding was from to manifest population, I have fixed that