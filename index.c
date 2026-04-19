// index.c — Staging area implementation
#include "index.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>

// --- Prototypes to keep the compiler happy ---
int object_write(int type, const void *data, size_t len, ObjectID *id_out);
void hash_to_hex(const ObjectID *id, char *hex_out);
int hex_to_hash(const char *hex, ObjectID *id_out);

#define OBJ_BLOB 1 // Assumed from standard Git types

// ─── PROVIDED ────────────────────────────────────────────────────────────────

IndexEntry* index_find(Index *index, const char *path) {
    for (int i = 0; i < index->count; i++) {
        if (strcmp(index->entries[i].path, path) == 0)
            return &index->entries[i];
    }
    return NULL;
}

int index_remove(Index *index, const char *path) {
    for (int i = 0; i < index->count; i++) {
        if (strcmp(index->entries[i].path, path) == 0) {
            int remaining = index->count - i - 1;
            if (remaining > 0)
                memmove(&index->entries[i], &index->entries[i + 1],
                        remaining * sizeof(IndexEntry));
            index->count--;
            return index_save(index);
        }
    }
    fprintf(stderr, "error: '%s' is not in the index\n", path);
    return -1;
}

int index_status(const Index *index) {
    printf("Staged changes:\n");
    int staged_count = 0;
    for (int i = 0; i < index->count; i++) {
        printf("  staged:     %s\n", index->entries[i].path);
        staged_count++;
    }
    if (staged_count == 0) printf("  (nothing to show)\n");
    printf("\n");

    printf("Unstaged changes:\n");
    int unstaged_count = 0;
    for (int i = 0; i < index->count; i++) {
        struct stat st;
        if (stat(index->entries[i].path, &st) != 0) {
            printf("  deleted:    %s\n", index->entries[i].path);
            unstaged_count++;
        } else {
            if (st.st_mtime != (time_t)index->entries[i].mtime_sec || st.st_size != (off_t)index->entries[i].size) {
                printf("  modified:   %s\n", index->entries[i].path);
                unstaged_count++;
            }
        }
    }
    if (unstaged_count == 0) printf("  (nothing to show)\n");
    printf("\n");

    printf("Untracked files:\n");
    int untracked_count = 0;
    DIR *dir = opendir(".");
    if (dir) {
        struct dirent *ent;
        while ((ent = readdir(dir)) != NULL) {
            if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0) continue;
            if (strcmp(ent->d_name, ".pes") == 0) continue;
            if (strcmp(ent->d_name, "pes") == 0) continue; 
            if (strstr(ent->d_name, ".o") != NULL) continue; 

            int is_tracked = 0;
            for (int i = 0; i < index->count; i++) {
                if (strcmp(index->entries[i].path, ent->d_name) == 0) {
                    is_tracked = 1; 
                    break;
                }
            }
            
            if (!is_tracked) {
                struct stat st;
                stat(ent->d_name, &st);
                if (S_ISREG(st.st_mode)) { 
                    printf("  untracked:  %s\n", ent->d_name);
                    untracked_count++;
                }
            }
        }
        closedir(dir);
    }
    if (untracked_count == 0) printf("  (nothing to show)\n");
    printf("\n");

    return 0;
}

// ─── IMPLEMENTATION ─────────────────────────────────────────────────────────

int index_load(Index *index) {
    index->count = 0;
    
    // Attempt to open the index file. If it doesn't exist, we start empty.
    FILE *f = fopen(".pes/index", "r");
    if (!f) return 0; 

    char hex[65]; // 64 chars + null terminator
    unsigned long mtime;
    
    // Parse each line based on the exact format: <mode> <hash> <mtime> <size> <path>
    while (fscanf(f, "%o %64s %lu %u %[^\n]\n", 
                  &index->entries[index->count].mode,
                  hex,
                  &mtime,
                  &index->entries[index->count].size,
                  index->entries[index->count].path) == 5) {
        
        index->entries[index->count].mtime_sec = mtime;
        
        if (hex_to_hash(hex, &index->entries[index->count].hash) != 0) {
            fclose(f);
            return -1; // Parsing error
        }
        
        index->count++;
        if (index->count >= MAX_INDEX_ENTRIES) break; // Protect against overflow
    }
    
    fclose(f);
    return 0;
}

int index_save(const Index *index) {
    (void)index;
    return -1;
}

int index_add(Index *index, const char *path) {
    (void)index; (void)path;
    return -1;
}
