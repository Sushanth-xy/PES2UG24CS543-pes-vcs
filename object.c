// object.c — Content-addressable object store
//
// Every piece of data (file contents, directory listings, commits) is stored
// as an "object" named by its SHA-256 hash. Objects are stored under
// .pes/objects/XX/YYYYYY... where XX is the first two hex characters of the
// hash (directory sharding).

#include "pes.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <openssl/evp.h>

// ─── PROVIDED ────────────────────────────────────────────────────────────────

void hash_to_hex(const ObjectID *id, char *hex_out) {
    for (int i = 0; i < HASH_SIZE; i++) {
        sprintf(hex_out + i * 2, "%02x", id->hash[i]);
    }
    hex_out[HASH_HEX_SIZE] = '\0';
}

int hex_to_hash(const char *hex, ObjectID *id_out) {
    if (strlen(hex) < HASH_HEX_SIZE) return -1;
    for (int i = 0; i < HASH_SIZE; i++) {
        unsigned int byte;
        if (sscanf(hex + i * 2, "%2x", &byte) != 1) return -1;
        id_out->hash[i] = (uint8_t)byte;
    }
    return 0;
}

void compute_hash(const void *data, size_t len, ObjectID *id_out) {
    unsigned int hash_len;
    EVP_MD_CTX *ctx = EVP_MD_CTX_new();
    EVP_DigestInit_ex(ctx, EVP_sha256(), NULL);
    EVP_DigestUpdate(ctx, data, len);
    EVP_DigestFinal_ex(ctx, id_out->hash, &hash_len);
    EVP_MD_CTX_free(ctx);
}

void object_path(const ObjectID *id, char *path_out, size_t path_size) {
    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(id, hex);
    snprintf(path_out, path_size, "%s/%.2s/%s", OBJECTS_DIR, hex, hex + 2);
}

int object_exists(const ObjectID *id) {
    char path[512];
    object_path(id, path, sizeof(path));
    return access(path, F_OK) == 0;
}

// ─── IMPLEMENTATION ─────────────────────────────────────────────────────────

int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out) {
    const char *type_str;
    if (type == OBJ_BLOB) type_str = "blob";
    else if (type == OBJ_TREE) type_str = "tree";
    else if (type == OBJ_COMMIT) type_str = "commit";
    else return -1;

    // 1. Build the full object: header ("type size\0") + data
    char header[64];
    int header_len = snprintf(header, sizeof(header), "%s %zu", type_str, len);
    if (header_len < 0 || header_len >= (int)sizeof(header)) return -1;

    size_t full_len = header_len + 1 + len;
    void *full_data = malloc(full_len);
    if (!full_data) return -1;

    memcpy(full_data, header, header_len);
    ((char *)full_data)[header_len] = '\0';
    memcpy((char *)full_data + header_len + 1, data, len);

    // 2. Compute SHA-256 hash of the FULL object
    compute_hash(full_data, full_len, id_out);

    // 3. Check if object already exists (deduplication)
    if (object_exists(id_out)) {
        free(full_data);
        return 0; // Success, already written
    }

    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(id_out, hex);

    // 4. Create shard directory (.pes/objects/XX/)
    char dir_path[512];
    snprintf(dir_path, sizeof(dir_path), "%s/%.2s", OBJECTS_DIR, hex);
    mkdir(dir_path, 0755);

    // 5. Write to a temporary file (Fix applied here to prevent GCC warning)
    char temp_path[512];
    snprintf(temp_path, sizeof(temp_path), "%s/%.2s/tmp_obj_XXXXXX", OBJECTS_DIR, hex);
    
    int fd = mkstemp(temp_path);
    if (fd < 0) { 
        free(full_data); 
        return -1; 
    }

    if (write(fd, full_data, full_len) != (ssize_t)full_len) {
        close(fd); 
        unlink(temp_path); 
        free(full_data); 
        return -1;
    }

    // 6. fsync() the temporary file
    fsync(fd);
    close(fd);

    // 7. rename() the temp file to the final path (atomic)
    char final_path[512];
    object_path(id_out, final_path, sizeof(final_path));
    if (rename(temp_path, final_path) != 0) {
        unlink(temp_path); 
        free(full_data); 
        return -1;
    }

    // 8. Open and fsync() the shard directory to persist the rename
    int dir_fd = open(dir_path, O_RDONLY);
    if (dir_fd >= 0) {
        fsync(dir_fd);
        close(dir_fd);
    }

    free(full_data);
    return 0; // Success
}

int object_read(const ObjectID *id, ObjectType *type_out, void **data_out, size_t *len_out) {
    char path[512];
    object_path(id, path, sizeof(path));

    // 1 & 2. Open and read the entire file
    FILE *f = fopen(path, "rb");
    if (!f) return -1;

    fseek(f, 0, SEEK_END);
    long file_size = ftell(f);
    fseek(f, 0, SEEK_SET);
    if (file_size < 0) { fclose(f); return -1; }

    void *full_data = malloc(file_size);
    if (!full_data) { fclose(f); return -1; }

    if (fread(full_data, 1, file_size, f) != (size_t)file_size) {
        free(full_data); fclose(f); return -1;
    }
    fclose(f);

    // 3. Parse the header to extract the type string and size
    char *null_byte = memchr(full_data, '\0', file_size);
    if (!null_byte) { free(full_data); return -1; }

    char type_str[16];
    size_t parsed_len;
    if (sscanf((char *)full_data, "%15s %zu", type_str, &parsed_len) != 2) {
        free(full_data); return -1;
    }

    // 4. Verify integrity
    ObjectID computed_id;
    compute_hash(full_data, file_size, &computed_id);
    if (memcmp(computed_id.hash, id->hash, HASH_SIZE) != 0) {
        free(full_data); return -1; // Hash mismatch!
    }

    // 5. Set type_out
    if (strcmp(type_str, "blob") == 0) *type_out = OBJ_BLOB;
    else if (strcmp(type_str, "tree") == 0) *type_out = OBJ_TREE;
    else if (strcmp(type_str, "commit") == 0) *type_out = OBJ_COMMIT;
    else { free(full_data); return -1; } // Unknown object type

    // 6. Allocate buffer for extracted data
    *data_out = malloc(parsed_len);
    if (!*data_out) { free(full_data); return -1; }

    memcpy(*data_out, null_byte + 1, parsed_len);
    *len_out = parsed_len;

    free(full_data);
    return 0; // Success
}
