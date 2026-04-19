// object.c — Content-addressable object store
//
// Every piece of data (file contents, directory listings, commits) is stored
// as an "object" named by its SHA-256 hash. Objects are stored under
// .pes/objects/XX/YYYYYY... where XX is the first two hex characters of the
// hash (directory sharding).
//
// PROVIDED functions: compute_hash, object_path, object_exists, hash_to_hex, hex_to_hash
// TODO functions:     object_write, object_read

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

// Get the filesystem path where an object should be stored.
// Format: .pes/objects/XX/YYYYYYYY...
// The first 2 hex chars form the shard directory; the rest is the filename.
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

// ─── TODO: Implement these ──────────────────────────────────────────────────

static const char *type_to_string(ObjectType type) {
    switch (type) {
        case OBJ_BLOB:   return "blob";
        case OBJ_TREE:   return "tree";
        case OBJ_COMMIT: return "commit";
        default:         return NULL;
    }
}

static int string_to_type(const char *s, size_t len, ObjectType *type_out) {
    if (len == 4 && strncmp(s, "blob", 4) == 0) {
        *type_out = OBJ_BLOB;
        return 0;
    }
    if (len == 4 && strncmp(s, "tree", 4) == 0) {
        *type_out = OBJ_TREE;
        return 0;
    }
    if (len == 6 && strncmp(s, "commit", 6) == 0) {
        *type_out = OBJ_COMMIT;
        return 0;
    }
    return -1;
}

// Returns 0 on success, -1 on error.
int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out) {
    const char *type_str = type_to_string(type);
    if (!type_str) return -1;

    // 1. Build full object: "<type> <size>\0" + data
    char header[64];
    int header_len = snprintf(header, sizeof(header), "%s %zu", type_str, len);
    if (header_len < 0 || (size_t)header_len >= sizeof(header)) {
        return -1;
    }
    header_len += 1; // include terminating '\0'

    size_t obj_size = (size_t)header_len + len;
    unsigned char *obj_buf = malloc(obj_size);
    if (!obj_buf) return -1;

    memcpy(obj_buf, header, (size_t)header_len);
    if (len > 0 && data != NULL) {
        memcpy(obj_buf + header_len, data, len);
    }

    // 2. Compute SHA-256 hash of full object
    compute_hash(obj_buf, obj_size, id_out);

    // 3. Check if object already exists
    if (object_exists(id_out)) {
        free(obj_buf);
        return 0;
    }

    // Build final path and shard dir
    char final_path[512];
    object_path(id_out, final_path, sizeof(final_path));

    // Extract shard directory: OBJECTS_DIR/XX
    char shard_dir[512];
    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(id_out, hex);
    snprintf(shard_dir, sizeof(shard_dir), "%s/%.2s", OBJECTS_DIR, hex);

    // 4. Create shard directory if it doesn't exist
    if (mkdir(shard_dir, 0755) < 0 && errno != EEXIST) {
        free(obj_buf);
        return -1;
    }

    // 5. Write to temporary file in same shard directory
    char tmp_path[512];
    snprintf(tmp_path, sizeof(tmp_path), "%s/.tmp-%s", shard_dir, hex + 2);

    int fd = open(tmp_path, O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (fd < 0) {
        free(obj_buf);
        return -1;
    }

    ssize_t written = 0;
    size_t remaining = obj_size;
    unsigned char *p = obj_buf;
    while (remaining > 0) {
        written = write(fd, p, remaining);
        if (written < 0) {
            close(fd);
            unlink(tmp_path);
            free(obj_buf);
            return -1;
        }
        remaining -= (size_t)written;
        p += written;
    }

    // 6. fsync temp file
    if (fsync(fd) < 0) {
        close(fd);
        unlink(tmp_path);
        free(obj_buf);
        return -1;
    }

    if (close(fd) < 0) {
        unlink(tmp_path);
        free(obj_buf);
        return -1;
    }

    // 7. rename temp file to final path (atomic)
    if (rename(tmp_path, final_path) < 0) {
        unlink(tmp_path);
        free(obj_buf);
        return -1;
    }

    // 8. Open and fsync shard directory to persist rename
    int dfd = open(shard_dir, O_DIRECTORY | O_RDONLY);
    if (dfd >= 0) {
        fsync(dfd);
        close(dfd);
    }

    // 9. id_out already set
    free(obj_buf);
    return 0;
}

// Read an object from the store.
// The caller is responsible for calling free(*data_out).
// Returns 0 on success, -1 on error (file not found, corrupt, etc.).
int object_read(const ObjectID *id, ObjectType *type_out, void **data_out, size_t *len_out) {
    char path[512];
    object_path(id, path, sizeof(path));

    FILE *f = fopen(path, "rb");
    if (!f) return -1;

    // 2. Read entire file
    if (fseek(f, 0, SEEK_END) != 0) {
        fclose(f);
        return -1;
    }
    long file_size_long = ftell(f);
    if (file_size_long < 0) {
        fclose(f);
        return -1;
    }
    size_t file_size = (size_t)file_size_long;
    if (fseek(f, 0, SEEK_SET) != 0) {
        fclose(f);
        return -1;
    }

    unsigned char *buf = malloc(file_size);
    if (!buf) {
        fclose(f);
        return -1;
    }

    size_t read_n = fread(buf, 1, file_size, f);
    fclose(f);
    if (read_n != file_size) {
        free(buf);
        return -1;
    }

    // 4. Verify integrity: recompute SHA-256 of file contents
    ObjectID computed;
    compute_hash(buf, file_size, &computed);
    if (memcmp(computed.hash, id->hash, HASH_SIZE) != 0) {
        free(buf);
        return -1;
    }

    // 3. Parse header "<type> <size>\0"
    // Find the '\0' that terminates header
    unsigned char *nul = memchr(buf, '\0', file_size);
    if (!nul) {
        free(buf);
        return -1;
    }

    size_t header_len = (size_t)(nul - buf); // excludes '\0'
    char *header = (char *)buf;

    // Find space between type and size
    char *space = memchr(header, ' ', header_len);
    if (!space) {
        free(buf);
        return -1;
    }

    size_t type_len = (size_t)(space - header);
    if (string_to_type(header, type_len, type_out) < 0) {
        free(buf);
        return -1;
    }

    char *size_str = space + 1;
    size_t size_str_len = header_len - (size_t)(size_str - header);
    // Null-terminate size string temporarily
    char saved = size_str[size_str_len];
    size_str[size_str_len] = '\0';
    char *endptr = NULL;
    unsigned long parsed_size = strtoul(size_str, &endptr, 10);
    size_str[size_str_len] = saved;
    if (endptr == size_str || parsed_size > SIZE_MAX) {
        free(buf);
        return -1;
    }

    size_t data_len = (size_t)parsed_size;
    size_t expected_total = (size_t)(header_len + 1) + data_len;
    if (expected_total != file_size) {
        free(buf);
        return -1;
    }

    // 6. Allocate buffer and return data portion
    unsigned char *data_start = buf + header_len + 1;
    void *out = malloc(data_len);
    if (!out) {
        free(buf);
        return -1;
    }
    memcpy(out, data_start, data_len);

    free(buf);

    *data_out = out;
    *len_out  = data_len;
    return 0;

}

