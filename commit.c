// commit.c — Commit creation and history traversal
#include "commit.h"
#include "index.h"
#include "tree.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>

int object_write(int type, const void *data, size_t len, ObjectID *id_out);
int object_read(const ObjectID *id, int *type_out, void **data_out, size_t *len_out);
#define OBJ_COMMIT 3

// ... [Keep the PROVIDED functions exactly the same as Commit 1] ...
// (commit_parse, commit_serialize, commit_walk, head_read, head_update)

int commit_create(const char *message, ObjectID *commit_id_out) {
    Commit commit;
    memset(&commit, 0, sizeof(Commit));

    ObjectID tree_id;
    if (tree_from_index(&tree_id) != 0) {
        fprintf(stderr, "error: nothing to commit\n");
        return -1;
    }
    commit.tree = tree_id;

    // 2. Read the current HEAD to set as parent (if it exists)
    ObjectID parent_id;
    if (head_read(&parent_id) == 0) {
        commit.has_parent = 1;
        commit.parent = parent_id;
    } else {
        // First commit in the repository has no parent
        commit.has_parent = 0;
    }

    strncpy(commit.author, pes_author(), sizeof(commit.author) - 1);
    commit.timestamp = (uint64_t)time(NULL);

    strncpy(commit.message, message, sizeof(commit.message) - 1);
    commit.message[sizeof(commit.message) - 1] = '\0';

    // 4. Serialize the commit struct to text buffer
    void *commit_data;
    size_t commit_len;
    if (commit_serialize(&commit, &commit_data, &commit_len) != 0) {
        return -1;
    }

    // TODO: Write object and update HEAD
    free(commit_data);
    (void)commit_id_out;
    return -1;
}
