/*
 * DarwinFUSE — dynamic inode/filehandle table
 *
 * Maps between inode numbers (uint64_t) and FUSE paths (strings).
 * Thread-safe via mutex. Inodes are monotonically increasing from 2
 * (root = 1), never reused.
 *
 * Copyright (c) 2026 Marcel Cotta. All rights reserved.
 * Licensed under the MIT License.
 */

#ifndef DARWINFUSE_INODE_TABLE_H
#define DARWINFUSE_INODE_TABLE_H

#include <stdint.h>
#include <stddef.h>
#include <pthread.h>

/* Inode number type */
typedef uint64_t dfuse_ino_t;

/* Root inode is always 1 */
#define DFUSE_INO_ROOT  1

/* Inode entry types */
typedef enum {
    DFUSE_INO_REGULAR = 0,    /* normal file or directory */
    DFUSE_INO_ATTRDIR,        /* named attribute directory (virtual) */
    DFUSE_INO_NAMEDATTR       /* individual named attribute (virtual) */
} dfuse_ino_type_t;

/* Hash table entry (separate chaining) */
typedef struct dfuse_ino_entry {
    dfuse_ino_t               ino;
    char                     *path;           /* heap-allocated */
    dfuse_ino_type_t          type;           /* entry type (default REGULAR) */
    dfuse_ino_t               parent_ino;     /* ATTRDIR/NAMEDATTR: owning file's ino */
    char                     *attr_name;      /* NAMEDATTR: xattr name (heap-allocated) */
    struct dfuse_ino_entry   *next_by_path;   /* chain in path→ino map */
    struct dfuse_ino_entry   *next_by_ino;    /* chain in ino→path map */
} dfuse_ino_entry_t;

/* Inode table */
typedef struct dfuse_inode_table_s {
    /* path → entry hash map */
    dfuse_ino_entry_t **by_path;
    size_t              path_cap;

    /* ino → entry hash map */
    dfuse_ino_entry_t **by_ino;
    size_t              ino_cap;

    size_t              count;        /* total entries */
    dfuse_ino_t         next_ino;     /* next inode to allocate (starts at 2) */
    pthread_mutex_t     lock;
} dfuse_inode_table_t;

/*
 * Create a new inode table. Root "/" is pre-inserted as inode 1.
 * Returns NULL on allocation failure.
 */
dfuse_inode_table_t *dfuse_itable_create(void);

/*
 * Destroy the inode table and free all entries.
 */
void dfuse_itable_destroy(dfuse_inode_table_t *tbl);

/*
 * Look up or create an inode for the given path.
 * Returns the inode number, or 0 on allocation failure.
 */
dfuse_ino_t dfuse_itable_get_or_create(dfuse_inode_table_t *tbl, const char *path);

/*
 * Look up the inode for a path without creating.
 * Returns 0 if the path is not in the table.
 */
dfuse_ino_t dfuse_itable_lookup(dfuse_inode_table_t *tbl, const char *path);

/*
 * Look up the path for an inode.
 * Returns NULL if the inode is not in the table.
 * The returned pointer is valid until the entry is removed or renamed.
 * Caller must hold no expectation of lifetime beyond the next mutation.
 */
const char *dfuse_itable_path(dfuse_inode_table_t *tbl, dfuse_ino_t ino);

/*
 * Remove an entry by path (after unlink/rmdir).
 * The inode number is never reused.
 */
void dfuse_itable_remove(dfuse_inode_table_t *tbl, const char *path);

/*
 * Rename: update the path associated with an inode.
 */
void dfuse_itable_rename(dfuse_inode_table_t *tbl,
                          const char *old_path, const char *new_path);

/* ---- Named attribute (xattr) inode support ---- */

/*
 * Get the type of an inode entry.
 */
dfuse_ino_type_t dfuse_itable_type(dfuse_inode_table_t *tbl, dfuse_ino_t ino);

/*
 * Get or create an attribute directory inode for a file.
 * Returns the attrdir inode, or 0 on failure.
 */
dfuse_ino_t dfuse_itable_get_attrdir(dfuse_inode_table_t *tbl,
                                      dfuse_ino_t file_ino);

/*
 * Get or create a named attribute inode within an attrdir.
 * Returns the namedattr inode, or 0 on failure.
 */
dfuse_ino_t dfuse_itable_get_namedattr(dfuse_inode_table_t *tbl,
                                        dfuse_ino_t attrdir_ino,
                                        const char *name);

/*
 * Get the parent (file) inode of an attrdir or namedattr.
 * Returns 0 if the inode is not an attrdir/namedattr.
 */
dfuse_ino_t dfuse_itable_parent_ino(dfuse_inode_table_t *tbl, dfuse_ino_t ino);

/*
 * Get the xattr name for a namedattr inode.
 * Returns NULL if the inode is not a namedattr.
 */
const char *dfuse_itable_attr_name(dfuse_inode_table_t *tbl, dfuse_ino_t ino);

/* ---- Thread-safe path duplication ---- */

/*
 * Look up the path for an inode and return a heap-allocated copy.
 * The copy is made while holding the table lock, so it is safe
 * against concurrent remove/rename operations.
 * Returns NULL if the inode is not in the table.
 * Caller must free() the returned string.
 */
char *dfuse_itable_path_dup(dfuse_inode_table_t *tbl, dfuse_ino_t ino);

/*
 * Get the xattr name for a namedattr inode (heap-allocated copy).
 * Caller must free() the returned string.
 */
char *dfuse_itable_attr_name_dup(dfuse_inode_table_t *tbl, dfuse_ino_t ino);

#endif /* DARWINFUSE_INODE_TABLE_H */
