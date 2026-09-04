/*
 * DarwinFUSE — dynamic inode/filehandle table
 *
 * Hash table mapping between inode numbers and FUSE paths.
 * Uses FNV-1a hashing with separate chaining.
 *
 * Copyright (c) 2026 Marcel Cotta. All rights reserved.
 * Licensed under the MIT License.
 */

#include "inode_table.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* ---- FNV-1a hash ---- */

#define FNV_OFFSET_BASIS  14695981039346656037ULL
#define FNV_PRIME         1099511628211ULL

static uint64_t fnv1a_string(const char *s)
{
    uint64_t h = FNV_OFFSET_BASIS;
    for (; *s; s++) {
        h ^= (uint64_t)(unsigned char)*s;
        h *= FNV_PRIME;
    }
    return h;
}

static uint64_t fnv1a_u64(uint64_t val)
{
    uint64_t h = FNV_OFFSET_BASIS;
    for (int i = 0; i < 8; i++) {
        h ^= (val >> (i * 8)) & 0xFF;
        h *= FNV_PRIME;
    }
    return h;
}

/* ---- Initial capacity and load factor ---- */

#define INITIAL_BUCKETS  256
#define LOAD_FACTOR_NUM  3   /* grow when count > cap * 3/4 */
#define LOAD_FACTOR_DEN  4

/* ---- Internal helpers ---- */

static void rehash_path_map(dfuse_inode_table_t *tbl)
{
    size_t new_cap = tbl->path_cap * 2;
    dfuse_ino_entry_t **new_buckets = calloc(new_cap, sizeof(*new_buckets));
    if (!new_buckets) return;  /* leave un-rehashed on OOM */

    for (size_t i = 0; i < tbl->path_cap; i++) {
        dfuse_ino_entry_t *e = tbl->by_path[i];
        while (e) {
            dfuse_ino_entry_t *next = e->next_by_path;
            size_t idx = fnv1a_string(e->path) % new_cap;
            e->next_by_path = new_buckets[idx];
            new_buckets[idx] = e;
            e = next;
        }
    }
    free(tbl->by_path);
    tbl->by_path = new_buckets;
    tbl->path_cap = new_cap;
}

static void rehash_ino_map(dfuse_inode_table_t *tbl)
{
    size_t new_cap = tbl->ino_cap * 2;
    dfuse_ino_entry_t **new_buckets = calloc(new_cap, sizeof(*new_buckets));
    if (!new_buckets) return;

    for (size_t i = 0; i < tbl->ino_cap; i++) {
        dfuse_ino_entry_t *e = tbl->by_ino[i];
        while (e) {
            dfuse_ino_entry_t *next = e->next_by_ino;
            size_t idx = fnv1a_u64(e->ino) % new_cap;
            e->next_by_ino = new_buckets[idx];
            new_buckets[idx] = e;
            e = next;
        }
    }
    free(tbl->by_ino);
    tbl->by_ino = new_buckets;
    tbl->ino_cap = new_cap;
}

static dfuse_ino_entry_t *find_by_path(dfuse_inode_table_t *tbl, const char *path)
{
    size_t idx = fnv1a_string(path) % tbl->path_cap;
    dfuse_ino_entry_t *e = tbl->by_path[idx];
    while (e) {
        if (strcmp(e->path, path) == 0)
            return e;
        e = e->next_by_path;
    }
    return NULL;
}

static dfuse_ino_entry_t *find_by_ino(dfuse_inode_table_t *tbl, dfuse_ino_t ino)
{
    size_t idx = fnv1a_u64(ino) % tbl->ino_cap;
    dfuse_ino_entry_t *e = tbl->by_ino[idx];
    while (e) {
        if (e->ino == ino)
            return e;
        e = e->next_by_ino;
    }
    return NULL;
}

static void insert_entry(dfuse_inode_table_t *tbl, dfuse_ino_entry_t *e)
{
    /* Insert into path map */
    size_t pidx = fnv1a_string(e->path) % tbl->path_cap;
    e->next_by_path = tbl->by_path[pidx];
    tbl->by_path[pidx] = e;

    /* Insert into ino map */
    size_t iidx = fnv1a_u64(e->ino) % tbl->ino_cap;
    e->next_by_ino = tbl->by_ino[iidx];
    tbl->by_ino[iidx] = e;

    tbl->count++;
}

static void remove_from_path_map(dfuse_inode_table_t *tbl, dfuse_ino_entry_t *target)
{
    size_t idx = fnv1a_string(target->path) % tbl->path_cap;
    dfuse_ino_entry_t **pp = &tbl->by_path[idx];
    while (*pp) {
        if (*pp == target) {
            *pp = target->next_by_path;
            target->next_by_path = NULL;
            return;
        }
        pp = &(*pp)->next_by_path;
    }
}

static void remove_from_ino_map(dfuse_inode_table_t *tbl, dfuse_ino_entry_t *target)
{
    size_t idx = fnv1a_u64(target->ino) % tbl->ino_cap;
    dfuse_ino_entry_t **pp = &tbl->by_ino[idx];
    while (*pp) {
        if (*pp == target) {
            *pp = target->next_by_ino;
            target->next_by_ino = NULL;
            return;
        }
        pp = &(*pp)->next_by_ino;
    }
}

/* ---- Public API ---- */

dfuse_inode_table_t *dfuse_itable_create(void)
{
    dfuse_inode_table_t *tbl = calloc(1, sizeof(*tbl));
    if (!tbl) return NULL;

    tbl->path_cap = INITIAL_BUCKETS;
    tbl->ino_cap  = INITIAL_BUCKETS;
    tbl->by_path  = calloc(tbl->path_cap, sizeof(*tbl->by_path));
    tbl->by_ino   = calloc(tbl->ino_cap, sizeof(*tbl->by_ino));
    tbl->next_ino = 2;  /* root=1 is pre-inserted below */

    if (!tbl->by_path || !tbl->by_ino) {
        free(tbl->by_path);
        free(tbl->by_ino);
        free(tbl);
        return NULL;
    }

    pthread_mutex_init(&tbl->lock, NULL);

    /* Pre-insert root */
    dfuse_ino_entry_t *root = calloc(1, sizeof(*root));
    if (!root) {
        free(tbl->by_path);
        free(tbl->by_ino);
        pthread_mutex_destroy(&tbl->lock);
        free(tbl);
        return NULL;
    }
    root->ino = DFUSE_INO_ROOT;
    root->path = strdup("/");
    if (!root->path) {
        free(root);
        free(tbl->by_path);
        free(tbl->by_ino);
        pthread_mutex_destroy(&tbl->lock);
        free(tbl);
        return NULL;
    }
    insert_entry(tbl, root);

    return tbl;
}

void dfuse_itable_destroy(dfuse_inode_table_t *tbl)
{
    if (!tbl) return;

    /* Free all entries via the ino map */
    for (size_t i = 0; i < tbl->ino_cap; i++) {
        dfuse_ino_entry_t *e = tbl->by_ino[i];
        while (e) {
            dfuse_ino_entry_t *next = e->next_by_ino;
            free(e->path);
            free(e->attr_name);
            free(e);
            e = next;
        }
    }
    free(tbl->by_path);
    free(tbl->by_ino);
    pthread_mutex_destroy(&tbl->lock);
    free(tbl);
}

dfuse_ino_t dfuse_itable_get_or_create(dfuse_inode_table_t *tbl, const char *path)
{
    if (!tbl || !path) return 0;

    pthread_mutex_lock(&tbl->lock);

    dfuse_ino_entry_t *e = find_by_path(tbl, path);
    if (e) {
        dfuse_ino_t ino = e->ino;
        pthread_mutex_unlock(&tbl->lock);
        return ino;
    }

    /* Create new entry */
    e = calloc(1, sizeof(*e));
    if (!e) {
        pthread_mutex_unlock(&tbl->lock);
        return 0;
    }
    e->path = strdup(path);
    if (!e->path) {
        free(e);
        pthread_mutex_unlock(&tbl->lock);
        return 0;
    }
    e->ino = tbl->next_ino++;

    insert_entry(tbl, e);

    /* Rehash if load factor exceeded */
    if (tbl->count * LOAD_FACTOR_DEN > tbl->path_cap * LOAD_FACTOR_NUM)
        rehash_path_map(tbl);
    if (tbl->count * LOAD_FACTOR_DEN > tbl->ino_cap * LOAD_FACTOR_NUM)
        rehash_ino_map(tbl);

    dfuse_ino_t ino = e->ino;
    pthread_mutex_unlock(&tbl->lock);
    return ino;
}

dfuse_ino_t dfuse_itable_lookup(dfuse_inode_table_t *tbl, const char *path)
{
    if (!tbl || !path) return 0;

    pthread_mutex_lock(&tbl->lock);
    dfuse_ino_entry_t *e = find_by_path(tbl, path);
    dfuse_ino_t ino = e ? e->ino : 0;
    pthread_mutex_unlock(&tbl->lock);
    return ino;
}

const char *dfuse_itable_path(dfuse_inode_table_t *tbl, dfuse_ino_t ino)
{
    if (!tbl) return NULL;

    pthread_mutex_lock(&tbl->lock);
    dfuse_ino_entry_t *e = find_by_ino(tbl, ino);
    const char *path = e ? e->path : NULL;
    pthread_mutex_unlock(&tbl->lock);
    return path;
}

void dfuse_itable_remove(dfuse_inode_table_t *tbl, const char *path)
{
    if (!tbl || !path) return;

    pthread_mutex_lock(&tbl->lock);
    dfuse_ino_entry_t *e = find_by_path(tbl, path);
    if (e) {
        remove_from_path_map(tbl, e);
        remove_from_ino_map(tbl, e);
        tbl->count--;
        free(e->path);
        free(e->attr_name);
        free(e);
    }
    pthread_mutex_unlock(&tbl->lock);
}

void dfuse_itable_rename(dfuse_inode_table_t *tbl,
                          const char *old_path, const char *new_path)
{
    if (!tbl || !old_path || !new_path) return;

    pthread_mutex_lock(&tbl->lock);
    dfuse_ino_entry_t *e = find_by_path(tbl, old_path);
    if (!e) {
        pthread_mutex_unlock(&tbl->lock);
        return;
    }

    /* Remove from path map (hash changes with new path) */
    remove_from_path_map(tbl, e);

    /* Update path */
    char *new_str = strdup(new_path);
    if (!new_str) {
        /* Re-insert with old path on OOM */
        size_t idx = fnv1a_string(e->path) % tbl->path_cap;
        e->next_by_path = tbl->by_path[idx];
        tbl->by_path[idx] = e;
        pthread_mutex_unlock(&tbl->lock);
        return;
    }
    free(e->path);
    e->path = new_str;

    /* Re-insert into path map with new hash */
    size_t idx = fnv1a_string(e->path) % tbl->path_cap;
    e->next_by_path = tbl->by_path[idx];
    tbl->by_path[idx] = e;

    pthread_mutex_unlock(&tbl->lock);
}

/* ---- Named attribute (xattr) inode support ---- */

dfuse_ino_type_t dfuse_itable_type(dfuse_inode_table_t *tbl, dfuse_ino_t ino)
{
    if (!tbl) return DFUSE_INO_REGULAR;
    pthread_mutex_lock(&tbl->lock);
    dfuse_ino_entry_t *e = find_by_ino(tbl, ino);
    dfuse_ino_type_t type = e ? e->type : DFUSE_INO_REGULAR;
    pthread_mutex_unlock(&tbl->lock);
    return type;
}

dfuse_ino_t dfuse_itable_get_attrdir(dfuse_inode_table_t *tbl,
                                      dfuse_ino_t file_ino)
{
    if (!tbl) return 0;

    /* Synthetic path: "\x01A<ino>" — can't collide with real FUSE paths */
    char synth[64];
    snprintf(synth, sizeof(synth), "\x01" "A%llu",
             (unsigned long long)file_ino);

    pthread_mutex_lock(&tbl->lock);

    dfuse_ino_entry_t *e = find_by_path(tbl, synth);
    if (e) {
        dfuse_ino_t ino = e->ino;
        pthread_mutex_unlock(&tbl->lock);
        return ino;
    }

    /* Create new attrdir entry */
    e = calloc(1, sizeof(*e));
    if (!e) { pthread_mutex_unlock(&tbl->lock); return 0; }
    e->path = strdup(synth);
    if (!e->path) { free(e); pthread_mutex_unlock(&tbl->lock); return 0; }
    e->ino = tbl->next_ino++;
    e->type = DFUSE_INO_ATTRDIR;
    e->parent_ino = file_ino;

    insert_entry(tbl, e);

    if (tbl->count * LOAD_FACTOR_DEN > tbl->path_cap * LOAD_FACTOR_NUM)
        rehash_path_map(tbl);
    if (tbl->count * LOAD_FACTOR_DEN > tbl->ino_cap * LOAD_FACTOR_NUM)
        rehash_ino_map(tbl);

    dfuse_ino_t ino = e->ino;
    pthread_mutex_unlock(&tbl->lock);
    return ino;
}

dfuse_ino_t dfuse_itable_get_namedattr(dfuse_inode_table_t *tbl,
                                        dfuse_ino_t attrdir_ino,
                                        const char *name)
{
    if (!tbl || !name) return 0;

    /* Synthetic path: "\x01N<attrdir_ino>:<name>" */
    char synth[512];
    snprintf(synth, sizeof(synth), "\x01" "N%llu:%s",
             (unsigned long long)attrdir_ino, name);

    pthread_mutex_lock(&tbl->lock);

    dfuse_ino_entry_t *e = find_by_path(tbl, synth);
    if (e) {
        dfuse_ino_t ino = e->ino;
        pthread_mutex_unlock(&tbl->lock);
        return ino;
    }

    /* Find the attrdir to get parent_ino */
    dfuse_ino_entry_t *ad = find_by_ino(tbl, attrdir_ino);
    if (!ad || ad->type != DFUSE_INO_ATTRDIR) {
        pthread_mutex_unlock(&tbl->lock);
        return 0;
    }

    /* Create new namedattr entry */
    e = calloc(1, sizeof(*e));
    if (!e) { pthread_mutex_unlock(&tbl->lock); return 0; }
    e->path = strdup(synth);
    if (!e->path) { free(e); pthread_mutex_unlock(&tbl->lock); return 0; }
    e->attr_name = strdup(name);
    if (!e->attr_name) {
        free(e->path); free(e);
        pthread_mutex_unlock(&tbl->lock);
        return 0;
    }
    e->ino = tbl->next_ino++;
    e->type = DFUSE_INO_NAMEDATTR;
    e->parent_ino = ad->parent_ino;  /* the owning file's inode */

    insert_entry(tbl, e);

    if (tbl->count * LOAD_FACTOR_DEN > tbl->path_cap * LOAD_FACTOR_NUM)
        rehash_path_map(tbl);
    if (tbl->count * LOAD_FACTOR_DEN > tbl->ino_cap * LOAD_FACTOR_NUM)
        rehash_ino_map(tbl);

    dfuse_ino_t ino = e->ino;
    pthread_mutex_unlock(&tbl->lock);
    return ino;
}

dfuse_ino_t dfuse_itable_parent_ino(dfuse_inode_table_t *tbl, dfuse_ino_t ino)
{
    if (!tbl) return 0;
    pthread_mutex_lock(&tbl->lock);
    dfuse_ino_entry_t *e = find_by_ino(tbl, ino);
    dfuse_ino_t parent = e ? e->parent_ino : 0;
    pthread_mutex_unlock(&tbl->lock);
    return parent;
}

const char *dfuse_itable_attr_name(dfuse_inode_table_t *tbl, dfuse_ino_t ino)
{
    if (!tbl) return NULL;
    pthread_mutex_lock(&tbl->lock);
    dfuse_ino_entry_t *e = find_by_ino(tbl, ino);
    const char *name = (e && e->type == DFUSE_INO_NAMEDATTR) ? e->attr_name : NULL;
    pthread_mutex_unlock(&tbl->lock);
    return name;
}

char *dfuse_itable_path_dup(dfuse_inode_table_t *tbl, dfuse_ino_t ino)
{
    if (!tbl) return NULL;
    pthread_mutex_lock(&tbl->lock);
    dfuse_ino_entry_t *e = find_by_ino(tbl, ino);
    char *dup = (e && e->path) ? strdup(e->path) : NULL;
    pthread_mutex_unlock(&tbl->lock);
    return dup;
}

char *dfuse_itable_attr_name_dup(dfuse_inode_table_t *tbl, dfuse_ino_t ino)
{
    if (!tbl) return NULL;
    pthread_mutex_lock(&tbl->lock);
    dfuse_ino_entry_t *e = find_by_ino(tbl, ino);
    char *dup = (e && e->type == DFUSE_INO_NAMEDATTR && e->attr_name)
                 ? strdup(e->attr_name) : NULL;
    pthread_mutex_unlock(&tbl->lock);
    return dup;
}
