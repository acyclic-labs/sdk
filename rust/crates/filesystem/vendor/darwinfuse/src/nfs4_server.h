/*
 * DarwinFUSE — NFSv4 TCP server lifecycle
 *
 * Copyright (c) 2026 Marcel Cotta. All rights reserved.
 * Licensed under the MIT License.
 */

#ifndef DARWINFUSE_NFS4_SERVER_H
#define DARWINFUSE_NFS4_SERVER_H

#include <stdint.h>
#include <stdatomic.h>
#include <sys/types.h>

/* Forward declarations */
struct fuse_operations;
typedef struct dfuse_inode_table_s dfuse_inode_table_t;

/* Server configuration passed from fuse_mount() */
typedef struct {
    const struct fuse_operations *ops;
    void       *user_data;
    uid_t       uid;            /* Owner UID (for access control) */
    gid_t       gid;            /* Owner GID */
    struct dfuse_inode_table_s  *inode_table;  /* dynamic inode table */
    atomic_uint_fast64_t namespace_change; /* READDIR continuation revision */
    atomic_uint_fast64_t fresh_change;     /* counter behind unlabeled change values */
    int durable_writes;        /* FUSE_CAP_DURABLE_WRITES was negotiated */
    int node_identity;         /* FUSE_CAP_NODE_IDENTITY was negotiated */
    uint8_t write_verifier[8]; /* unique per mount, including in-process remounts */
} darwinfuse_config_t;

/* Opaque server state */
typedef struct darwinfuse_server darwinfuse_server_t;

/*
 * Create the server listening on a local socket in a fresh private
 * directory; only the kernel's NFS client may connect.
 * On failure, returns NULL.
 */
darwinfuse_server_t *nfs4_server_create(const darwinfuse_config_t *config);

/* The listening socket's path, for mount_nfs's "<path>:/" server spec. */
const char *nfs4_server_socket_path(const darwinfuse_server_t *srv);

/*
 * Run the NFS event loop. Blocks until the server is stopped
 * (via nfs4_server_stop or when all clients disconnect after mount).
 * Returns 0 on clean exit, -1 on error.
 */
int nfs4_server_run(darwinfuse_server_t *srv);

/*
 * Signal the server to stop (safe to call from signal handler).
 */
void nfs4_server_stop(darwinfuse_server_t *srv);

/*
 * Re-arm the server after stop so nfs4_server_run() can be called again.
 * Used after fork() to restart the event loop in the child process.
 */
void nfs4_server_restart(darwinfuse_server_t *srv);

/*
 * Destroy and free all server resources.
 */
void nfs4_server_destroy(darwinfuse_server_t *srv);

/*
 * Update the FUSE operations and user_data on a running server.
 * Used by the component API (fuse_new) to attach real ops after mount.
 */
void nfs4_server_set_ops(darwinfuse_server_t *srv,
                          const struct fuse_operations *ops,
                          void *user_data);

/*
 * Enable multi-threaded request processing with a thread pool.
 * Must be called before nfs4_server_run().
 * num_threads: number of worker threads (0 = single-threaded).
 */
void nfs4_server_set_multithreaded(darwinfuse_server_t *srv, int num_threads);

/*
 * Set private_data to propagate to worker thread TLS contexts.
 * Should be called after ops->init() returns.
 */
void nfs4_server_set_private_data(darwinfuse_server_t *srv, void *private_data);

/* Record whether init() negotiated FUSE_CAP_DURABLE_WRITES. */
void nfs4_server_set_durable_writes(darwinfuse_server_t *srv, int durable);

/* Record whether init() negotiated FUSE_CAP_NODE_IDENTITY. */
void nfs4_server_set_node_identity(darwinfuse_server_t *srv, int node_identity);

/* Invalidate outstanding READDIR cookie verifiers after an external change. */
void nfs4_server_mark_namespace_changed(darwinfuse_server_t *srv);

#endif /* DARWINFUSE_NFS4_SERVER_H */
