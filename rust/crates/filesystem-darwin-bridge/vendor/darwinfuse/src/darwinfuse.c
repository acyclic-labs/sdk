/*
 * DarwinFUSE — component API implementation
 *
 * The libfuse component API (fuse_mount/fuse_new/fuse_loop_mt/etc.) over
 * the NFSv4 loopback server.
 *
 * Copyright (c) 2026 Marcel Cotta. All rights reserved.
 * Licensed under the MIT License.
 */

#include <fuse.h>

#include "nfs4_server.h"
#include "darwin_mount_owner.h"
#include "darwinfuse_internal.h"
#include "fuse_context.h"
#include "inode_table.h"
#include "rpc.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdatomic.h>
#include <sys/mount.h>
#include <sys/stat.h>

/* ---- Diagnostics ---- */

static const char *log_path;
static pthread_once_t log_path_once = PTHREAD_ONCE_INIT;

static void log_path_init(void)
{
    log_path = getenv("DARWINFUSE_LOG");
}

const char *darwinfuse_log_path(void)
{
    pthread_once(&log_path_once, log_path_init);
    return log_path;
}

/* ---- Thread-local FUSE context ---- */

static __thread struct fuse_context tls_context;
static __thread gid_t tls_groups[RPC_AUTH_SYS_MAX_GROUPS];
static __thread unsigned tls_ngroups;

struct fuse_context *fuse_get_context(void)
{
    return &tls_context;
}

/* NFS carries no requesting pid, and the client applies its caller's umask
 * before sending a mode, so pid and umask stay zero.  (Sampling this
 * process's umask would briefly clear it for every other thread.) */
void darwinfuse_set_context(uid_t uid, gid_t gid,
                            unsigned ngroups, const gid_t *groups)
{
    tls_context.uid = uid;
    tls_context.gid = gid;
    tls_ngroups = ngroups < RPC_AUTH_SYS_MAX_GROUPS
        ? ngroups : RPC_AUTH_SYS_MAX_GROUPS;
    if (tls_ngroups > 0)
        memcpy(tls_groups, groups, tls_ngroups * sizeof(*groups));
}

void darwinfuse_set_private_data(void *private_data)
{
    tls_context.private_data = private_data;
}

/* ---- Argument parsing helpers ---- */

typedef struct {
    const char *mount_point;
    int         nosuid;
    int         nodev;
    int         rdonly;
    int         nobrowse;
    int         namedattr;
} parsed_args_t;

/* Reject what mount_nfs would not honor rather than dropping it silently. */
static int parse_mount_opts(const char *opts, parsed_args_t *out)
{
    char buf[1024];
    if (strlcpy(buf, opts, sizeof(buf)) >= sizeof(buf)) {
        DFUSE_ERR("Mount options are too long");
        return -1;
    }

    char *saveptr = NULL;
    for (char *tok = strtok_r(buf, ",", &saveptr);
         tok != NULL;
         tok = strtok_r(NULL, ",", &saveptr))
    {
        if (strcmp(tok, "nosuid") == 0)       out->nosuid = 1;
        else if (strcmp(tok, "nodev") == 0)   out->nodev = 1;
        else if (strcmp(tok, "ro") == 0)      out->rdonly = 1;
        else if (strcmp(tok, "nobrowse") == 0) out->nobrowse = 1;
        else if (strcmp(tok, "namedattr") == 0) out->namedattr = 1;
        else {
            DFUSE_ERR("Unsupported mount option: %s", tok);
            return -1;
        }
    }
    return 0;
}

/* ---- Component API types ---- */

struct fuse_chan {
    darwinfuse_server_t *server;
    dfuse_inode_table_t *inode_table;
    char                *mountpoint;
    parsed_args_t        mount_args;
    struct acyclic_mount_owner owner;
    const unsigned char  *owner_image;
    size_t               owner_image_len;
    int                  fence;
    const int           *holds;
    size_t               hold_count;
};

struct fuse {
    struct fuse_chan              *chan;
    const struct fuse_operations *ops;
    size_t                        ops_size;
    void                         *user_data;
    void                         *init_result;
    void                        (*mounted)(void *arg);
    void                         *mounted_arg;
};

static void *server_thread_func(void *arg)
{
    struct fuse_chan *channel = arg;
    int result = nfs4_server_run(channel->server);
    acyclic_mount_owner_release(&channel->owner);
    return (void *)(intptr_t)result;
}

/* ---- Mount via mount_nfs ---- */

/*
 * Mount the server's local socket. Metadata is revalidated on access; the
 * change attribute makes every revalidation exact. NFSv4.0 callbacks need an
 * IP address, so a local socket mount takes none: the server never delegates.
 * Explicit locks keep mount_nfs from selecting local locking, which NFSv4
 * rejects, for a soft, read-only mount.
 */
static int do_mount_nfs(struct fuse_chan *ch)
{
    const char *socket_path = nfs4_server_socket_path(ch->server);
    const parsed_args_t *args = &ch->mount_args;
    char opts[512];
    int len = snprintf(opts, sizeof(opts),
        "vers=4,locks,nocallback,actimeo=%d,nonegnamecache,noacl,"
        "rsize=262144,wsize=262144,"
        "soft,intr,retrycnt=0", DARWINFUSE_ATTRIBUTE_TIMEOUT);
    char server[128];
    if (snprintf(server, sizeof(server), "<%s>:/", socket_path) >= (int)sizeof(server)) {
        DFUSE_ERR("Socket path is too long: %s", socket_path);
        return -1;
    }

    if (args && args->nosuid)
        len += snprintf(opts + len, sizeof(opts) - (size_t)len, ",nosuid");
    if (args && args->nodev)
        len += snprintf(opts + len, sizeof(opts) - (size_t)len, ",nodev");
    if (args && args->rdonly)
        len += snprintf(opts + len, sizeof(opts) - (size_t)len, ",rdonly");
    if (args && args->nobrowse)
        len += snprintf(opts + len, sizeof(opts) - (size_t)len, ",nobrowse");
    if (args && args->namedattr)
        len += snprintf(opts + len, sizeof(opts) - (size_t)len, ",namedattr");

    DFUSE_LOG("mount_nfs -o %s %s %s", opts, server, ch->mountpoint);
    int status = acyclic_mount_owner_start(&ch->owner, ch->owner_image,
        ch->owner_image_len, ch->fence, ch->holds, ch->hold_count, opts, server, ch->mountpoint, socket_path);
    if (status) {
        DFUSE_ERR("Mount custodian could not attach NFS: %s", strerror(status));
        return -1;
    }

    DFUSE_LOG("mount_nfs succeeded");
    return 0;
}

/* ---- Component API implementation ---- */

struct fuse_chan *fuse_mount(const char *mountpoint, struct fuse_args *args,
                            const unsigned char *owner_image, size_t owner_image_len, int fence,
                            const int *holds, size_t hold_count)
{
    if (!mountpoint) return NULL;

    /* Parse mount options from args */
    parsed_args_t mount_args;
    memset(&mount_args, 0, sizeof(mount_args));

    if (args) {
        for (int i = 1; i < args->argc; i++) {
            if (strcmp(args->argv[i], "-o") == 0 && i + 1 < args->argc) {
                i++;
                if (parse_mount_opts(args->argv[i], &mount_args) < 0)
                    return NULL;
            }
        }
    }

    /* Create channel and inode table */
    struct fuse_chan *ch = calloc(1, sizeof(*ch));
    if (!ch) return NULL;
    ch->mountpoint = strdup(mountpoint);
    ch->inode_table = dfuse_itable_create();
    if (!ch->mountpoint || !ch->inode_table) {
        DFUSE_ERR("Failed to create inode table");
        dfuse_itable_destroy(ch->inode_table);
        free(ch->mountpoint);
        free(ch);
        return NULL;
    }
    ch->mount_args = mount_args;
    ch->mount_args.mount_point = ch->mountpoint;
    ch->owner_image = owner_image;
    ch->owner_image_len = owner_image_len;
    ch->fence = fence;
    ch->holds = holds;
    ch->hold_count = hold_count;

    /* Create NFS server; fuse_new() attaches the operations */
    darwinfuse_config_t config;
    memset(&config, 0, sizeof(config));
    config.uid = getuid();
    config.gid = getgid();
    config.acyclic_owner = args && args->argc > 0 && args->argv &&
        strcmp(args->argv[0], "acyclic-fs") == 0;
    config.inode_table = ch->inode_table;

    ch->server = nfs4_server_create(&config);
    if (!ch->server) {
        DFUSE_ERR("Failed to create NFS server");
        dfuse_itable_destroy(ch->inode_table);
        free(ch->mountpoint);
        free(ch);
        return NULL;
    }
    int owner_error = acyclic_mount_owner_init(&ch->owner);
    if (owner_error) {
        DFUSE_ERR("Failed to create NFS mount custody: %s", strerror(owner_error));
        nfs4_server_destroy(ch->server);
        dfuse_itable_destroy(ch->inode_table);
        free(ch->mountpoint);
        free(ch);
        return NULL;
    }

    DFUSE_LOG("fuse_mount: prepared %s (%s)", mountpoint,
              nfs4_server_socket_path(ch->server));
    return ch;
}

/*
 * Mount a channel whose operations and private data are attached. An
 * event-loop thread answers both mount_nfs and subsequent I/O. Keep it
 * running across readiness: a mounted callback may itself issue NFS I/O.
 */
static int mount_channel(struct fuse_chan *ch, pthread_t *srv_thread)
{
    if (pthread_create(srv_thread, NULL, server_thread_func, ch) != 0) {
        DFUSE_ERR("Failed to create server thread");
        return -1;
    }
    int rc = do_mount_nfs(ch);
    if (rc < 0) {
        DFUSE_ERR("Failed to mount NFS");
        nfs4_server_stop(ch->server);
        pthread_join(*srv_thread, NULL);
    }
    if (rc == 0)
        DFUSE_LOG("fuse_loop: mounted on %s", ch->mountpoint);
    return rc;
}

struct fuse *fuse_new(struct fuse_chan *ch, struct fuse_args *args,
                      const struct fuse_operations *op, size_t op_size,
                      void *user_data)
{
    (void)args;  /* additional args already parsed in fuse_mount */

    if (!ch || !op) return NULL;

    struct fuse *f = calloc(1, sizeof(*f));
    if (!f) return NULL;

    f->chan = ch;
    f->ops = op;
    f->ops_size = op_size;
    f->user_data = user_data;

    /* Attach real ops to the server */
    nfs4_server_set_ops(ch->server, op, user_data);

    /* Set private_data to user_data (init() may override later) */
    darwinfuse_set_private_data(user_data);

    DFUSE_LOG("fuse_new: ops attached");
    return f;
}

void fuse_chan_destroy(struct fuse_chan *ch)
{
    if (!ch) return;
    acyclic_mount_owner_destroy(&ch->owner);
    if (ch->server)
        nfs4_server_destroy(ch->server);
    if (ch->inode_table)
        dfuse_itable_destroy(ch->inode_table);
    free(ch->mountpoint);
    free(ch);
}

void fuse_destroy(struct fuse *f)
{
    if (!f) return;

    fuse_chan_destroy(f->chan);

    free(f);
}

int fuse_loop(struct fuse *f)
{
    if (!f || !f->chan || !f->chan->server) return -1;

    /* Set private_data to user_data before init() */
    darwinfuse_set_private_data(f->user_data);

    /* Call ops->init() */
    if (f->ops->init) {
        struct fuse_conn_info conn_info;
        memset(&conn_info, 0, sizeof(conn_info));
        conn_info.proto_major = 7;
        conn_info.proto_minor = 26;
        conn_info.max_write = DFUSE_IO_SIZE;
        conn_info.max_readahead = DFUSE_IO_SIZE;
        conn_info.capable = FUSE_CAP_BIG_WRITES | FUSE_CAP_EXPORT_SUPPORT |
                            FUSE_CAP_ATOMIC_O_TRUNC
#ifdef __APPLE__
                            | FUSE_CAP_XTIMES | FUSE_CAP_CASE_INSENSITIVE
                            | FUSE_CAP_VOL_RENAME | FUSE_CAP_ALLOCATE
                            | FUSE_CAP_EXCHANGE_DATA
#endif
                            | FUSE_CAP_DURABLE_WRITES
                            | FUSE_CAP_NODE_IDENTITY;
        f->init_result = f->ops->init(&conn_info);
        nfs4_server_set_durable_writes(
            f->chan->server, (conn_info.want & FUSE_CAP_DURABLE_WRITES) != 0);
        nfs4_server_set_node_identity(
            f->chan->server, (conn_info.want & FUSE_CAP_NODE_IDENTITY) != 0);
    }

    /* Set private_data to init() return value (or keep user_data) */
    darwinfuse_set_private_data(f->init_result ? f->init_result : f->user_data);

    /* Store private_data on server for worker threads */
    nfs4_server_set_private_data(f->chan->server,
                                  f->init_result ? f->init_result : f->user_data);

    /* Mount only now: every answer the NFS client caches must come from the
     * attached operations. The same event loop serves the entire mount
     * lifetime, including synchronous I/O from the readiness callback. */
    pthread_t srv_thread;
    int rc = mount_channel(f->chan, &srv_thread);
    if (rc == 0) {
        if (f->mounted)
            f->mounted(f->mounted_arg);
        /* Keep serving kernel I/O while the custodian detaches the mount. */
        int retirement = acyclic_mount_owner_wait(&f->chan->owner);
        nfs4_server_stop(f->chan->server);
        void *result = NULL;
        pthread_join(srv_thread, &result);
        rc = (int)(intptr_t)result;
        if (retirement) {
            DFUSE_ERR("Mount custodian did not confirm retirement: %s", strerror(retirement));
            if (rc == 0) rc = retirement;
        }
    }

    /* Call ops->destroy() */
    if (f->ops->destroy)
        f->ops->destroy(f->init_result);

    return rc;
}

int fuse_loop_mt(struct fuse *f)
{
    if (!f || !f->chan || !f->chan->server) return -1;
    nfs4_server_set_multithreaded(f->chan->server, DFUSE_DEFAULT_THREADS);
    return fuse_loop(f);
}

void fuse_exit(struct fuse *f)
{
    if (f && f->chan)
        acyclic_mount_owner_release(&f->chan->owner);
}

pid_t fuse_mount_owner_pid(const struct fuse *f)
{
    return f->chan->owner.pid;
}

void fuse_mark_namespace_changed(struct fuse *f)
{
    if (f && f->chan)
        nfs4_server_mark_namespace_changed(f->chan->server);
}

void fuse_set_mounted_callback(struct fuse *f, void (*mounted)(void *arg),
                               void *arg)
{
    if (!f) return;
    f->mounted = mounted;
    f->mounted_arg = arg;
}

/* ---- Utility functions ---- */

/* The requesting caller's supplementary groups (libfuse semantics), not
 * this process's. With size 0, returns how many there are. */
int fuse_getgroups(int size, gid_t list[])
{
    if (size == 0)
        return (int)tls_ngroups;
    if (size < 0 || (unsigned)size < tls_ngroups)
        return -ERANGE;
    memcpy(list, tls_groups, tls_ngroups * sizeof(*list));
    return (int)tls_ngroups;
}

/* A fresh child lookup requires an RPC even if the mount root is cached. */
struct mounted_io_test {
    struct fuse *fuse;
    const char *root;
    _Atomic int looked_up;
    _Atomic mode_t created_mode;
    int result;
};

static int mounted_io_getattr(const char *path, struct stat *st)
{
    struct mounted_io_test *test = fuse_get_context()->private_data;
    int root = strcmp(path, "/") == 0;
    int created = strcmp(path, "/created") == 0;
    mode_t mode = created ? atomic_load(&test->created_mode) : 0;
    if ((created && !mode) || (!root && !created && strcmp(path, "/ready") != 0))
        return -ENOENT;
    memset(st, 0, sizeof(*st));
    st->st_mode = root ? S_IFDIR | 0755 : (created ? mode : S_IFREG | 0444);
    st->st_nlink = root || created ? 2 : 1;
    st->st_ino = root ? 1 : (created ? 3 : 2);
    st->st_uid = getuid();
    st->st_gid = getgid();
    if (!root && !created) {
        st->st_size = 1;
        atomic_store(&test->looked_up, 1);
    }
    return 0;
}

static int mounted_io_mkdir(const char *path, mode_t mode)
{
    struct mounted_io_test *test = fuse_get_context()->private_data;
    if (strcmp(path, "/created") != 0)
        return -ENOENT;
    mode_t absent = 0;
    return atomic_compare_exchange_strong(&test->created_mode, &absent, S_IFDIR | (mode & 0777))
        ? 0 : -EEXIST;
}

static void mounted_io_ready(void *arg)
{
    struct mounted_io_test *test = arg;
    char path[4096];
    int len = snprintf(path, sizeof(path), "%s/ready", test->root);
    struct stat stat;
    if (len < 0 || (size_t)len >= sizeof(path))
        test->result = ENAMETOOLONG;
    else if (lstat(path, &stat) != 0)
        test->result = errno;
    else if (S_ISREG(stat.st_mode) && stat.st_size == 1 && atomic_load(&test->looked_up))
        test->result = 0;
    if (test->result != 0)
        DFUSE_ERR("Mounted readiness lookup failed: %s (callback reached=%d)",
                  strerror(test->result), atomic_load(&test->looked_up));
    if (test->result == 0) {
        len = snprintf(path, sizeof(path), "%s/created", test->root);
        if (len < 0 || (size_t)len >= sizeof(path))
            test->result = ENAMETOOLONG;
        else if (mkdir(path, 0700) != 0 || lstat(path, &stat) != 0)
            test->result = errno;
        else if (!S_ISDIR(stat.st_mode) || atomic_load(&test->created_mode) != (S_IFDIR | 0700))
            test->result = EIO;
        if (test->result != 0)
            DFUSE_ERR("Mounted readiness mkdir failed: %s (callback reached=%d)",
                      strerror(test->result), atomic_load(&test->created_mode) != 0);
    }
    if (test->result == 0) {
        /* The product holds a view by opening its root exactly so
         * (`open_root_directory`); data-access denial refuses this open. */
        int root = open(test->root, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
        if (root < 0 || fstat(root, &stat) != 0)
            test->result = errno;
        else if (!S_ISDIR(stat.st_mode))
            test->result = EIO;
        if (root >= 0)
            close(root);
        if (test->result != 0)
            DFUSE_ERR("Mounted readiness root open failed: %s", strerror(test->result));
    }
    fuse_exit(test->fuse);
}

int nfs4_test_mounted_io(const char *root, const unsigned char *owner_image, size_t owner_image_len)
{
    struct mounted_io_test test = { .root = root, .result = EIO };
    atomic_init(&test.looked_up, 0);
    atomic_init(&test.created_mode, 0);
    /* Mount exactly as the product does: the SDK owner name and `nobrowse`.
     * A browsable NFS mount is a user-visible network volume, which the
     * system's network-volume protection denies for data access (EPERM on
     * read, write, open and create) while still allowing metadata. */
    char *argv[] = {(char *)"acyclic-fs", (char *)"-o", (char *)"nobrowse", NULL};
    struct fuse_args args = FUSE_ARGS_INIT(3, argv);
    struct fuse_chan *channel = fuse_mount(root, &args, owner_image, owner_image_len, -1, NULL, 0);
    if (!channel) return EIO;
    struct fuse_operations ops = {
        .getattr = mounted_io_getattr,
        .mkdir = mounted_io_mkdir,
    };
    test.fuse = fuse_new(channel, &args, &ops, sizeof(ops), &test);
    if (!test.fuse) {
        fuse_chan_destroy(channel);
        return EIO;
    }
    fuse_set_mounted_callback(test.fuse, mounted_io_ready, &test);
    int rc = fuse_loop_mt(test.fuse);
    fuse_destroy(test.fuse);
    return rc == 0 ? test.result : rc;
}
