/*
 * DarwinFUSE — fuse_main() and component API implementation
 *
 * Provides both the monolithic fuse_main() entry point and the
 * component API (fuse_mount/fuse_new/fuse_loop/etc.) used by
 * programs like sshfs, encfs, and others.
 *
 * Copyright (c) 2026 Marcel Cotta. All rights reserved.
 * Licensed under the MIT License.
 */

#include <fuse.h>

#include "nfs4_server.h"
#include "darwinfuse_internal.h"
#include "fuse_context.h"
#include "inode_table.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <sys/wait.h>

/* ---- Thread-local FUSE context ---- */

static __thread struct fuse_context tls_context;

struct fuse_context *fuse_get_context(void)
{
    return &tls_context;
}

void darwinfuse_set_context(uid_t uid, gid_t gid)
{
    tls_context.uid = uid;
    tls_context.gid = gid;
    tls_context.pid = getpid();
    mode_t m = umask(0);
    umask(m);
    tls_context.umask = m;
}

void darwinfuse_set_private_data(void *private_data)
{
    tls_context.private_data = private_data;
}

/* ---- Component API types ---- */

struct fuse_chan {
    darwinfuse_server_t *server;
    dfuse_inode_table_t *inode_table;
    uint16_t             port;
    char                *mountpoint;
};

struct fuse_session {
    struct fuse *fuse;
};

struct fuse {
    struct fuse_chan              *chan;
    const struct fuse_operations *ops;
    size_t                        ops_size;
    void                         *user_data;
    void                         *init_result;
    struct fuse_session           session;
    volatile int                  exited;
};

/* ---- Global fuse instance for signal handling ---- */

static struct fuse *g_fuse = NULL;
static darwinfuse_server_t *g_server = NULL;

static void signal_handler(int sig)
{
    (void)sig;
    if (g_fuse)
        fuse_exit(g_fuse);
    else if (g_server)
        nfs4_server_stop(g_server);
}

static void *server_thread_func(void *arg)
{
    nfs4_server_run((darwinfuse_server_t *)arg);
    return NULL;
}

/* ---- Synthetic mount-time ops ---- */

/*
 * Minimal FUSE operations used during mount_nfs.
 * Only needs to answer GETATTR/ACCESS on root so mount_nfs succeeds.
 * The real ops are attached later via fuse_new().
 */
static int mount_getattr(const char *path, struct stat *st)
{
    memset(st, 0, sizeof(*st));
    if (strcmp(path, "/") == 0) {
        st->st_mode = S_IFDIR | 0755;
        st->st_nlink = 2;
        st->st_uid = getuid();
        st->st_gid = getgid();
        return 0;
    }
    return -ENOENT;
}

static int mount_access(const char *path, int mask)
{
    (void)mask;
    if (strcmp(path, "/") == 0) return 0;
    return -ENOENT;
}

static int mount_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
                          off_t offset, struct fuse_file_info *fi)
{
    (void)offset; (void)fi;
    if (strcmp(path, "/") != 0) return -ENOENT;
    filler(buf, ".", NULL, 0);
    filler(buf, "..", NULL, 0);
    return 0;
}

static struct fuse_operations mount_ops = {
    .getattr = mount_getattr,
    .access  = mount_access,
    .readdir = mount_readdir,
};

/* ---- Argument parsing helpers ---- */

typedef struct {
    const char *mount_point;
    int         nosuid;
    int         nodev;
    int         rdonly;
    int         nobrowse;
    int         foreground;
    int         debug;
    int         singlethreaded;
} parsed_args_t;

static void parse_mount_opts(const char *opts, parsed_args_t *out)
{
    char buf[1024];
    strncpy(buf, opts, sizeof(buf) - 1);
    buf[sizeof(buf) - 1] = '\0';

    char *saveptr = NULL;
    for (char *tok = strtok_r(buf, ",", &saveptr);
         tok != NULL;
         tok = strtok_r(NULL, ",", &saveptr))
    {
        if (strcmp(tok, "nosuid") == 0)       out->nosuid = 1;
        else if (strcmp(tok, "nodev") == 0)   out->nodev = 1;
        else if (strcmp(tok, "ro") == 0)      out->rdonly = 1;
        else if (strcmp(tok, "nobrowse") == 0) out->nobrowse = 1;
    }
}

static int parse_args(int argc, char *argv[], parsed_args_t *out)
{
    memset(out, 0, sizeof(*out));

    if (argc < 2) {
        DFUSE_ERR("Usage: <program> [options] <mount_point>");
        return -1;
    }

    /* Detect Basalt-style args: argv[1] is a path (starts with '/') */
    int basalt_style = (argv[1][0] == '/');

    if (basalt_style) {
        out->mount_point = argv[1];
        for (int i = 2; i < argc; i++) {
            if (strcmp(argv[i], "-o") == 0 && i + 1 < argc) {
                i++;
                parse_mount_opts(argv[i], out);
            } else if (strcmp(argv[i], "-f") == 0) {
                out->foreground = 1;
            } else if (strcmp(argv[i], "-d") == 0) {
                out->debug = 1;
                out->foreground = 1;
            }
        }
    } else {
        /* Standard FUSE-style */
        const char *mountpoint = NULL;
        for (int i = 1; i < argc; i++) {
            if (strcmp(argv[i], "-f") == 0) {
                out->foreground = 1;
            } else if (strcmp(argv[i], "-d") == 0) {
                out->debug = 1;
                out->foreground = 1;
            } else if (strcmp(argv[i], "-s") == 0) {
                out->singlethreaded = 1;
            } else if (strcmp(argv[i], "-o") == 0 && i + 1 < argc) {
                i++;
                parse_mount_opts(argv[i], out);
            } else if (argv[i][0] != '-') {
                mountpoint = argv[i];
            }
        }
        if (!mountpoint) {
            DFUSE_ERR("No mount point specified");
            return -1;
        }
        out->mount_point = mountpoint;
    }

    return 0;
}

/* ---- Mount via mount_nfs ---- */

static int do_mount_nfs(uint16_t port, const char *mount_point,
                         const parsed_args_t *args)
{
    char opts[512];
    int len = snprintf(opts, sizeof(opts),
        "vers=4,tcp,noac,noacl,noresvport,"
        "rsize=65536,wsize=65536,"
        "soft,intr,retrycnt=0,"
        "port=%u",
        (unsigned)port);

    if (args && args->nosuid)
        len += snprintf(opts + len, sizeof(opts) - (size_t)len, ",nosuid");
    if (args && args->nodev)
        len += snprintf(opts + len, sizeof(opts) - (size_t)len, ",nodev");
    if (args && args->rdonly)
        len += snprintf(opts + len, sizeof(opts) - (size_t)len, ",rdonly");
    if (args && args->nobrowse)
        len += snprintf(opts + len, sizeof(opts) - (size_t)len, ",nobrowse");

    DFUSE_LOG("mount_nfs -o %s 127.0.0.1:/ %s", opts, mount_point);

    int err_pipe[2];
    if (pipe(err_pipe) < 0) {
        DFUSE_ERR("pipe for mount_nfs stderr failed");
        return -1;
    }

    pid_t pid = fork();
    if (pid < 0) {
        DFUSE_ERR("fork failed: %s", strerror(errno));
        close(err_pipe[0]);
        close(err_pipe[1]);
        return -1;
    }

    if (pid == 0) {
        close(err_pipe[0]);
        dup2(err_pipe[1], STDERR_FILENO);
        close(err_pipe[1]);
        execlp("mount_nfs", "mount_nfs",
               "-o", opts,
               "127.0.0.1:/",
               mount_point,
               NULL);
        _exit(127);
    }

    close(err_pipe[1]);

    char errbuf[1024];
    ssize_t errlen = 0;
    ssize_t n;
    while ((n = read(err_pipe[0], errbuf + errlen,
                     sizeof(errbuf) - 1 - (size_t)errlen)) > 0)
        errlen += n;
    errbuf[errlen] = '\0';
    close(err_pipe[0]);

    int status;
    if (waitpid(pid, &status, 0) < 0) {
        DFUSE_ERR("waitpid failed: %s", strerror(errno));
        return -1;
    }

    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        DFUSE_ERR("mount_nfs failed (exit status %d): %s",
                  WIFEXITED(status) ? WEXITSTATUS(status) : -1,
                  errlen > 0 ? errbuf : "(no stderr output)");
        return -1;
    }

    DFUSE_LOG("mount_nfs succeeded");
    return 0;
}

/* ---- Component API implementation ---- */

struct fuse_chan *fuse_mount(const char *mountpoint, struct fuse_args *args)
{
    if (!mountpoint) return NULL;

    /* Parse mount options from args */
    parsed_args_t mount_args;
    memset(&mount_args, 0, sizeof(mount_args));
    mount_args.mount_point = mountpoint;

    if (args) {
        for (int i = 1; i < args->argc; i++) {
            if (strcmp(args->argv[i], "-o") == 0 && i + 1 < args->argc) {
                i++;
                parse_mount_opts(args->argv[i], &mount_args);
            }
        }
    }

    /* Create inode table */
    dfuse_inode_table_t *itable = dfuse_itable_create();
    if (!itable) {
        DFUSE_ERR("Failed to create inode table");
        return NULL;
    }

    /* Configure NFS server with synthetic mount-time ops */
    darwinfuse_config_t config;
    memset(&config, 0, sizeof(config));
    config.ops = &mount_ops;
    config.user_data = NULL;
    config.uid = getuid();
    config.gid = getgid();
    config.inode_table = itable;

    /* Create NFS server */
    uint16_t port = 0;
    darwinfuse_server_t *srv = nfs4_server_create(&config, &port);
    if (!srv) {
        DFUSE_ERR("Failed to create NFS server");
        dfuse_itable_destroy(itable);
        return NULL;
    }

    /* Start temporary server thread for mount */
    pthread_t srv_thread;
    if (pthread_create(&srv_thread, NULL, server_thread_func, srv) != 0) {
        DFUSE_ERR("Failed to create server thread");
        nfs4_server_destroy(srv);
        dfuse_itable_destroy(itable);
        return NULL;
    }

    /* Mount */
    darwinfuse_set_context(getuid(), getgid());
    if (do_mount_nfs(port, mountpoint, &mount_args) < 0) {
        DFUSE_ERR("Failed to mount NFS");
        nfs4_server_stop(srv);
        pthread_join(srv_thread, NULL);
        nfs4_server_destroy(srv);
        dfuse_itable_destroy(itable);
        return NULL;
    }

    /* Stop temporary server thread */
    nfs4_server_stop(srv);
    pthread_join(srv_thread, NULL);

    /* Create channel */
    struct fuse_chan *ch = calloc(1, sizeof(*ch));
    if (!ch) {
        nfs4_server_destroy(srv);
        dfuse_itable_destroy(itable);
        return NULL;
    }

    ch->server = srv;
    ch->inode_table = itable;
    ch->port = port;
    ch->mountpoint = strdup(mountpoint);

    DFUSE_LOG("fuse_mount: mounted on %s (port %u)", mountpoint, port);
    return ch;
}

void fuse_unmount(const char *mountpoint, struct fuse_chan *ch)
{
    if (mountpoint) {
        /* Attempt to unmount */
        pid_t pid = fork();
        if (pid == 0) {
            execlp("umount", "umount", mountpoint, NULL);
            _exit(127);
        }
        if (pid > 0) {
            int status;
            waitpid(pid, &status, 0);
        }
    }

    if (ch) {
        if (ch->server)
            nfs4_server_stop(ch->server);
        /* Note: server and inode_table are destroyed in fuse_destroy */
    }
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
    f->session.fuse = f;

    /* Attach real ops to the server */
    nfs4_server_set_ops(ch->server, op, user_data);

    /* Set private_data to user_data (init() may override later) */
    darwinfuse_set_private_data(user_data);

    DFUSE_LOG("fuse_new: ops attached");
    return f;
}

void fuse_destroy(struct fuse *f)
{
    if (!f) return;

    if (g_fuse == f)
        g_fuse = NULL;

    if (f->chan) {
        if (f->chan->server) {
            nfs4_server_destroy(f->chan->server);
            if (g_server == f->chan->server)
                g_server = NULL;
        }
        if (f->chan->inode_table)
            dfuse_itable_destroy(f->chan->inode_table);
        free(f->chan->mountpoint);
        free(f->chan);
    }

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
        conn_info.max_write = 65536;
        conn_info.max_readahead = 65536;
        conn_info.capable = FUSE_CAP_BIG_WRITES | FUSE_CAP_EXPORT_SUPPORT |
                            FUSE_CAP_ATOMIC_O_TRUNC
#ifdef __APPLE__
                            | FUSE_CAP_XTIMES | FUSE_CAP_CASE_INSENSITIVE
                            | FUSE_CAP_VOL_RENAME | FUSE_CAP_ALLOCATE
                            | FUSE_CAP_EXCHANGE_DATA
#endif
                            ;
        f->init_result = f->ops->init(&conn_info);
    }

    /* Set private_data to init() return value (or keep user_data) */
    darwinfuse_set_private_data(f->init_result ? f->init_result : f->user_data);

    /* Store private_data on server for worker threads */
    nfs4_server_set_private_data(f->chan->server,
                                  f->init_result ? f->init_result : f->user_data);

    /* Restart and run the server event loop */
    nfs4_server_restart(f->chan->server);

    DFUSE_LOG("fuse_loop: running event loop (pid=%d)", getpid());
    int rc = nfs4_server_run(f->chan->server);
    DFUSE_LOG("fuse_loop: server exited");

    /* Call ops->destroy() */
    if (f->ops->destroy)
        f->ops->destroy(f->init_result);

    if (g_fuse == f)
        g_fuse = NULL;
    if (g_server == f->chan->server)
        g_server = NULL;

    return rc;
}

int fuse_loop_mt(struct fuse *f)
{
    if (!f || !f->chan || !f->chan->server) return -1;
    nfs4_server_set_multithreaded(f->chan->server, DFUSE_DEFAULT_THREADS);
    return fuse_loop(f);
}

struct fuse_session *fuse_get_session(struct fuse *f)
{
    if (!f) return NULL;
    return &f->session;
}

int fuse_set_signal_handlers(struct fuse_session *se)
{
    if (!se || !se->fuse) return -1;

    g_fuse = se->fuse;
    if (se->fuse->chan)
        g_server = se->fuse->chan->server;

    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = signal_handler;
    sigaction(SIGTERM, &sa, NULL);
    sigaction(SIGINT, &sa, NULL);

    return 0;
}

void fuse_remove_signal_handlers(struct fuse_session *se)
{
    (void)se;

    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = SIG_DFL;
    sigaction(SIGTERM, &sa, NULL);
    sigaction(SIGINT, &sa, NULL);

    g_fuse = NULL;
}

void fuse_exit(struct fuse *f)
{
    if (!f) return;
    f->exited = 1;
    if (f->chan && f->chan->server)
        nfs4_server_stop(f->chan->server);
}

/* ---- Utility functions ---- */

int fuse_parse_cmdline(struct fuse_args *args, char **mountpoint,
                       int *multithreaded, int *foreground)
{
    if (!args) return -1;

    if (mountpoint) *mountpoint = NULL;
    if (multithreaded) *multithreaded = 0;
    if (foreground) *foreground = 0;

    for (int i = 1; i < args->argc; i++) {
        const char *arg = args->argv[i];
        if (strcmp(arg, "-f") == 0) {
            if (foreground) *foreground = 1;
        } else if (strcmp(arg, "-d") == 0) {
            if (foreground) *foreground = 1;
        } else if (strcmp(arg, "-s") == 0) {
            /* single-threaded (our default) */
        } else if (strcmp(arg, "-o") == 0) {
            i++;  /* skip option value */
        } else if (arg[0] != '-') {
            if (mountpoint && !*mountpoint)
                *mountpoint = strdup(arg);
        }
    }

    return 0;
}

int fuse_daemonize(int foreground)
{
    if (foreground)
        return 0;

    pid_t pid = fork();
    if (pid < 0) return -1;
    if (pid > 0) _exit(0);

    setsid();

    int devnull = open("/dev/null", O_RDWR);
    if (devnull >= 0) {
        dup2(devnull, STDIN_FILENO);
        dup2(devnull, STDOUT_FILENO);
        dup2(devnull, STDERR_FILENO);
        if (devnull > STDERR_FILENO)
            close(devnull);
    }

    return 0;
}

int fuse_version(void)
{
    return 26;
}

/* ---- fuse_main / fuse_main_real ---- */

int fuse_main_real(int argc, char *argv[],
                   const struct fuse_operations *op, size_t op_size,
                   void *user_data)
{
    (void)op_size;

    if (!op) return -1;

    /* Parse arguments */
    parsed_args_t args;
    if (parse_args(argc, argv, &args) < 0)
        return -1;

    DFUSE_LOG("fuse_main: uid=%u euid=%u mount_point=%s foreground=%d",
              getuid(), geteuid(), args.mount_point, args.foreground);

    /* Set initial FUSE context */
    darwinfuse_set_context(getuid(), getgid());
    darwinfuse_set_private_data(user_data);

    /* Create dynamic inode table */
    dfuse_inode_table_t *itable = dfuse_itable_create();
    if (!itable) {
        DFUSE_ERR("Failed to create inode table");
        return -1;
    }

    void *init_result = NULL;

    /* Configure NFS server */
    darwinfuse_config_t config;
    memset(&config, 0, sizeof(config));
    config.ops = op;
    config.user_data = user_data;
    config.uid = getuid();
    config.gid = getgid();
    config.inode_table = itable;

    /* Create NFS server */
    uint16_t port = 0;
    darwinfuse_server_t *srv = nfs4_server_create(&config, &port);
    if (!srv) {
        DFUSE_ERR("Failed to create NFS server");
        dfuse_itable_destroy(itable);
        return -1;
    }

    g_server = srv;
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = signal_handler;
    sigaction(SIGTERM, &sa, NULL);
    sigaction(SIGINT, &sa, NULL);

    pthread_t srv_thread;
    if (pthread_create(&srv_thread, NULL, server_thread_func, srv) != 0) {
        DFUSE_ERR("Failed to create server thread");
        nfs4_server_destroy(srv);
        dfuse_itable_destroy(itable);
        g_server = NULL;
        return -1;
    }

    if (do_mount_nfs(port, args.mount_point, &args) < 0) {
        DFUSE_ERR("Failed to mount NFS");
        nfs4_server_stop(srv);
        pthread_join(srv_thread, NULL);
        nfs4_server_destroy(srv);
        dfuse_itable_destroy(itable);
        g_server = NULL;
        return -1;
    }

    if (args.foreground) {
        /* Foreground mode */
        nfs4_server_stop(srv);
        pthread_join(srv_thread, NULL);

        DFUSE_LOG("Foreground mode: running in current process (pid=%d)", getpid());

        if (op->init) {
            struct fuse_conn_info conn_info;
            memset(&conn_info, 0, sizeof(conn_info));
            conn_info.proto_major = 7;
            conn_info.proto_minor = 26;
            conn_info.max_write = 65536;
            conn_info.max_readahead = 65536;
            conn_info.capable = FUSE_CAP_BIG_WRITES | FUSE_CAP_EXPORT_SUPPORT |
                                FUSE_CAP_ATOMIC_O_TRUNC;
            init_result = op->init(&conn_info);
        }
        darwinfuse_set_private_data(init_result ? init_result : user_data);

        if (!args.singlethreaded)
            nfs4_server_set_multithreaded(srv, DFUSE_DEFAULT_THREADS);
        nfs4_server_set_private_data(srv, init_result ? init_result : user_data);

        {
            struct sigaction sa2;
            memset(&sa2, 0, sizeof(sa2));
            sa2.sa_handler = signal_handler;
            sigaction(SIGTERM, &sa2, NULL);
            sigaction(SIGINT, &sa2, NULL);
        }

        nfs4_server_restart(srv);
        nfs4_server_run(srv);

        DFUSE_LOG("Foreground: server exited");

        nfs4_server_destroy(srv);
        dfuse_itable_destroy(itable);
        g_server = NULL;

        if (op->destroy)
            op->destroy(init_result);

        return 0;
    }

    /* Daemon mode */
    nfs4_server_stop(srv);
    pthread_join(srv_thread, NULL);

    DFUSE_LOG("mount succeeded, daemonizing");

    pid_t daemon_pid = fork();
    if (daemon_pid < 0) {
        nfs4_server_destroy(srv);
        dfuse_itable_destroy(itable);
        g_server = NULL;
        return -1;
    }

    if (daemon_pid > 0) {
        DFUSE_LOG("Parent: _exit(0), daemon pid=%d", daemon_pid);
        _exit(0);
    }

    /* ---- Child (daemon) ---- */
    setsid();

    int devnull = open("/dev/null", O_RDWR);
    if (devnull >= 0) {
        dup2(devnull, STDIN_FILENO);
        dup2(devnull, STDOUT_FILENO);
        dup2(devnull, STDERR_FILENO);
        if (devnull > STDERR_FILENO)
            close(devnull);
    }

    nfs4_server_close_inherited_pipes(srv);

    if (op->init) {
        struct fuse_conn_info conn_info;
        memset(&conn_info, 0, sizeof(conn_info));
        conn_info.proto_major = 7;
        conn_info.proto_minor = 26;
        conn_info.max_write = 65536;
        conn_info.max_readahead = 65536;
        conn_info.capable = FUSE_CAP_BIG_WRITES | FUSE_CAP_EXPORT_SUPPORT |
                            FUSE_CAP_ATOMIC_O_TRUNC
#ifdef __APPLE__
                            | FUSE_CAP_XTIMES | FUSE_CAP_CASE_INSENSITIVE
                            | FUSE_CAP_VOL_RENAME | FUSE_CAP_ALLOCATE
                            | FUSE_CAP_EXCHANGE_DATA
#endif
                            ;
        init_result = op->init(&conn_info);
    }
    darwinfuse_set_private_data(init_result ? init_result : user_data);

    if (!args.singlethreaded)
        nfs4_server_set_multithreaded(srv, DFUSE_DEFAULT_THREADS);
    nfs4_server_set_private_data(srv, init_result ? init_result : user_data);

    {
        struct sigaction sa2;
        memset(&sa2, 0, sizeof(sa2));
        sa2.sa_handler = signal_handler;
        sigaction(SIGTERM, &sa2, NULL);
        sigaction(SIGINT, &sa2, NULL);
    }

    nfs4_server_restart(srv);

    DFUSE_LOG("Daemon: running event loop (pid=%d)", getpid());
    nfs4_server_run(srv);
    DFUSE_LOG("Daemon: server exited");

    nfs4_server_destroy(srv);
    dfuse_itable_destroy(itable);
    g_server = NULL;

    if (op->destroy)
        op->destroy(init_result);

    _exit(0);
}

int fuse_main(int argc, char *argv[],
              const struct fuse_operations *op, void *user_data)
{
    return fuse_main_real(argc, argv, op, sizeof(*op), user_data);
}

int fuse_getgroups(int size, gid_t list[])
{
    return getgroups(size, list);
}

int fuse_interrupted(void)
{
    return 0;
}
