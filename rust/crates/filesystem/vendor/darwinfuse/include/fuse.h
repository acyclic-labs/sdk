/*
 * DarwinFUSE — libfuse-compatible public API
 *
 * Drop-in replacement for macFUSE / OSXFUSE / libfuse on macOS.
 * Compatible with FUSE API version 26; includes macFUSE extensions.
 *
 * Copyright (c) 2026 Marcel Cotta. All rights reserved.
 * Licensed under the MIT License.
 */

#ifndef DARWINFUSE_FUSE_H
#define DARWINFUSE_FUSE_H

#define FUSE_USE_VERSION 26

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <stdint.h>
#include <stddef.h>
#include <fcntl.h>

#ifdef __APPLE__
#include <sys/mount.h>   /* struct statfs (BSD) for statfs_x */
#endif

#include "fuse_opt.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ---- Opaque / forward-declared types ---- */

struct fuse;
struct fuse_chan;
struct fuse_session;
struct fuse_pollhandle;
struct fuse_bufvec;

/* ---- Data structures ---- */

struct fuse_file_info {
    int           flags;
    unsigned long fh_old;
    int           writepage;
    unsigned int  direct_io : 1;
    unsigned int  keep_cache : 1;
    unsigned int  flush : 1;
    unsigned int  nonseekable : 1;
    unsigned int  flock_release : 1;
#ifdef __APPLE__
    unsigned int  purge_attr : 1;
    unsigned int  purge_ubc : 1;
    unsigned int  padding : 25;
#else
    unsigned int  padding : 27;
#endif
    uint64_t      fh;
    uint64_t      lock_owner;
};

struct fuse_conn_info {
    unsigned proto_major;
    unsigned proto_minor;
    unsigned async_read;
    unsigned max_write;
    unsigned max_readahead;
    unsigned capable;
    unsigned want;
    unsigned max_background;
    unsigned congestion_threshold;
    unsigned reserved[23];
};

struct fuse_context {
    struct fuse *fuse;
    uid_t  uid;
    gid_t  gid;
    pid_t  pid;
    void  *private_data;
    mode_t umask;
};

/* ---- Capability flags for fuse_conn_info ---- */

/* Standard libfuse 2.x */
#define FUSE_CAP_ASYNC_READ      (1 << 0)
#define FUSE_CAP_POSIX_LOCKS     (1 << 1)
#define FUSE_CAP_ATOMIC_O_TRUNC  (1 << 3)
#define FUSE_CAP_EXPORT_SUPPORT  (1 << 4)
#define FUSE_CAP_BIG_WRITES      (1 << 5)
#define FUSE_CAP_DONT_MASK       (1 << 6)
#define FUSE_CAP_SPLICE_WRITE    (1 << 7)   /* Linux-only, no-op on macOS */
#define FUSE_CAP_SPLICE_MOVE     (1 << 8)   /* Linux-only, no-op on macOS */
#define FUSE_CAP_SPLICE_READ     (1 << 9)   /* Linux-only, no-op on macOS */
#define FUSE_CAP_FLOCK_LOCKS     (1 << 10)
#define FUSE_CAP_IOCTL_DIR       (1 << 11)

/* DarwinFUSE: a write is as durable once the write callback returns as
 * fsync would make it (fsync publishes nothing more), so WRITE replies are
 * stable and the NFS client never needs to COMMIT. */
#define FUSE_CAP_DURABLE_WRITES  (1 << 20)

/* DarwinFUSE: whole seconds the NFS client caches attributes and names
 * (actimeo); a change made around the mount reaches it once they expire. */
#define DARWINFUSE_ATTRIBUTE_TIMEOUT 1

#ifdef __APPLE__
/* macFUSE-specific capability flags */
#define FUSE_CAP_ALLOCATE          (1 << 27)
#define FUSE_CAP_EXCHANGE_DATA     (1 << 28)
#define FUSE_CAP_CASE_INSENSITIVE  (1 << 29)
#define FUSE_CAP_VOL_RENAME        (1 << 30)
#define FUSE_CAP_XTIMES            (1u << 31)

/* Convenience macros (deprecated in macFUSE, kept for compatibility) */
#define FUSE_ENABLE_SETVOLNAME(c)       ((c)->want |= FUSE_CAP_VOL_RENAME)
#define FUSE_ENABLE_XTIMES(c)           ((c)->want |= FUSE_CAP_XTIMES)
#define FUSE_ENABLE_CASE_INSENSITIVE(c) ((c)->want |= FUSE_CAP_CASE_INSENSITIVE)
#endif /* __APPLE__ */

typedef int (*fuse_fill_dir_t)(void *buf, const char *name,
                                const struct stat *stbuf, off_t off);

/* ---- macFUSE setattr_x ---- */

#ifdef __APPLE__
struct setattr_x {
    int32_t         valid;
    mode_t          mode;
    uid_t           uid;
    gid_t           gid;
    off_t           size;
    struct timespec acctime;
    struct timespec modtime;
    struct timespec crtime;
    struct timespec chgtime;
    struct timespec bkuptime;
    uint32_t        flags;
};

#define SETATTR_WANTS_MODE(a)       ((a)->valid & (1 << 0))
#define SETATTR_WANTS_UID(a)        ((a)->valid & (1 << 1))
#define SETATTR_WANTS_GID(a)        ((a)->valid & (1 << 2))
#define SETATTR_WANTS_SIZE(a)       ((a)->valid & (1 << 3))
#define SETATTR_WANTS_ACCTIME(a)    ((a)->valid & (1 << 4))
#define SETATTR_WANTS_MODTIME(a)    ((a)->valid & (1 << 5))
#define SETATTR_WANTS_CRTIME(a)     ((a)->valid & (1 << 28))
#define SETATTR_WANTS_CHGTIME(a)    ((a)->valid & (1 << 29))
#define SETATTR_WANTS_BKUPTIME(a)   ((a)->valid & (1 << 30))
#define SETATTR_WANTS_FLAGS(a)      ((a)->valid & (1 << 31))
#endif /* __APPLE__ */

/*
 * FUSE filesystem operations.
 * Field order matches libfuse 2.9 / macFUSE for struct-layout compatibility.
 * Unused callbacks should be set to NULL.
 */
struct fuse_operations {
    /* --- libfuse 2.6 core --- */
    int (*getattr)    (const char *, struct stat *);
    int (*readlink)   (const char *, char *, size_t);
    /* getdir — deprecated, was removed in FUSE 3.x */
    void *_deprecated_getdir;
    int (*mknod)      (const char *, mode_t, dev_t);
    int (*mkdir)      (const char *, mode_t);
    int (*unlink)     (const char *);
    int (*rmdir)      (const char *);
    int (*symlink)    (const char *, const char *);
    int (*rename)     (const char *, const char *);
    int (*link)       (const char *, const char *);
    int (*chmod)      (const char *, mode_t);
    int (*chown)      (const char *, uid_t, gid_t);
    int (*truncate)   (const char *, off_t);
    /* utime — deprecated */
    void *_deprecated_utime;
    int (*open)       (const char *, struct fuse_file_info *);
    int (*read)       (const char *, char *, size_t, off_t,
                       struct fuse_file_info *);
    int (*write)      (const char *, const char *, size_t, off_t,
                       struct fuse_file_info *);
    int (*statfs)     (const char *, struct statvfs *);
    int (*flush)      (const char *, struct fuse_file_info *);
    int (*release)    (const char *, struct fuse_file_info *);
    int (*fsync)      (const char *, int, struct fuse_file_info *);
#ifdef __APPLE__
    int (*setxattr)   (const char *, const char *, const char *, size_t, int,
                       uint32_t /* position */);
    int (*getxattr)   (const char *, const char *, char *, size_t,
                       uint32_t /* position */);
#else
    int (*setxattr)   (const char *, const char *, const char *, size_t, int);
    int (*getxattr)   (const char *, const char *, char *, size_t);
#endif
    int (*listxattr)  (const char *, char *, size_t);
    int (*removexattr)(const char *, const char *);
    int (*opendir)    (const char *, struct fuse_file_info *);
    int (*readdir)    (const char *, void *, fuse_fill_dir_t, off_t,
                       struct fuse_file_info *);
    int (*releasedir) (const char *, struct fuse_file_info *);
    int (*fsyncdir)   (const char *, int, struct fuse_file_info *);
    void *(*init)     (struct fuse_conn_info *);
    void (*destroy)   (void *);
    int (*access)     (const char *, int);
    int (*create)     (const char *, mode_t, struct fuse_file_info *);
    int (*ftruncate)  (const char *, off_t, struct fuse_file_info *);
    int (*fgetattr)   (const char *, struct stat *, struct fuse_file_info *);
    int (*lock)       (const char *, struct fuse_file_info *, int,
                       struct flock *);
    int (*utimens)    (const char *, const struct timespec tv[2]);
    int (*bmap)       (const char *, size_t, uint64_t *);

    /* --- libfuse 2.8+ flags and callbacks --- */
    unsigned int flag_nullpath_ok : 1;
    unsigned int flag_nopath : 1;
    unsigned int flag_utime_omit_ok : 1;
    unsigned int flag_reserved : 29;

    int (*ioctl)      (const char *, int, void *, struct fuse_file_info *,
                       unsigned int, void *);
    int (*poll)       (const char *, struct fuse_file_info *,
                       struct fuse_pollhandle *, unsigned *);
    int (*write_buf)  (const char *, struct fuse_bufvec *, off_t,
                       struct fuse_file_info *);
    int (*read_buf)   (const char *, struct fuse_bufvec **, size_t, off_t,
                       struct fuse_file_info *);
    int (*flock)      (const char *, struct fuse_file_info *, int);
    int (*fallocate)  (const char *, int, off_t, off_t,
                       struct fuse_file_info *);

#ifdef __APPLE__
    /* --- macFUSE extensions --- */
    void *_reserved00;
    void *_reserved01;
    int (*renamex)    (const char *, const char *, unsigned int);
    int (*statfs_x)   (const char *, struct statfs *);
    int (*setvolname) (const char *);
    int (*exchange)   (const char *, const char *, unsigned long);
    int (*getxtimes)  (const char *, struct timespec *bkuptime,
                       struct timespec *crtime);
    int (*setbkuptime)(const char *, const struct timespec *);
    int (*setchgtime) (const char *, const struct timespec *);
    int (*setcrtime)  (const char *, const struct timespec *);
    int (*chflags)    (const char *, uint32_t);
    int (*setattr_x)  (const char *, struct setattr_x *);
    int (*fsetattr_x) (const char *, struct setattr_x *,
                       struct fuse_file_info *);
#endif /* __APPLE__ */
};

/*
 * DarwinFUSE extension: the NFSv4 change attribute (RFC 7530 s5.4) travels
 * in the struct stat it labels, from getattr, fgetattr, and readdir alike.
 * A filesystem stores a nonzero label below FUSE_CHANGE_UNLABELED that
 * differs from every label it gave the object before whenever the object's
 * attributes, listing, or data may differ.  The NFS client trusts cached
 * state while the label is unchanged and revalidates at most a second later.
 * Zero means unlabeled: every report is then new, so nothing is trusted
 * past a revalidation.
 */
#ifdef __APPLE__
#define FUSE_STAT_CHANGE(st)   ((uint64_t)(st)->st_qspare[0])
#else
#define FUSE_STAT_CHANGE(st)   ((void)(st), UINT64_C(0))
#endif
#define FUSE_CHANGE_UNLABELED  (UINT64_C(1) << 63)

/* ---- High-level API (fuse_main) ---- */

/*
 * Main entry point. Parses arguments, starts NFSv4 server, mounts,
 * and runs event loop. Blocks until the filesystem is unmounted.
 *
 * Returns 0 on success, non-zero on failure.
 */
int fuse_main(int argc, char *argv[],
              const struct fuse_operations *op, void *user_data);

/*
 * Same as fuse_main but accepts ops_size for ABI compatibility.
 * This is what the libfuse macro typically expands to.
 */
int fuse_main_real(int argc, char *argv[],
                   const struct fuse_operations *op, size_t op_size,
                   void *user_data);

/* ---- Component API ---- */

/*
 * Prepare a FUSE filesystem mount. Creates the NFSv4 server for mountpoint
 * and validates the options; the kernel mount itself happens when the
 * event loop starts, so the NFS client never caches answers produced before
 * fuse_new() attached the filesystem callbacks.
 * Returns a channel on success, NULL on failure (including unknown options).
 */
struct fuse_chan *fuse_mount(const char *mountpoint, struct fuse_args *args);

/*
 * Unmount a FUSE filesystem.
 */
void fuse_unmount(const char *mountpoint, struct fuse_chan *ch);

/*
 * Create a new FUSE filesystem instance.
 * Attaches the filesystem callbacks to a channel from fuse_mount().
 * Returns the FUSE handle on success, NULL on failure.
 */
struct fuse *fuse_new(struct fuse_chan *ch, struct fuse_args *args,
                      const struct fuse_operations *op, size_t op_size,
                      void *user_data);

/*
 * Destroy a FUSE filesystem instance (free resources).
 */
void fuse_destroy(struct fuse *f);

/*
 * Run the FUSE event loop (single-threaded).
 * Calls init() at start, mounts the channel, and calls destroy() at end.
 * Blocks until the filesystem is unmounted or fuse_exit() is called.
 * Returns 0 on clean exit, -1 on error (including a failed mount).
 */
int fuse_loop(struct fuse *f);

/*
 * Run the FUSE event loop (multi-threaded).
 */
int fuse_loop_mt(struct fuse *f);

/*
 * Get the session from a FUSE handle (for signal handler setup).
 */
struct fuse_session *fuse_get_session(struct fuse *f);

/*
 * Install signal handlers for clean shutdown.
 * SIGINT and SIGTERM will trigger fuse_exit().
 */
int fuse_set_signal_handlers(struct fuse_session *se);

/*
 * Remove previously installed signal handlers.
 */
void fuse_remove_signal_handlers(struct fuse_session *se);

/*
 * Signal the event loop to exit.
 */
void fuse_exit(struct fuse *f);

/* Reject stale NFS READDIR continuations after provider-side changes. */
void fuse_mark_namespace_changed(struct fuse *f);

/*
 * Report, from inside fuse_loop(), the moment the mount became visible:
 * the loop calls mounted(arg) once, after the kernel accepted the mount and
 * before it serves the mount's requests. A caller that must not expose the
 * mount point earlier waits for this instead of polling the mount table.
 */
void fuse_set_mounted_callback(struct fuse *f, void (*mounted)(void *arg),
                               void *arg);

/* ---- Utility functions ---- */

/*
 * Returns the FUSE context for the current request.
 */
struct fuse_context *fuse_get_context(void);

/*
 * Parse standard FUSE command-line options.
 * Extracts mountpoint, foreground flag, and multi-thread flag.
 * Returns 0 on success, -1 on error.
 */
int fuse_parse_cmdline(struct fuse_args *args, char **mountpoint,
                       int *multithreaded, int *foreground);

/*
 * Daemonize the process (unless foreground is true).
 * Returns 0 on success, -1 on error.
 */
int fuse_daemonize(int foreground);

/*
 * Return the FUSE library version number (26).
 */
int fuse_version(void);

/*
 * Get supplementary group IDs for the current request.
 * Returns number of groups on success, -1 on error.
 */
int fuse_getgroups(int size, gid_t list[]);

/*
 * Check if the current request has been interrupted.
 * Returns 1 if interrupted, 0 otherwise.
 */
int fuse_interrupted(void);

#ifdef __cplusplus
}
#endif

#endif /* DARWINFUSE_FUSE_H */
