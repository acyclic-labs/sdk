/*
 * DarwinFUSE — NFSv4 COMPOUND dispatcher and operation handlers
 *
 * Translates NFSv4 operations into FUSE callback invocations.
 * Implements the subset of NFSv4.0 (RFC 7530) required for
 * macOS mount_nfs to mount and operate on a general-purpose
 * virtual filesystem.
 *
 * Copyright (c) 2026 Marcel Cotta. All rights reserved.
 * Licensed under the MIT License.
 */

#include "nfs4_ops.h"
#include "nfs4_xdr.h"
#include "darwinfuse_internal.h"
#include "fuse_context.h"
#include "inode_table.h"
#include "rpc.h"

#include <fuse.h>

#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <time.h>
#include <unistd.h>
#include <errno.h>
#include <arpa/inet.h>
#include <sys/xattr.h>

/* xattr calling conventions — macOS adds a position parameter */
#ifdef __APPLE__
#define FUSE_GETXATTR(ops, p, n, b, s)     (ops)->getxattr((p), (n), (b), (s), 0)
#define FUSE_SETXATTR(ops, p, n, v, s, f)  (ops)->setxattr((p), (n), (v), (s), (f), 0)
#else
#define FUSE_GETXATTR(ops, p, n, b, s)     (ops)->getxattr((p), (n), (b), (s))
#define FUSE_SETXATTR(ops, p, n, v, s, f)  (ops)->setxattr((p), (n), (v), (s), (f))
#endif

/* ---- errno to NFS4 status mapping ---- */

static void encode_write_verifier(const darwinfuse_config_t *config,
                                  xdr_buf_t *rep)
{
    xdr_encode_opaque_fixed(rep, config->write_verifier,
                            sizeof(config->write_verifier));
}

static uint32_t errno_to_nfs4(int fuse_rc)
{
    if (fuse_rc == 0) return NFS4_OK;
    int e = (fuse_rc < 0) ? -fuse_rc : fuse_rc;
    switch (e) {
    case EPERM:        return NFS4ERR_PERM;
    case ENOENT:       return NFS4ERR_NOENT;
#ifdef ENOATTR
    case ENOATTR:      return NFS4ERR_NOENT;
#endif
#if defined(ENODATA) && (!defined(ENOATTR) || ENODATA != ENOATTR)
    case ENODATA:      return NFS4ERR_NOENT;
#endif
    case EIO:          return NFS4ERR_IO;
    case ENXIO:        return NFS4ERR_NXIO;
    case EACCES:       return NFS4ERR_ACCESS;
    case EEXIST:       return NFS4ERR_EXIST;
    case EXDEV:        return NFS4ERR_XDEV;
    case ENOTDIR:      return NFS4ERR_NOTDIR;
    case EISDIR:       return NFS4ERR_ISDIR;
    case EINVAL:       return NFS4ERR_INVAL;
    case EFBIG:        return NFS4ERR_FBIG;
    case ENOSPC:       return NFS4ERR_NOSPC;
    case EROFS:        return NFS4ERR_ROFS;
    case ENAMETOOLONG: return NFS4ERR_NAMETOOLONG;
    case ENOTEMPTY:    return NFS4ERR_NOTEMPTY;
    case ESTALE:       return NFS4ERR_STALE;
    case ENOTSUP:      return NFS4ERR_NOTSUPP;
    default:           return NFS4ERR_IO;
    }
}

/* ---- Filehandle helpers (dynamic inodes) ---- */

/*
 * Encode an inode number as an 8-byte filehandle (network byte order).
 */
static void fh_set_ino(uint8_t *fh, uint32_t *fh_len, dfuse_ino_t ino)
{
    uint64_t net_hi = htonl((uint32_t)(ino >> 32));
    uint64_t net_lo = htonl((uint32_t)(ino & 0xFFFFFFFF));
    memcpy(fh, &net_hi, 4);
    memcpy(fh + 4, &net_lo, 4);
    *fh_len = DFUSE_FH_LEN;
}

/*
 * Decode an inode number from an 8-byte filehandle.
 * Returns 0 on invalid length.
 */
static dfuse_ino_t fh_get_ino(const uint8_t *fh, uint32_t fh_len)
{
    if (fh_len == DFUSE_FH_LEN) {
        uint32_t hi, lo;
        memcpy(&hi, fh, 4);
        memcpy(&lo, fh + 4, 4);
        return ((uint64_t)ntohl(hi) << 32) | ntohl(lo);
    }
    return 0;
}

/*
 * Resolve filehandle to a FUSE path via the inode table.
 * Returns a heap-allocated string; caller must free().
 * Thread-safe: the copy is made under the table lock.
 */
static char *fh_to_path(const darwinfuse_config_t *config,
                          const uint8_t *fh, uint32_t fh_len)
{
    dfuse_ino_t ino = fh_get_ino(fh, fh_len);
    if (ino == 0) return NULL;
    return dfuse_itable_path_dup(config->inode_table, ino);
}

/*
 * Build child path from parent path + name.
 * Handles the root case (parent="/") to avoid double slashes.
 */
static void build_child_path(char *out, size_t out_size,
                              const char *parent, const char *name)
{
    if (strcmp(parent, "/") == 0)
        snprintf(out, out_size, "/%s", name);
    else
        snprintf(out, out_size, "%s/%s", parent, name);
}

/*
 * Ensure the open_files array has room for one more entry.
 * Grows by doubling capacity. Returns 0 on success, -1 on allocation failure.
 */
static int ensure_open_file_capacity(nfs4_conn_state_t *conn)
{
    if (conn->open_file_count < conn->open_file_cap)
        return 0;

    int new_cap = conn->open_file_cap ? conn->open_file_cap * 2 : OPEN_FILES_INITIAL_CAP;
    nfs4_open_file_t *new_arr = realloc(conn->open_files,
                                         (size_t)new_cap * sizeof(nfs4_open_file_t));
    if (!new_arr)
        return -1;

    conn->open_files = new_arr;
    conn->open_file_cap = new_cap;
    return 0;
}

/*
 * Track one open under a fresh stateid, with the FUSE handle fi names (none
 * when fi is NULL). Returns 0, or -1 when the open cannot be tracked.
 */
static int track_open(nfs4_conn_state_t *conn, dfuse_ino_t ino,
                      const struct fuse_file_info *fi, nfs4_stateid_t *sid)
{
    int rc = -1;
    pthread_mutex_lock(&conn->lock);
    if (ensure_open_file_capacity(conn) == 0) {
        conn->open_seqid++;
        sid->seqid = conn->open_seqid;
        arc4random_buf(sid->other, sizeof(sid->other));
        nfs4_open_file_t *of = &conn->open_files[conn->open_file_count++];
        of->stateid = *sid;
        of->ino = ino;
        of->fuse_fh = fi ? fi->fh : 0;
        of->flags = fi ? fi->flags : 0;
        rc = 0;
    }
    pthread_mutex_unlock(&conn->lock);
    return rc;
}

/*
 * Find an open file entry by stateid.
 */
static nfs4_open_file_t *find_open_file(nfs4_conn_state_t *conn,
                                         const uint8_t *sid_other)
{
    for (int i = 0; i < conn->open_file_count; i++) {
        if (memcmp(conn->open_files[i].stateid.other, sid_other, 12) == 0)
            return &conn->open_files[i];
    }
    return NULL;
}

/*
 * Find an open file entry by inode (for fgetattr).
 */
static nfs4_open_file_t *find_open_file_by_ino(nfs4_conn_state_t *conn,
                                                 dfuse_ino_t ino)
{
    for (int i = 0; i < conn->open_file_count; i++) {
        if (conn->open_files[i].ino == ino)
            return &conn->open_files[i];
    }
    return NULL;
}

/*
 * Fill fi from the open named by sid_other (NULL for none), else from any
 * open of ino: lock and special stateids name no open, and a handle-less
 * callback would resolve the object again for every request.
 * Returns 1 when fi names an open file handle.
 */
static int open_file_info(nfs4_conn_state_t *conn, const uint8_t *sid_other,
                          dfuse_ino_t ino, struct fuse_file_info *fi)
{
    memset(fi, 0, sizeof(*fi));
    pthread_mutex_lock(&conn->lock);
    nfs4_open_file_t *of = sid_other ? find_open_file(conn, sid_other) : NULL;
    if (!of)
        of = find_open_file_by_ino(conn, ino);
    if (of) {
        fi->fh = of->fuse_fh;
        fi->flags = of->flags;
    }
    pthread_mutex_unlock(&conn->lock);
    return of != NULL;
}

/*
 * Get the real file path for an xattr inode (attrdir or namedattr).
 * Returns a heap-allocated string; caller must free().
 */
static char *xattr_file_path(const darwinfuse_config_t *config,
                               dfuse_ino_t ino)
{
    dfuse_ino_t parent = dfuse_itable_parent_ino(config->inode_table, ino);
    if (parent == 0) return NULL;
    return dfuse_itable_path_dup(config->inode_table, parent);
}

/* ---- Attribute bitmap helpers ---- */

/* Our supported attributes — two bitmap words */
static const uint32_t supported_bitmap[2] = {
    /* Word 0 */
    (1u << FATTR4_SUPPORTED_ATTRS) |
    (1u << FATTR4_TYPE) |
    (1u << FATTR4_FH_EXPIRE_TYPE) |
    (1u << FATTR4_CHANGE) |
    (1u << FATTR4_SIZE) |
    (1u << FATTR4_LINK_SUPPORT) |
    (1u << FATTR4_SYMLINK_SUPPORT) |
    (1u << FATTR4_NAMED_ATTR) |
    (1u << FATTR4_FSID) |
    (1u << FATTR4_UNIQUE_HANDLES) |
    (1u << FATTR4_LEASE_TIME) |
    (1u << FATTR4_RDATTR_ERROR) |
    (1u << FATTR4_FILEHANDLE) |
    (1u << FATTR4_FILEID) |
    (1u << FATTR4_FILES_AVAIL) |
    (1u << FATTR4_FILES_FREE) |
    (1u << FATTR4_FILES_TOTAL) |
    (1u << FATTR4_MAXFILESIZE) |
    (1u << FATTR4_MAXLINK) |
    (1u << FATTR4_MAXNAME) |
    (1u << FATTR4_MAXREAD) |
    (1u << FATTR4_MAXWRITE),

    /* Word 1 (bits 32+, stored as word index 1) */
    (1u << (FATTR4_MODE - 32)) |
    (1u << (FATTR4_NUMLINKS - 32)) |
    (1u << (FATTR4_OWNER - 32)) |
    (1u << (FATTR4_OWNER_GROUP - 32)) |
    (1u << (FATTR4_RAWDEV - 32)) |
    (1u << (FATTR4_SPACE_AVAIL - 32)) |
    (1u << (FATTR4_SPACE_FREE - 32)) |
    (1u << (FATTR4_SPACE_TOTAL - 32)) |
    (1u << (FATTR4_SPACE_USED - 32)) |
    (1u << (FATTR4_TIME_ACCESS - 32)) |
    (1u << (FATTR4_TIME_BACKUP - 32)) |
    (1u << (FATTR4_TIME_CREATE - 32)) |
    (1u << (FATTR4_TIME_METADATA - 32)) |
    (1u << (FATTR4_TIME_MODIFY - 32)) |
    (1u << (FATTR4_MOUNTED_ON_FILEID - 32))
};

static void decode_bitmap(xdr_buf_t *xdr, uint32_t *bitmap, int *nwords)
{
    *nwords = (int)xdr_decode_uint32(xdr);
    for (int i = 0; i < *nwords && i < 2; i++)
        bitmap[i] = xdr_decode_uint32(xdr);
    /* Skip extra words if any */
    for (int i = 2; i < *nwords; i++)
        xdr_decode_uint32(xdr);
    if (*nwords > 2) *nwords = 2;
}

static inline int bitmap_isset(const uint32_t *bitmap, int nwords, int bit)
{
    int word = bit / 32;
    if (word >= nwords) return 0;
    return (bitmap[word] >> (bit % 32)) & 1;
}

/* A change attribute (RFC 7530 s5.4) never reported before.  These have
 * the top bit set, so they never equal a filesystem's own label. */
static uint64_t fresh_change(const darwinfuse_config_t *config)
{
    return FUSE_CHANGE_UNLABELED |
           (atomic_fetch_add_explicit(
                (atomic_uint_fast64_t *)&config->fresh_change,
                1,
                memory_order_relaxed) + 1);
}

/* The change attribute of the object st describes: the label the
 * filesystem carried with st, else a fresh value, so the client never
 * trusts unlabeled cached state past a revalidation. */
static uint64_t change_attribute(const darwinfuse_config_t *config,
                                 const struct stat *st)
{
    uint64_t label = FUSE_STAT_CHANGE(st);
    if (label != 0 && label < FUSE_CHANGE_UNLABELED)
        return label;
    return fresh_change(config);
}

static uint64_t namespace_change(const darwinfuse_config_t *config)
{
    return atomic_load_explicit(&config->namespace_change, memory_order_acquire);
}

/* Record a completed namespace mutation: retire READDIR continuations and
 * return a change value after it.  Change info is always reported
 * non-atomic, so the client revalidates the directory rather than trusting
 * these values as its attribute. */
static uint64_t namespace_changed(const darwinfuse_config_t *config)
{
    atomic_fetch_add_explicit(
        (atomic_uint_fast64_t *)&config->namespace_change,
        1,
        memory_order_acq_rel);
    return fresh_change(config);
}

static void encode_cookie_verifier(uint64_t revision, uint8_t verifier[8])
{
    for (unsigned index = 0; index < 8; index++)
        verifier[7U - index] = (uint8_t)(revision >> (index * 8U));
}

static int readdir_cookie_is_current(uint64_t cookie,
                                     const uint8_t verifier[8],
                                     uint64_t revision)
{
    uint8_t expected[8];
    if (cookie == 0) return 1;
    encode_cookie_verifier(revision, expected);
    return memcmp(verifier, expected, sizeof(expected)) == 0;
}

/*
 * change_info4 (RFC 7530 s3.3.5).  Atomic info whose `before` matches the
 * client's cached directory change lets it keep its cached names; anything
 * else makes it revalidate the directory.  A mutation is never reported
 * atomic: another surface may have changed the directory alongside it.
 */
static void encode_change_info(xdr_buf_t *reply, int atomic,
                               uint64_t before, uint64_t after)
{
    xdr_encode_bool(reply, atomic);
    xdr_encode_uint64(reply, before);
    xdr_encode_uint64(reply, after);
}

/* The change attribute of the directory at path, or 0 when unknown. */
static uint64_t directory_change(const darwinfuse_config_t *config,
                                 const char *path)
{
    struct stat st;
    memset(&st, 0, sizeof(st));
    if (!config->ops->getattr || config->ops->getattr(path, &st) != 0)
        return 0;
    return change_attribute(config, &st);
}

/*
 * Encode fattr4 for a given stat result.
 * Only encodes attributes that are both requested AND supported.
 * Attributes MUST be encoded in bit order (RFC 7530 §2.8).
 */
/*
 * type_override: 0 = auto-detect from st_mode, otherwise use this NFS type
 * (e.g. NF4ATTRDIR, NF4NAMEDATTR for xattr virtual inodes)
 */
static void encode_fattr4(xdr_buf_t *xdr,
                           const struct stat *st,
                           const uint32_t *req_bitmap, int req_nwords,
                           const uint8_t *fh, uint32_t fh_len,
                           const darwinfuse_config_t *config,
                           uint32_t type_override)
{
    /* Compute effective bitmap (intersection of requested and supported) */
    uint32_t eff[2] = {0, 0};
    int eff_nwords = req_nwords < 2 ? req_nwords : 2;
    for (int i = 0; i < eff_nwords; i++)
        eff[i] = req_bitmap[i] & supported_bitmap[i];

    /* Determine how many bitmap words to encode (strip trailing zeros) */
    int enc_nwords = 0;
    if (eff[1]) enc_nwords = 2;
    else if (eff[0]) enc_nwords = 1;

    /* Encode bitmap */
    xdr_encode_uint32(xdr, (uint32_t)enc_nwords);
    for (int i = 0; i < enc_nwords; i++)
        xdr_encode_uint32(xdr, eff[i]);

    /* Encode attribute values into a temporary buffer, then emit as opaque */
    uint8_t attr_buf[4096];
    xdr_buf_t attr;
    xdr_init(&attr, attr_buf, sizeof(attr_buf));

    /* Helper to check if a specific attribute bit is set */
    #define ATTR_SET(bit) bitmap_isset(eff, 2, (bit))

    /* Check if any statfs bits are requested — fetch once */
    int need_statfs = ATTR_SET(FATTR4_FILES_AVAIL) ||
                      ATTR_SET(FATTR4_FILES_FREE) ||
                      ATTR_SET(FATTR4_FILES_TOTAL) ||
                      ATTR_SET(FATTR4_SPACE_AVAIL) ||
                      ATTR_SET(FATTR4_SPACE_FREE) ||
                      ATTR_SET(FATTR4_SPACE_TOTAL);
    struct statvfs stvfs;
    memset(&stvfs, 0, sizeof(stvfs));
    if (need_statfs && config && config->ops->statfs) {
        config->ops->statfs("/", &stvfs);
    }

    /* Word 0 attributes (bits 0-31), in order */

    if (ATTR_SET(FATTR4_SUPPORTED_ATTRS)) {
        /* Encode our supported bitmap */
        xdr_encode_uint32(&attr, 2);  /* 2 words */
        xdr_encode_uint32(&attr, supported_bitmap[0]);
        xdr_encode_uint32(&attr, supported_bitmap[1]);
    }

    if (ATTR_SET(FATTR4_TYPE)) {
        uint32_t nfs_type;
        if (type_override) {
            nfs_type = type_override;
        } else if (S_ISDIR(st->st_mode))       nfs_type = NF4DIR;
        else if (S_ISREG(st->st_mode))  nfs_type = NF4REG;
        else if (S_ISLNK(st->st_mode))  nfs_type = NF4LNK;
        else if (S_ISBLK(st->st_mode))  nfs_type = NF4BLK;
        else if (S_ISCHR(st->st_mode))  nfs_type = NF4CHR;
        else if (S_ISFIFO(st->st_mode)) nfs_type = NF4FIFO;
        else if (S_ISSOCK(st->st_mode)) nfs_type = NF4SOCK;
        else                             nfs_type = NF4REG;
        xdr_encode_uint32(&attr, nfs_type);
    }

    if (ATTR_SET(FATTR4_FH_EXPIRE_TYPE)) {
        xdr_encode_uint32(&attr, FH4_PERSISTENT);
    }

    if (ATTR_SET(FATTR4_CHANGE)) {
        xdr_encode_uint64(&attr, change_attribute(config, st));
    }

    if (ATTR_SET(FATTR4_SIZE)) {
        xdr_encode_uint64(&attr, (uint64_t)st->st_size);
    }

    if (ATTR_SET(FATTR4_LINK_SUPPORT)) {
        xdr_encode_bool(&attr, 1);  /* hard links supported */
    }

    if (ATTR_SET(FATTR4_SYMLINK_SUPPORT)) {
        xdr_encode_bool(&attr, 1);  /* symlinks supported */
    }

    if (ATTR_SET(FATTR4_NAMED_ATTR)) {
        int has_xattr = config && (config->ops->getxattr ||
                                   config->ops->listxattr);
        xdr_encode_bool(&attr, has_xattr ? 1 : 0);
    }

    if (ATTR_SET(FATTR4_FSID)) {
        /* fsid4 = { major: uint64, minor: uint64 } */
        xdr_encode_uint64(&attr, 0);  /* major */
        xdr_encode_uint64(&attr, 1);  /* minor — distinguish from root fs */
    }

    if (ATTR_SET(FATTR4_UNIQUE_HANDLES)) {
        xdr_encode_bool(&attr, 1);
    }

    if (ATTR_SET(FATTR4_LEASE_TIME)) {
        xdr_encode_uint32(&attr, 90);  /* 90-second lease */
    }

    if (ATTR_SET(FATTR4_RDATTR_ERROR)) {
        xdr_encode_uint32(&attr, NFS4_OK);
    }

    if (ATTR_SET(FATTR4_FILEHANDLE)) {
        xdr_encode_opaque(&attr, fh, fh_len);
    }

    if (ATTR_SET(FATTR4_FILEID)) {
        xdr_encode_uint64(&attr, (uint64_t)fh_get_ino(fh, fh_len));
    }

    if (ATTR_SET(FATTR4_FILES_AVAIL)) {
        xdr_encode_uint64(&attr, (uint64_t)stvfs.f_favail);
    }

    if (ATTR_SET(FATTR4_FILES_FREE)) {
        xdr_encode_uint64(&attr, (uint64_t)stvfs.f_ffree);
    }

    if (ATTR_SET(FATTR4_FILES_TOTAL)) {
        xdr_encode_uint64(&attr, (uint64_t)stvfs.f_files);
    }

    if (ATTR_SET(FATTR4_MAXFILESIZE)) {
        xdr_encode_uint64(&attr, 0x7FFFFFFFFFFFFFFFULL);
    }

    if (ATTR_SET(FATTR4_MAXLINK)) {
        xdr_encode_uint32(&attr, 32000);
    }

    if (ATTR_SET(FATTR4_MAXNAME)) {
        xdr_encode_uint32(&attr, 255);
    }

    if (ATTR_SET(FATTR4_MAXREAD)) {
        xdr_encode_uint64(&attr, DFUSE_IO_SIZE);
    }

    if (ATTR_SET(FATTR4_MAXWRITE)) {
        xdr_encode_uint64(&attr, DFUSE_IO_SIZE);
    }

    /* Word 1 attributes (bits 32+), in order */

    if (ATTR_SET(FATTR4_MODE)) {
        xdr_encode_uint32(&attr, st->st_mode & 07777);
    }

    if (ATTR_SET(FATTR4_NUMLINKS)) {
        xdr_encode_uint32(&attr, (uint32_t)st->st_nlink);
    }

    if (ATTR_SET(FATTR4_OWNER)) {
        char owner_str[32];
        snprintf(owner_str, sizeof(owner_str), "%u", (unsigned)st->st_uid);
        xdr_encode_string(&attr, owner_str);
    }

    if (ATTR_SET(FATTR4_OWNER_GROUP)) {
        char group_str[32];
        snprintf(group_str, sizeof(group_str), "%u", (unsigned)st->st_gid);
        xdr_encode_string(&attr, group_str);
    }

    if (ATTR_SET(FATTR4_RAWDEV)) {
        /* specdata4 = { specdata1: uint32, specdata2: uint32 } */
        xdr_encode_uint32(&attr, major(st->st_rdev));
        xdr_encode_uint32(&attr, minor(st->st_rdev));
    }

    if (ATTR_SET(FATTR4_SPACE_AVAIL)) {
        xdr_encode_uint64(&attr, (uint64_t)stvfs.f_bavail * (uint64_t)stvfs.f_frsize);
    }

    if (ATTR_SET(FATTR4_SPACE_FREE)) {
        xdr_encode_uint64(&attr, (uint64_t)stvfs.f_bfree * (uint64_t)stvfs.f_frsize);
    }

    if (ATTR_SET(FATTR4_SPACE_TOTAL)) {
        xdr_encode_uint64(&attr, (uint64_t)stvfs.f_blocks * (uint64_t)stvfs.f_frsize);
    }

    if (ATTR_SET(FATTR4_SPACE_USED)) {
        xdr_encode_uint64(&attr, (uint64_t)st->st_blocks * 512);
    }

    if (ATTR_SET(FATTR4_TIME_ACCESS)) {
        /* nfstime4 = { seconds: int64, nseconds: uint32 } */
        xdr_encode_int64(&attr, (int64_t)st->st_atime);
#ifdef __APPLE__
        xdr_encode_uint32(&attr, (uint32_t)st->st_atimespec.tv_nsec);
#else
        xdr_encode_uint32(&attr, 0);
#endif
    }

    if (ATTR_SET(FATTR4_TIME_BACKUP)) {
        struct timespec bkup = {0, 0};
#ifdef __APPLE__
        if (config->ops->getxtimes) {
            char *xt_path = fh_to_path(config, fh, fh_len);
            if (xt_path) {
                struct timespec cr = {0, 0};
                config->ops->getxtimes(xt_path, &bkup, &cr);
                free(xt_path);
            }
        }
#endif
        xdr_encode_int64(&attr, (int64_t)bkup.tv_sec);
        xdr_encode_uint32(&attr, (uint32_t)bkup.tv_nsec);
    }

    if (ATTR_SET(FATTR4_TIME_CREATE)) {
#ifdef __APPLE__
        xdr_encode_int64(&attr, (int64_t)st->st_birthtimespec.tv_sec);
        xdr_encode_uint32(&attr, (uint32_t)st->st_birthtimespec.tv_nsec);
#else
        xdr_encode_int64(&attr, 0);
        xdr_encode_uint32(&attr, 0);
#endif
    }

    if (ATTR_SET(FATTR4_TIME_METADATA)) {
        xdr_encode_int64(&attr, (int64_t)st->st_ctime);
#ifdef __APPLE__
        xdr_encode_uint32(&attr, (uint32_t)st->st_ctimespec.tv_nsec);
#else
        xdr_encode_uint32(&attr, 0);
#endif
    }

    if (ATTR_SET(FATTR4_TIME_MODIFY)) {
        xdr_encode_int64(&attr, (int64_t)st->st_mtime);
#ifdef __APPLE__
        xdr_encode_uint32(&attr, (uint32_t)st->st_mtimespec.tv_nsec);
#else
        xdr_encode_uint32(&attr, 0);
#endif
    }

    if (ATTR_SET(FATTR4_MOUNTED_ON_FILEID)) {
        xdr_encode_uint64(&attr, (uint64_t)fh_get_ino(fh, fh_len));
    }

    #undef ATTR_SET

    /* Encode attr_data as opaque (length + data) */
    xdr_encode_opaque(xdr, attr_buf, (uint32_t)xdr_getpos(&attr));
}

/* ---- Individual operation handlers ---- */

static uint32_t handle_putrootfh(const darwinfuse_config_t *config,
                                  nfs4_conn_state_t *conn,
                                  nfs4_request_ctx_t *ctx,
                                  xdr_buf_t *req, xdr_buf_t *rep)
{
    (void)config; (void)req;
    fh_set_ino(ctx->current_fh, &ctx->current_fh_len, DFUSE_INO_ROOT);
    return NFS4_OK;
}

static uint32_t handle_putfh(const darwinfuse_config_t *config,
                              nfs4_conn_state_t *conn,
                              nfs4_request_ctx_t *ctx,
                              xdr_buf_t *req, xdr_buf_t *rep)
{
    uint8_t fh[128];
    uint32_t fh_len = xdr_decode_opaque(req, fh, sizeof(fh));
    if (req->error) return NFS4ERR_BADHANDLE;

    dfuse_ino_t ino = fh_get_ino(fh, fh_len);
    if (ino == 0) return NFS4ERR_BADHANDLE;

    /* Verify the inode still exists in the table */
    if (!dfuse_itable_contains(config->inode_table, ino))
        return NFS4ERR_STALE;

    memcpy(ctx->current_fh, fh, fh_len);
    ctx->current_fh_len = fh_len;
    return NFS4_OK;
}

static uint32_t handle_getfh(const darwinfuse_config_t *config,
                              nfs4_conn_state_t *conn,
                              nfs4_request_ctx_t *ctx,
                              xdr_buf_t *req, xdr_buf_t *rep)
{
    (void)config; (void)req;
    if (ctx->current_fh_len == 0)
        return NFS4ERR_NOENT;
    xdr_encode_opaque(rep, ctx->current_fh, ctx->current_fh_len);
    return NFS4_OK;
}

static uint32_t handle_savefh(const darwinfuse_config_t *config,
                               nfs4_conn_state_t *conn,
                               nfs4_request_ctx_t *ctx,
                               xdr_buf_t *req, xdr_buf_t *rep)
{
    (void)config; (void)req; (void)rep;
    memcpy(ctx->saved_fh, ctx->current_fh, ctx->current_fh_len);
    ctx->saved_fh_len = ctx->current_fh_len;
    return NFS4_OK;
}

static uint32_t handle_restorefh(const darwinfuse_config_t *config,
                                  nfs4_conn_state_t *conn,
                                  nfs4_request_ctx_t *ctx,
                                  xdr_buf_t *req, xdr_buf_t *rep)
{
    (void)config; (void)req; (void)rep;
    if (ctx->saved_fh_len == 0)
        return NFS4ERR_RESTOREFH;
    memcpy(ctx->current_fh, ctx->saved_fh, ctx->saved_fh_len);
    ctx->current_fh_len = ctx->saved_fh_len;
    return NFS4_OK;
}

static uint32_t handle_lookup(const darwinfuse_config_t *config,
                               nfs4_conn_state_t *conn,
                               nfs4_request_ctx_t *ctx,
                               xdr_buf_t *req, xdr_buf_t *rep)
{
    dfuse_ino_t dir_ino = fh_get_ino(ctx->current_fh, ctx->current_fh_len);
    if (dir_ino == 0) return NFS4ERR_BADHANDLE;

    dfuse_ino_type_t dir_type = dfuse_itable_type(config->inode_table, dir_ino);

    char name[256];
    xdr_decode_string(req, name, sizeof(name));
    if (req->error) return NFS4ERR_INVAL;

    if (dir_type == DFUSE_INO_ATTRDIR) {
        /* Lookup a named attribute within an attrdir */
        char *file_path = xattr_file_path(config, dir_ino);
        if (!file_path) return NFS4ERR_STALE;

        /* Verify the xattr exists by querying its size */
        if (config->ops->getxattr) {
            int sz = FUSE_GETXATTR(config->ops, file_path, name, NULL, 0);
            if (sz < 0) {
                free(file_path);
                return errno_to_nfs4(sz);
            }
        } else {
            free(file_path);
            return NFS4ERR_NOENT;
        }

        free(file_path);

        dfuse_ino_t na_ino = dfuse_itable_get_namedattr(
            config->inode_table, dir_ino, name);
        if (na_ino == 0) return NFS4ERR_SERVERFAULT;

        fh_set_ino(ctx->current_fh, &ctx->current_fh_len, na_ino);
        return NFS4_OK;
    }

    /* Regular directory lookup */
    if (!config->ops->getattr) return NFS4ERR_IO;

    char *dir_path = dfuse_itable_path_dup(config->inode_table, dir_ino);
    if (!dir_path) return NFS4ERR_STALE;

    char child_path[1024];
    build_child_path(child_path, sizeof(child_path), dir_path, name);

    DFUSE_LOG("  LOOKUP dir='%s' name='%s' -> '%s'", dir_path, name, child_path);

    /* A resolved child proves its parent is a directory, so only a failed
     * lookup needs the parent's type to report NOTDIR. */
    struct stat st;
    memset(&st, 0, sizeof(st));
    int rc = config->ops->getattr(child_path, &st);
    if (rc != 0) {
        DFUSE_LOG("  LOOKUP '%s' -> error %d", child_path, rc);
        memset(&st, 0, sizeof(st));
        int dir_rc = config->ops->getattr(dir_path, &st);
        free(dir_path);
        if (dir_rc != 0) return errno_to_nfs4(dir_rc);
        return S_ISDIR(st.st_mode) ? errno_to_nfs4(rc) : NFS4ERR_NOTDIR;
    }
    free(dir_path);

    dfuse_ino_t child_ino = dfuse_itable_get_or_create(config->inode_table, child_path);
    if (child_ino == 0) return NFS4ERR_SERVERFAULT;

    fh_set_ino(ctx->current_fh, &ctx->current_fh_len, child_ino);
    return NFS4_OK;
}

static uint32_t handle_lookupp(const darwinfuse_config_t *config,
                                nfs4_conn_state_t *conn,
                                nfs4_request_ctx_t *ctx,
                                xdr_buf_t *req, xdr_buf_t *rep)
{
    (void)req; (void)rep;

    char *path = fh_to_path(config, ctx->current_fh, ctx->current_fh_len);
    if (!path) return NFS4ERR_STALE;

    /* Root has no parent */
    if (strcmp(path, "/") == 0) {
        free(path);
        return NFS4ERR_NOENT;
    }

    /* Find last '/' to derive parent path */
    char *slash = strrchr(path, '/');
    if (!slash) {
        free(path);
        return NFS4ERR_SERVERFAULT;
    }

    char parent[1024];
    if (slash == path) {
        /* Parent is root */
        parent[0] = '/';
        parent[1] = '\0';
    } else {
        size_t len = (size_t)(slash - path);
        if (len >= sizeof(parent)) len = sizeof(parent) - 1;
        memcpy(parent, path, len);
        parent[len] = '\0';
    }
    free(path);

    /* Verify parent exists */
    if (config->ops->getattr) {
        struct stat st;
        memset(&st, 0, sizeof(st));
        int rc = config->ops->getattr(parent, &st);
        if (rc != 0) return errno_to_nfs4(rc);
    }

    dfuse_ino_t parent_ino = dfuse_itable_get_or_create(config->inode_table, parent);
    if (parent_ino == 0) return NFS4ERR_SERVERFAULT;

    fh_set_ino(ctx->current_fh, &ctx->current_fh_len, parent_ino);
    return NFS4_OK;
}

/*
 * Attributes of the current filehandle's object, as GETATTR, VERIFY, and
 * NVERIFY report them. type_override is as for encode_fattr4().
 */
static uint32_t current_attributes(const darwinfuse_config_t *config,
                                   nfs4_conn_state_t *conn,
                                   const nfs4_request_ctx_t *ctx,
                                   struct stat *st, uint32_t *type_override)
{
    dfuse_ino_t ino = fh_get_ino(ctx->current_fh, ctx->current_fh_len);
    if (ino == 0) return NFS4ERR_BADHANDLE;

    dfuse_ino_type_t ino_type = dfuse_itable_type(config->inode_table, ino);

    memset(st, 0, sizeof(*st));
    *type_override = 0;

    if (ino_type == DFUSE_INO_ATTRDIR) {
        /* Named attribute directory — synthetic stat */
        st->st_mode = S_IFDIR | 0755;
        st->st_nlink = 2;
        st->st_uid = config->uid;
        st->st_gid = config->gid;
        *type_override = NF4ATTRDIR;
        return NFS4_OK;
    }
    if (ino_type == DFUSE_INO_NAMEDATTR) {
        /* Named attribute — get size via getxattr */
        char *file_path = xattr_file_path(config, ino);
        char *attr_name = dfuse_itable_attr_name_dup(config->inode_table, ino);
        if (!file_path || !attr_name) {
            free(file_path);
            free(attr_name);
            return NFS4ERR_STALE;
        }

        st->st_mode = S_IFREG | 0644;
        st->st_nlink = 1;
        st->st_uid = config->uid;
        st->st_gid = config->gid;

        if (config->ops->getxattr) {
            int sz = FUSE_GETXATTR(config->ops, file_path, attr_name, NULL, 0);
            if (sz >= 0)
                st->st_size = sz;
        }
        free(file_path);
        free(attr_name);
        *type_override = NF4NAMEDATTR;
        return NFS4_OK;
    }

    /* Regular file/directory */
    char *path = dfuse_itable_path_dup(config->inode_table, ino);
    if (!path) return NFS4ERR_STALE;

    int rc;
    struct fuse_file_info fi;
    if (config->ops->fgetattr && open_file_info(conn, NULL, ino, &fi)) {
        rc = config->ops->fgetattr(path, st, &fi);
    } else if (config->ops->getattr) {
        rc = config->ops->getattr(path, st);
    } else {
        rc = -ENOTSUP;
    }
    free(path);
    return errno_to_nfs4(rc);
}

static uint32_t handle_getattr(const darwinfuse_config_t *config,
                                nfs4_conn_state_t *conn,
                                nfs4_request_ctx_t *ctx,
                                xdr_buf_t *req, xdr_buf_t *rep)
{
    /* Decode requested bitmap */
    uint32_t req_bitmap[2] = {0, 0};
    int req_nwords = 0;
    decode_bitmap(req, req_bitmap, &req_nwords);
    if (req->error) return NFS4ERR_INVAL;

    struct stat st;
    uint32_t type_override;
    uint32_t status = current_attributes(config, conn, ctx, &st, &type_override);
    if (status != NFS4_OK) return status;

    encode_fattr4(rep, &st, req_bitmap, req_nwords,
                  ctx->current_fh, ctx->current_fh_len, config,
                  type_override);
    return NFS4_OK;
}

static uint32_t handle_setattr(const darwinfuse_config_t *config,
                                nfs4_conn_state_t *conn,
                                nfs4_request_ctx_t *ctx,
                                xdr_buf_t *req, xdr_buf_t *rep)
{
    /* stateid4 */
    xdr_decode_uint32(req);  /* seqid */
    uint8_t sid_other[12];
    xdr_decode_opaque_fixed(req, sid_other, 12);

    /* Decode attribute bitmap + data */
    uint32_t bitmap[2] = {0, 0};
    int nwords = 0;
    decode_bitmap(req, bitmap, &nwords);

    /* Decode attr data as opaque */
    uint8_t attr_data[4096];
    uint32_t attr_data_len = xdr_decode_opaque(req, attr_data, sizeof(attr_data));
    if (req->error) return NFS4ERR_INVAL;

    dfuse_ino_t ino = fh_get_ino(ctx->current_fh, ctx->current_fh_len);
    char *path = fh_to_path(config, ctx->current_fh, ctx->current_fh_len);
    if (!path) return NFS4ERR_STALE;

    /* Parse and apply attributes from attr_data */
    xdr_buf_t ad;
    xdr_init(&ad, attr_data, attr_data_len);

    uint32_t attrsset[2] = {0, 0};

    /*
     * Parse all requested attribute values from XDR in bit order.
     * We parse into local variables first, then apply via either
     * setattr_x (combined, preferred on macOS) or individual callbacks.
     */
    uint64_t set_size = 0;
    int has_size = 0;
    uint32_t set_mode = 0;
    int has_mode = 0;
    uid_t set_uid = (uid_t)-1;
    int has_uid = 0;
    gid_t set_gid = (gid_t)-1;
    int has_gid = 0;
    struct timespec set_atime = {0, UTIME_OMIT};
    int has_atime = 0;
    struct timespec set_mtime = {0, UTIME_OMIT};
    int has_mtime = 0;

    /* Decode in bit order */
    if (bitmap_isset(bitmap, nwords, FATTR4_SIZE)) {
        set_size = xdr_decode_uint64(&ad);
        has_size = !ad.error;
    }

    if (bitmap_isset(bitmap, nwords, FATTR4_MODE)) {
        set_mode = xdr_decode_uint32(&ad);
        has_mode = !ad.error;
    }

    if (bitmap_isset(bitmap, nwords, FATTR4_OWNER)) {
        char owner_str[64];
        xdr_decode_string(&ad, owner_str, sizeof(owner_str));
        if (!ad.error) {
            set_uid = (uid_t)strtoul(owner_str, NULL, 10);
            has_uid = 1;
        }
    }

    if (bitmap_isset(bitmap, nwords, FATTR4_OWNER_GROUP)) {
        char group_str[64];
        xdr_decode_string(&ad, group_str, sizeof(group_str));
        if (!ad.error) {
            set_gid = (gid_t)strtoul(group_str, NULL, 10);
            has_gid = 1;
        }
    }

    if (bitmap_isset(bitmap, nwords, FATTR4_TIME_ACCESS_SET)) {
        uint32_t how = xdr_decode_uint32(&ad);
        if (how == 0) {
            clock_gettime(CLOCK_REALTIME, &set_atime);
        } else {
            set_atime.tv_sec = (time_t)xdr_decode_int64(&ad);
            set_atime.tv_nsec = (long)xdr_decode_uint32(&ad);
        }
        has_atime = !ad.error;
    }

    if (bitmap_isset(bitmap, nwords, FATTR4_TIME_MODIFY_SET)) {
        uint32_t mhow = xdr_decode_uint32(&ad);
        if (mhow == 0) {
            clock_gettime(CLOCK_REALTIME, &set_mtime);
        } else {
            set_mtime.tv_sec = (time_t)xdr_decode_int64(&ad);
            set_mtime.tv_nsec = (long)xdr_decode_uint32(&ad);
        }
        has_mtime = !ad.error;
    }

#ifdef __APPLE__
    /*
     * macFUSE setattr_x path: combine all attributes into a single call.
     * This is preferred when available as it's atomic and supports
     * extended attributes (crtime, bkuptime, chgtime, flags).
     */
    if (config->ops->setattr_x) {
        struct setattr_x sa;
        memset(&sa, 0, sizeof(sa));

        if (has_size)  { sa.valid |= (1 << 3); sa.size = (off_t)set_size; }
        if (has_mode)  { sa.valid |= (1 << 0); sa.mode = (mode_t)set_mode; }
        if (has_uid)   { sa.valid |= (1 << 1); sa.uid = set_uid; }
        if (has_gid)   { sa.valid |= (1 << 2); sa.gid = set_gid; }
        if (has_atime) { sa.valid |= (1 << 4); sa.acctime = set_atime; }
        if (has_mtime) { sa.valid |= (1 << 5); sa.modtime = set_mtime; }

        int rc = -1;

        /* Prefer fsetattr_x if the file is open */
        struct fuse_file_info fi;
        if (config->ops->fsetattr_x &&
            open_file_info(conn, sid_other, ino, &fi))
            rc = config->ops->fsetattr_x(path, &sa, &fi);

        if (rc != 0)
            rc = config->ops->setattr_x(path, &sa);

        if (rc == 0) {
            if (has_size)  attrsset[0] |= (1u << FATTR4_SIZE);
            if (has_mode)  attrsset[1] |= (1u << (FATTR4_MODE - 32));
            if (has_uid)   attrsset[1] |= (1u << (FATTR4_OWNER - 32));
            if (has_gid)   attrsset[1] |= (1u << (FATTR4_OWNER_GROUP - 32));
            if (has_atime) attrsset[1] |= (1u << (FATTR4_TIME_ACCESS_SET - 32));
            if (has_mtime) attrsset[1] |= (1u << (FATTR4_TIME_MODIFY_SET - 32));
        }
        goto setattr_reply;
    }
#endif /* __APPLE__ */

    /* Standard path: apply attributes via individual FUSE callbacks */

    if (has_size) {
        int rc;
        struct fuse_file_info fi;
        if (config->ops->ftruncate &&
            open_file_info(conn, sid_other, ino, &fi)) {
            rc = config->ops->ftruncate(path, (off_t)set_size, &fi);
        } else {
            if (!config->ops->truncate) {
                free(path);
                return NFS4ERR_NOTSUPP;
            }
            rc = config->ops->truncate(path, (off_t)set_size);
        }
        if (rc == 0) attrsset[0] |= (1u << FATTR4_SIZE);
    }

    if (has_mode && config->ops->chmod) {
        int rc = config->ops->chmod(path, (mode_t)set_mode);
        if (rc == 0) attrsset[1] |= (1u << (FATTR4_MODE - 32));
    }

    if (has_uid && config->ops->chown) {
        int rc = config->ops->chown(path, set_uid, (gid_t)-1);
        if (rc == 0) attrsset[1] |= (1u << (FATTR4_OWNER - 32));
    }

    if (has_gid && config->ops->chown) {
        int rc = config->ops->chown(path, (uid_t)-1, set_gid);
        if (rc == 0) attrsset[1] |= (1u << (FATTR4_OWNER_GROUP - 32));
    }

    if ((has_atime || has_mtime) && config->ops->utimens) {
        struct timespec tv[2] = {set_atime, set_mtime};
        int rc = config->ops->utimens(path, tv);
        if (rc == 0) {
            if (has_atime) attrsset[1] |= (1u << (FATTR4_TIME_ACCESS_SET - 32));
            if (has_mtime) attrsset[1] |= (1u << (FATTR4_TIME_MODIFY_SET - 32));
        }
    }

#ifdef __APPLE__
setattr_reply:
    ;  /* label requires a statement before declarations */
#endif
    {
    /* Reply: attrsset bitmap */
    int enc_nwords = 0;
    if (attrsset[1]) enc_nwords = 2;
    else if (attrsset[0]) enc_nwords = 1;
    xdr_encode_uint32(rep, (uint32_t)enc_nwords);
    for (int i = 0; i < enc_nwords; i++)
        xdr_encode_uint32(rep, attrsset[i]);
    }

    free(path);
    return NFS4_OK;
}

static int caller_in_group(const struct fuse_context *caller, gid_t gid)
{
    if (caller->gid == gid)
        return 1;
    gid_t groups[RPC_AUTH_SYS_MAX_GROUPS];
    int count = fuse_getgroups(RPC_AUTH_SYS_MAX_GROUPS, groups);
    for (int i = 0; i < count; i++) {
        if (groups[i] == gid)
            return 1;
    }
    return 0;
}

/*
 * ACCESS4 rights a caller holds on an object, by the default_permissions
 * rules the Linux FUSE mount also enforces: the caller's AUTH_SYS identity
 * selects the owner, group, or other mode bits.  Root holds every right
 * except executing a file no one may execute.  A directory's write bit
 * grants deleting its entries; deleting a non-directory is decided by the
 * directory that holds it, so the file itself never withholds DELETE.
 */
static uint32_t access_granted(const struct stat *st,
                               const struct fuse_context *caller)
{
    int directory = S_ISDIR(st->st_mode);
    unsigned bits;
    if (caller->uid == 0) {
        bits = 06 | ((directory || (st->st_mode & 0111)) ? 01 : 0);
    } else if (caller->uid == st->st_uid) {
        bits = (st->st_mode >> 6) & 07;
    } else if (caller_in_group(caller, st->st_gid)) {
        bits = (st->st_mode >> 3) & 07;
    } else {
        bits = st->st_mode & 07;
    }

    uint32_t granted = directory ? 0 : ACCESS4_DELETE;
    if (bits & 04)
        granted |= ACCESS4_READ;
    if (bits & 02)
        granted |= ACCESS4_MODIFY | ACCESS4_EXTEND |
                   (directory ? ACCESS4_DELETE : 0);
    if (bits & 01)
        granted |= directory ? ACCESS4_LOOKUP : ACCESS4_EXECUTE;
    return granted;
}

static uint32_t handle_access(const darwinfuse_config_t *config,

                               nfs4_conn_state_t *conn,
                               nfs4_request_ctx_t *ctx,
                               xdr_buf_t *req, xdr_buf_t *rep)
{
    uint32_t requested = xdr_decode_uint32(req);
    if (req->error) return NFS4ERR_INVAL;

    dfuse_ino_t ino = fh_get_ino(ctx->current_fh, ctx->current_fh_len);
    if (ino == 0) return NFS4ERR_BADHANDLE;
    dfuse_ino_type_t type = dfuse_itable_type(config->inode_table, ino);
    int is_attribute_inode = type == DFUSE_INO_ATTRDIR ||
                             type == DFUSE_INO_NAMEDATTR;
    char *path = is_attribute_inode
        ? xattr_file_path(config, ino)
        : fh_to_path(config, ctx->current_fh, ctx->current_fh_len);
    if (!path) return NFS4ERR_STALE;

    /* Attribute inodes are synthetic: authorize them against their owning
     * file rather than passing the inode table's synthetic path to the
     * filesystem. */
    if (!config->ops->getattr) {
        free(path);
        return NFS4ERR_NOTSUPP;
    }
    struct stat st;
    memset(&st, 0, sizeof(st));
    int rc = config->ops->getattr(path, &st);
    free(path);
    if (rc != 0) return errno_to_nfs4(rc);

    uint32_t granted = access_granted(&st, fuse_get_context());
    if (is_attribute_inode) {
        /* An attribute directory and its streams carry the owning file's
         * read and write rights; streams are data, never executable. */
        uint32_t rights = 0;
        if (granted & ACCESS4_READ)
            rights |= ACCESS4_READ | ACCESS4_LOOKUP;
        if (granted & ACCESS4_MODIFY)
            rights |= ACCESS4_MODIFY | ACCESS4_EXTEND | ACCESS4_DELETE;
        granted = rights;
    }
    granted &= requested;

    /* Encode: supported, access */
    xdr_encode_uint32(rep, requested);  /* supported */
    xdr_encode_uint32(rep, granted);    /* access */
    return NFS4_OK;
}

/* ---- READDIR ---- */

typedef struct {
    char     name[256];
    uint64_t cookie;
    struct stat attributes;
} readdir_entry_t;

typedef struct {
    readdir_entry_t *entries;
    int count;
    int cap;
    int limit;
    uint32_t directory_bytes;
    uint32_t directory_limit;
    int full;
} readdir_collector_t;

static int readdir_filler(void *buf, const char *name,
                           const struct stat *stbuf, off_t off)
{
    readdir_collector_t *col = (readdir_collector_t *)buf;

    if (strcmp(name, ".") == 0 || strcmp(name, "..") == 0)
        return 0;

    size_t name_len = strlen(name);
    uint32_t directory_bytes = 16U + (uint32_t)((name_len + 3U) & ~3U);
    if (col->count >= col->limit ||
        (col->directory_limit > 0 &&
         col->directory_bytes + directory_bytes > col->directory_limit)) {
        col->full = 1;
        return 1;
    }

    /* Grow if needed */
    if (col->count >= col->cap) {
        int new_cap = col->cap ? col->cap * 2 : 128;
        if (new_cap > col->limit) new_cap = col->limit;
        readdir_entry_t *new_arr = realloc(col->entries,
                                            (size_t)new_cap * sizeof(*new_arr));
        if (!new_arr) {
            col->full = 1;
            return 1;  /* stop filling on OOM */
        }
        col->entries = new_arr;
        col->cap = new_cap;
    }

    readdir_entry_t *e = &col->entries[col->count];
    strncpy(e->name, name, sizeof(e->name) - 1);
    e->name[sizeof(e->name) - 1] = '\0';
    e->cookie = (uint64_t)off;
    if (stbuf)
        e->attributes = *stbuf;
    else
        memset(&e->attributes, 0, sizeof(e->attributes));
    col->count++;
    col->directory_bytes += directory_bytes;
    return 0;
}

static uint32_t handle_readdir(const darwinfuse_config_t *config,
                                nfs4_conn_state_t *conn,
                                nfs4_request_ctx_t *ctx,
                                xdr_buf_t *req, xdr_buf_t *rep)
{
    dfuse_ino_t dir_ino = fh_get_ino(ctx->current_fh, ctx->current_fh_len);
    if (dir_ino == 0) return NFS4ERR_BADHANDLE;

    dfuse_ino_type_t dir_type = dfuse_itable_type(config->inode_table, dir_ino);

    uint64_t cookie      = xdr_decode_uint64(req);
    uint8_t cookieverf[8];
    xdr_decode_opaque_fixed(req, cookieverf, 8);
    uint32_t dircount    = xdr_decode_uint32(req);
    uint32_t maxcount    = xdr_decode_uint32(req);

    uint32_t attr_bitmap[2] = {0, 0};
    int attr_nwords = 0;
    decode_bitmap(req, attr_bitmap, &attr_nwords);
    if (req->error) return NFS4ERR_INVAL;
    if (maxcount < 16U) return NFS4ERR_TOOSMALL;
    if (cookie > (uint64_t)INT64_MAX) return NFS4ERR_BAD_COOKIE;
    uint64_t directory_revision = namespace_change(config);
    if (!readdir_cookie_is_current(cookie, cookieverf, directory_revision))
        return NFS4ERR_BAD_COOKIE;

    /* Encode cookieverf */
    size_t reply_start = xdr_getpos(rep);
    uint8_t reply_verf[8];
    encode_cookie_verifier(directory_revision, reply_verf);
    xdr_encode_opaque_fixed(rep, reply_verf, 8);

    if (dir_type == DFUSE_INO_ATTRDIR) {
        /* Named attribute directory — list xattrs */
        char *file_path = xattr_file_path(config, dir_ino);
        if (!file_path) return NFS4ERR_STALE;

        if (!config->ops->listxattr) {
            free(file_path);
            xdr_encode_bool(rep, 0);  /* no entries */
            xdr_encode_bool(rep, 1);  /* eof */
            return NFS4_OK;
        }

        int list_size = config->ops->listxattr(file_path, NULL, 0);
        if (list_size < 0) {
            free(file_path);
            return errno_to_nfs4(list_size);
        }
        if (list_size == 0) {
            free(file_path);
            xdr_encode_bool(rep, 0);
            xdr_encode_bool(rep, 1);
            return NFS4_OK;
        }
        if ((size_t)list_size > DFUSE_XDR_MAXBUF) {
            free(file_path);
            return NFS4ERR_RESOURCE;
        }
        char *list_buf = malloc((size_t)list_size);
        if (!list_buf) {
            free(file_path);
            return NFS4ERR_RESOURCE;
        }
        int listed = config->ops->listxattr(file_path, list_buf,
                                             (size_t)list_size);
        if (listed < 0) {
            free(list_buf);
            free(file_path);
            return errno_to_nfs4(listed);
        }
        list_size = listed;

        /* Parse the bounded null-terminated list and emit one maxcount-safe page. */
        const char *p = list_buf;
        uint64_t entry_cookie = 0;
        uint32_t directory_bytes = 0;
        int full = 0;
        while (p < list_buf + list_size && *p != '\0') {
            entry_cookie++;
            size_t name_len = strlen(p);

            if (entry_cookie > cookie) {
                uint32_t next_directory_bytes =
                    16U + (uint32_t)((name_len + 3U) & ~3U);
                if (dircount > 0 &&
                    directory_bytes + next_directory_bytes > dircount) {
                    if (directory_bytes == 0) {
                        free(list_buf);
                        free(file_path);
                        xdr_setpos(rep, reply_start);
                        return NFS4ERR_TOOSMALL;
                    }
                    full = 1;
                    break;
                }
                /* Get or create namedattr inode */
                dfuse_ino_t na_ino = dfuse_itable_get_namedattr(
                    config->inode_table, dir_ino, p);

                /* Synthetic stat for named attribute */
                struct stat na_st;
                memset(&na_st, 0, sizeof(na_st));
                na_st.st_mode = S_IFREG | 0644;
                na_st.st_nlink = 1;
                na_st.st_uid = config->uid;
                na_st.st_gid = config->gid;
                if (config->ops->getxattr) {
                    int sz = FUSE_GETXATTR(config->ops, file_path, p,
                                           NULL, 0);
                    if (sz >= 0) na_st.st_size = sz;
                }

                uint8_t efh[8];
                uint32_t efh_len = 0;
                if (na_ino)
                    fh_set_ino(efh, &efh_len, na_ino);

                uint8_t entry_buf[8192];
                xdr_buf_t entry;
                xdr_init(&entry, entry_buf, sizeof(entry_buf));
                xdr_encode_bool(&entry, 1);
                xdr_encode_uint64(&entry, entry_cookie);
                xdr_encode_string(&entry, p);
                encode_fattr4(&entry, &na_st, attr_bitmap, attr_nwords,
                              efh, efh_len, config, NF4NAMEDATTR);
                size_t entry_len = xdr_getpos(&entry);
                size_t response_len = xdr_getpos(rep) - reply_start;
                if (entry.error || entry_len + 8U > xdr_remaining(rep) ||
                    entry_len + 8U > (size_t)maxcount ||
                    response_len > (size_t)maxcount - entry_len - 8U) {
                    if (directory_bytes == 0) {
                        free(list_buf);
                        free(file_path);
                        xdr_setpos(rep, reply_start);
                        return NFS4ERR_TOOSMALL;
                    }
                    full = 1;
                    break;
                }
                memcpy(rep->data + rep->pos, entry.data, entry_len);
                rep->pos += entry_len;
                directory_bytes += next_directory_bytes;
            }

            p += name_len + 1;
        }

        free(list_buf);
        free(file_path);
        xdr_encode_bool(rep, 0);  /* no more entries */
        xdr_encode_bool(rep, !full);
        return NFS4_OK;
    }

    /* Regular directory */
    char *dir_path = dfuse_itable_path_dup(config->inode_table, dir_ino);
    if (!dir_path) return NFS4ERR_STALE;

    struct stat dir_st;
    memset(&dir_st, 0, sizeof(dir_st));
    if (config->ops->getattr) {
        int rc = config->ops->getattr(dir_path, &dir_st);
        if (rc != 0) { free(dir_path); return errno_to_nfs4(rc); }
    }
    if (!S_ISDIR(dir_st.st_mode)) {
        free(dir_path);
        return NFS4ERR_NOTDIR;
    }

    readdir_collector_t collector;
    memset(&collector, 0, sizeof(collector));
    uint32_t collection_budget = maxcount;
    if (dircount > 0 && dircount < collection_budget)
        collection_budget = dircount;
    collector.limit = (int)(collection_budget / 32U);
    if (collector.limit < 1) collector.limit = 1;
    if (collector.limit > 1024) collector.limit = 1024;
    collector.directory_limit = dircount;

    struct fuse_file_info dir_fi;
    memset(&dir_fi, 0, sizeof(dir_fi));

    if (config->ops->opendir) {
        int rc = config->ops->opendir(dir_path, &dir_fi);
        if (rc != 0) {
            free(dir_path);
            return errno_to_nfs4(rc);
        }
    }

    int readdir_rc = 0;
    if (config->ops->readdir)
        readdir_rc = config->ops->readdir(dir_path, &collector, readdir_filler,
                                          (off_t)cookie, &dir_fi);

    int releasedir_rc = 0;
    if (config->ops->releasedir)
        releasedir_rc = config->ops->releasedir(dir_path, &dir_fi);
    if (readdir_rc != 0 || releasedir_rc != 0) {
        free(collector.entries);
        free(dir_path);
        return errno_to_nfs4(readdir_rc != 0 ? readdir_rc : releasedir_rc);
    }
    if (collector.full && collector.count == 0) {
        free(collector.entries);
        free(dir_path);
        xdr_setpos(rep, reply_start);
        return NFS4ERR_TOOSMALL;
    }

    int encoded_entries = 0;
    for (int i = 0; i < collector.count; i++) {
        readdir_entry_t *e = &collector.entries[i];
        char child_path[1024];
        build_child_path(child_path, sizeof(child_path), dir_path, e->name);

        dfuse_ino_t entry_ino = dfuse_itable_get_or_create(config->inode_table,
                                                            child_path);
        uint8_t efh[8];
        uint32_t efh_len = 0;
        if (entry_ino)
            fh_set_ino(efh, &efh_len, entry_ino);

        uint8_t entry_buf[8192];
        xdr_buf_t entry;
        xdr_init(&entry, entry_buf, sizeof(entry_buf));
        xdr_encode_bool(&entry, 1);
        xdr_encode_uint64(&entry, e->cookie);
        xdr_encode_string(&entry, e->name);
        encode_fattr4(&entry, &e->attributes, attr_bitmap, attr_nwords,
                      efh, efh_len, config, 0);

        size_t entry_len = xdr_getpos(&entry);
        size_t response_len = xdr_getpos(rep) - reply_start;
        if (entry.error || entry_len + 8U > xdr_remaining(rep) ||
            entry_len + 8U > (size_t)maxcount ||
            response_len > (size_t)maxcount - entry_len - 8U) {
            if (encoded_entries == 0) {
                free(collector.entries);
                free(dir_path);
                xdr_setpos(rep, reply_start);
                return NFS4ERR_TOOSMALL;
            }
            collector.full = 1;
            break;
        }
        memcpy(rep->data + rep->pos, entry.data, entry_len);
        rep->pos += entry_len;
        encoded_entries++;
    }

    free(collector.entries);
    free(dir_path);

    xdr_encode_bool(rep, 0);
    xdr_encode_bool(rep, !collector.full);

    return NFS4_OK;
}

/* ---- OPEN ---- */

/* EXCLUSIVE4 create verifiers (RFC 7530 s16.16.5). The spec parks the
 * verifier in the new file's atime/mtime and relies on the client's
 * follow-up SETATTR to put real times back; the macOS client never sends
 * that SETATTR, so a file created with O_EXCL would keep a 1970 atime and
 * a far-future mtime. Keep the verifiers server-side instead: a bounded
 * ring shared by every connection, because the retransmit that needs it
 * arrives on a fresh connection after the old one dropped. The ring is
 * keyed by mount, path, and object identity so concurrent mounts in one
 * process stay apart and an unlinked/replaced path cannot inherit a stale
 * verifier.
 * A daemon restart loses it, but a restart loses the open-owner state the
 * replay would need anyway. */
#define EXCLUSIVE_VERIFIERS 64
typedef struct {
    const darwinfuse_config_t *config;
    uint64_t verifier;
    dev_t device;
    ino_t inode;
    char path[1024];
} exclusive_verifier_t;
static exclusive_verifier_t exclusive_verifiers[EXCLUSIVE_VERIFIERS];
static unsigned exclusive_verifier_next;
static pthread_mutex_t exclusive_verifier_lock = PTHREAD_MUTEX_INITIALIZER;

static int exclusive_remember(const darwinfuse_config_t *config,
                              const char *path, uint64_t verf,
                              struct fuse_file_info *fi) {
    struct stat st;
    memset(&st, 0, sizeof(st));
    if (!config->ops->fgetattr ||
        config->ops->fgetattr(path, &st, fi) != 0)
        return 0;

    pthread_mutex_lock(&exclusive_verifier_lock);
    exclusive_verifier_t *slot =
        &exclusive_verifiers[exclusive_verifier_next++ % EXCLUSIVE_VERIFIERS];
    slot->config = config;
    slot->verifier = verf;
    slot->device = st.st_dev;
    slot->inode = st.st_ino;
    strlcpy(slot->path, path, sizeof(slot->path));
    pthread_mutex_unlock(&exclusive_verifier_lock);
    return 1;
}

/* True when this mount created `path` under `verf` recently: the OPEN is a
 * retransmit of one that succeeded, not a collision. The entry stays until
 * the ring overwrites it, because every lost reply is followed by another
 * retransmit with the same verifier and each must succeed. An unrelated
 * later create of the same path carries a fresh 64-bit verifier, so a
 * lingering entry cannot make it succeed by mistake. */
static int exclusive_recall(const darwinfuse_config_t *config,
                            const char *path, uint64_t verf,
                            struct fuse_file_info *fi) {
    struct stat st;
    memset(&st, 0, sizeof(st));
    if (!config->ops->fgetattr ||
        config->ops->fgetattr(path, &st, fi) != 0)
        return 0;

    int found = 0;
    pthread_mutex_lock(&exclusive_verifier_lock);
    for (unsigned i = 0; i < EXCLUSIVE_VERIFIERS; i++) {
        const exclusive_verifier_t *slot = &exclusive_verifiers[i];
        if (slot->config == config && slot->verifier == verf &&
            slot->device == st.st_dev && slot->inode == st.st_ino &&
            strcmp(slot->path, path) == 0) {
            found = 1;
            break;
        }
    }
    pthread_mutex_unlock(&exclusive_verifier_lock);
    return found;
}

static int exclusive_validate_replay(const darwinfuse_config_t *config,
                                     const char *path, uint64_t verf,
                                     struct fuse_file_info *fi) {
    if (exclusive_recall(config, path, verf, fi))
        return 1;
    if (config->ops->release)
        config->ops->release(path, fi);
    return 0;
}

/* Named attributes have no FUSE file handle whose identity can be checked.
 * Bind EXCLUSIVE4 replay to the owning file identity plus the attribute name,
 * and use XATTR_CREATE for the actual absent-to-present transition. */
typedef struct {
    const darwinfuse_config_t *config;
    uint64_t verifier;
    dev_t device;
    ino_t inode;
    char path[1024];
    char name[256];
} namedattr_exclusive_verifier_t;
static namedattr_exclusive_verifier_t
    namedattr_exclusive_verifiers[EXCLUSIVE_VERIFIERS];
static unsigned namedattr_exclusive_verifier_next;
static pthread_mutex_t namedattr_exclusive_verifier_lock =
    PTHREAD_MUTEX_INITIALIZER;

#define NAMEDATTR_OPERATION_LOCKS 64
static pthread_mutex_t namedattr_operation_locks[NAMEDATTR_OPERATION_LOCKS];
static pthread_once_t namedattr_operation_locks_once = PTHREAD_ONCE_INIT;

static void namedattr_operation_locks_init(void) {
    for (unsigned i = 0; i < NAMEDATTR_OPERATION_LOCKS; i++)
        pthread_mutex_init(&namedattr_operation_locks[i], NULL);
}

static pthread_mutex_t *namedattr_operation_lock(
    const darwinfuse_config_t *config, const char *path, const char *name,
    int have_identity, dev_t device, ino_t inode) {
    pthread_once(&namedattr_operation_locks_once,
                 namedattr_operation_locks_init);
    uintptr_t hash = (uintptr_t)config ^ (uintptr_t)device ^
                     ((uintptr_t)inode * UINT64_C(11400714819323198485));
    const unsigned char *cursor = (const unsigned char *)name;
    while (*cursor)
        hash = (hash ^ *cursor++) * UINT64_C(1099511628211);
    if (!have_identity) {
        cursor = (const unsigned char *)path;
        while (*cursor)
            hash = (hash ^ *cursor++) * UINT64_C(1099511628211);
    }
    return &namedattr_operation_locks[hash % NAMEDATTR_OPERATION_LOCKS];
}

static int namedattr_owner_identity(const darwinfuse_config_t *config,
                                    const char *path,
                                    dev_t *device, ino_t *inode) {
    struct stat st;
    memset(&st, 0, sizeof(st));
    if (!config->ops->getattr || config->ops->getattr(path, &st) != 0)
        return 0;
    *device = st.st_dev;
    *inode = st.st_ino;
    return 1;
}

static void namedattr_exclusive_forget_locked(
    const darwinfuse_config_t *config, const char *path, const char *name,
    int have_identity, dev_t device, ino_t inode) {
    for (unsigned i = 0; i < EXCLUSIVE_VERIFIERS; i++) {
        namedattr_exclusive_verifier_t *slot =
            &namedattr_exclusive_verifiers[i];
        int same_owner = have_identity && slot->device == device &&
                         slot->inode == inode;
        int same_path = strcmp(slot->path, path) == 0;
        if (slot->config == config && strcmp(slot->name, name) == 0 &&
            (same_owner || same_path))
            memset(slot, 0, sizeof(*slot));
    }
}

static int namedattr_exclusive_recall_locked(
    const darwinfuse_config_t *config, const char *path, const char *name,
    uint64_t verf, dev_t device, ino_t inode) {
    for (unsigned i = 0; i < EXCLUSIVE_VERIFIERS; i++) {
        const namedattr_exclusive_verifier_t *slot =
            &namedattr_exclusive_verifiers[i];
        if (slot->config == config && slot->verifier == verf &&
            slot->device == device && slot->inode == inode &&
            strcmp(slot->path, path) == 0 && strcmp(slot->name, name) == 0)
            return 1;
    }
    return 0;
}

static void namedattr_exclusive_remember_locked(
    const darwinfuse_config_t *config, const char *path, const char *name,
    uint64_t verf, dev_t device, ino_t inode) {
    namedattr_exclusive_verifier_t *slot = &namedattr_exclusive_verifiers[
        namedattr_exclusive_verifier_next++ % EXCLUSIVE_VERIFIERS];
    slot->config = config;
    slot->verifier = verf;
    slot->device = device;
    slot->inode = inode;
    strlcpy(slot->path, path, sizeof(slot->path));
    strlcpy(slot->name, name, sizeof(slot->name));
}

static int namedattr_exclusive_recall_identity(
    const darwinfuse_config_t *config, const char *path, const char *name,
    uint64_t verf, dev_t device, ino_t inode) {
    pthread_mutex_lock(&namedattr_exclusive_verifier_lock);
    int found = namedattr_exclusive_recall_locked(
        config, path, name, verf, device, inode);
    pthread_mutex_unlock(&namedattr_exclusive_verifier_lock);
    return found;
}

static void namedattr_exclusive_forget_identity(
    const darwinfuse_config_t *config, const char *path, const char *name,
    int have_identity, dev_t device, ino_t inode) {
    pthread_mutex_lock(&namedattr_exclusive_verifier_lock);
    namedattr_exclusive_forget_locked(config, path, name, have_identity,
                                      device, inode);
    pthread_mutex_unlock(&namedattr_exclusive_verifier_lock);
}

static void namedattr_exclusive_remember_identity(
    const darwinfuse_config_t *config, const char *path, const char *name,
    uint64_t verf, dev_t device, ino_t inode) {
    pthread_mutex_lock(&namedattr_exclusive_verifier_lock);
    namedattr_exclusive_remember_locked(config, path, name, verf, device,
                                        inode);
    pthread_mutex_unlock(&namedattr_exclusive_verifier_lock);
}

static int namedattr_exclusive_remember(const darwinfuse_config_t *config,
                                        const char *path, const char *name,
                                        uint64_t verf) {
    dev_t device;
    ino_t inode;
    if (!namedattr_owner_identity(config, path, &device, &inode))
        return 0;
    namedattr_exclusive_forget_identity(config, path, name, 1, device, inode);
    namedattr_exclusive_remember_identity(config, path, name, verf, device,
                                          inode);
    return 1;
}

static int namedattr_exclusive_recall(const darwinfuse_config_t *config,
                                      const char *path, const char *name,
                                      uint64_t verf) {
    dev_t device;
    ino_t inode;
    if (!namedattr_owner_identity(config, path, &device, &inode))
        return 0;
    return namedattr_exclusive_recall_identity(config, path, name, verf,
                                               device, inode);
}

static void namedattr_exclusive_forget(const darwinfuse_config_t *config,
                                       const char *path, const char *name) {
    dev_t device = 0;
    ino_t inode = 0;
    int have_identity = namedattr_owner_identity(config, path, &device, &inode);
    namedattr_exclusive_forget_identity(config, path, name, have_identity,
                                        device, inode);
}

static int namedattr_remove(const darwinfuse_config_t *config,
                            const char *path, const char *name) {
    dev_t device = 0;
    ino_t inode = 0;
    int have_identity = namedattr_owner_identity(config, path, &device, &inode);
    pthread_mutex_t *operation_lock = namedattr_operation_lock(
        config, path, name, have_identity, device, inode);
    pthread_mutex_lock(operation_lock);
    int rc = config->ops->removexattr(path, name);
    if (rc == 0)
        namedattr_exclusive_forget_identity(config, path, name, have_identity,
                                            device, inode);
    pthread_mutex_unlock(operation_lock);
    return rc;
}

/* Serialize existence testing, create-only transition, and verifier
 * publication.  REMOVE takes the same lock, so no replay can observe an
 * unrecorded creation or retain a verifier across remove/recreate. */
static uint32_t namedattr_open_create(const darwinfuse_config_t *config,
                                      const char *path, const char *name,
                                      uint32_t createmode, uint64_t verf,
                                      int *created) {
    *created = 0;
    dev_t device = 0;
    ino_t inode = 0;
    int have_identity = namedattr_owner_identity(config, path, &device, &inode);
    pthread_mutex_t *operation_lock = namedattr_operation_lock(
        config, path, name, have_identity, device, inode);
    pthread_mutex_lock(operation_lock);
    int size = config->ops->getxattr
        ? FUSE_GETXATTR(config->ops, path, name, NULL, 0)
        : -ENOTSUP;
    uint32_t status = NFS4_OK;

    if (size >= 0) {
        if (createmode == EXCLUSIVE4 && have_identity &&
            namedattr_exclusive_recall_identity(config, path, name, verf,
                                                 device, inode)) {
            /* Lost-reply replay of this exact creation. */
        } else if (createmode == GUARDED4 || createmode == EXCLUSIVE4) {
            status = NFS4ERR_EXIST;
        }
        goto done;
    }

    status = errno_to_nfs4(size);
    if (status != NFS4ERR_NOENT)
        goto done;
    if (!config->ops->setxattr) {
        status = NFS4ERR_ROFS;
        goto done;
    }
    if (createmode == EXCLUSIVE4 && !have_identity) {
        status = NFS4ERR_IO;
        goto done;
    }

    int rc = FUSE_SETXATTR(config->ops, path, name, "", 0, XATTR_CREATE);
    if (rc == 0) {
        *created = 1;
        namedattr_exclusive_forget_identity(config, path, name, have_identity,
                                            device, inode);
        if (createmode == EXCLUSIVE4) {
            namedattr_exclusive_remember_identity(config, path, name, verf,
                                                  device, inode);
        }
        status = NFS4_OK;
    } else if (rc == -EEXIST && createmode == UNCHECKED4) {
        status = NFS4_OK;
    } else if (rc == -EEXIST && createmode == EXCLUSIVE4 && have_identity &&
               namedattr_exclusive_recall_identity(config, path, name, verf,
                                                    device, inode)) {
        status = NFS4_OK;
    } else {
        status = errno_to_nfs4(rc);
    }

done:
    pthread_mutex_unlock(operation_lock);
    return status;
}

/* Deterministic callback-level regression for the exact-handle replay rule.
 * This is called by the macOS Rust test suite. */
static uint64_t exclusive_test_released_fh;
static unsigned exclusive_test_release_count;

static int exclusive_test_fgetattr(const char *path, struct stat *st,
                                   struct fuse_file_info *fi) {
    (void)path;
    memset(st, 0, sizeof(*st));
    st->st_dev = 7;
    st->st_ino = (ino_t)fi->fh;
    return 0;
}

static int exclusive_test_release(const char *path,
                                  struct fuse_file_info *fi) {
    (void)path;
    exclusive_test_released_fh = fi->fh;
    exclusive_test_release_count++;
    return 0;
}

int nfs4_test_exclusive_replay_identity(void) {
    static struct fuse_operations ops;
    static darwinfuse_config_t config;
    const char *path = "/exclusive-replay-self-test";
    const uint64_t verifier = UINT64_C(0x8f3a2d1c7b6e5049);
    struct fuse_file_info created;
    struct fuse_file_info replacement;
    memset(&ops, 0, sizeof(ops));
    memset(&config, 0, sizeof(config));
    memset(&created, 0, sizeof(created));
    memset(&replacement, 0, sizeof(replacement));
    ops.fgetattr = exclusive_test_fgetattr;
    ops.release = exclusive_test_release;
    config.ops = &ops;
    created.fh = 101;
    replacement.fh = 202;
    exclusive_test_released_fh = 0;
    exclusive_test_release_count = 0;

    if (!exclusive_remember(&config, path, verifier, &created))
        return 1;
    if (!exclusive_recall(&config, path, verifier, &created))
        return 2;
    if (exclusive_validate_replay(&config, path, verifier, &replacement))
        return 3;
    if (exclusive_test_release_count != 1 ||
        exclusive_test_released_fh != replacement.fh)
        return 4;
    return 0;
}

int nfs4_test_readdir_cookie_verifier(void) {
    uint8_t verifier[8];
    uint8_t stale[8];
    const uint64_t revision = UINT64_C(0x0102030405060708);
    encode_cookie_verifier(revision, verifier);
    encode_cookie_verifier(revision - 1, stale);
    if (!readdir_cookie_is_current(0, stale, revision))
        return 1;
    if (!readdir_cookie_is_current(1, verifier, revision))
        return 2;
    if (readdir_cookie_is_current(1, stale, revision))
        return 3;
    if (memcmp(verifier, "\x01\x02\x03\x04\x05\x06\x07\x08", 8) != 0)
        return 4;
    return 0;
}

static ino_t namedattr_test_inode;
static int namedattr_test_present;
static unsigned namedattr_test_create_count;

static int namedattr_test_getattr(const char *path, struct stat *st) {
    (void)path;
    memset(st, 0, sizeof(*st));
    st->st_dev = 11;
    st->st_ino = namedattr_test_inode;
    return 0;
}

static int namedattr_test_getxattr(const char *path, const char *name,
                                   char *value, size_t size,
                                   uint32_t position) {
    (void)path;
    (void)name;
    (void)value;
    (void)size;
    (void)position;
    return namedattr_test_present ? 0 : -ENOATTR;
}

static int namedattr_test_setxattr(const char *path, const char *name,
                                   const char *value, size_t size, int flags,
                                   uint32_t position) {
    (void)path;
    (void)name;
    (void)value;
    (void)size;
    (void)position;
    if ((flags & XATTR_CREATE) && namedattr_test_present)
        return -EEXIST;
    namedattr_test_present = 1;
    namedattr_test_create_count++;
    return 0;
}

static int namedattr_test_removexattr(const char *path, const char *name) {
    (void)path;
    (void)name;
    if (!namedattr_test_present)
        return -ENOATTR;
    namedattr_test_present = 0;
    return 0;
}

typedef struct {
    const darwinfuse_config_t *config;
    const char *path;
    const char *name;
    uint64_t verifier;
    uint32_t status;
    int created;
} namedattr_test_open_t;

static void *namedattr_test_open(void *opaque) {
    namedattr_test_open_t *open = opaque;
    open->status = namedattr_open_create(
        open->config, open->path, open->name, EXCLUSIVE4, open->verifier,
        &open->created);
    return NULL;
}

int nfs4_test_namedattr_exclusive_replay_identity(void) {
    static struct fuse_operations ops;
    static darwinfuse_config_t config;
    const char *path = "/namedattr-exclusive-replay-self-test";
    const char *name = "com.acyclic.test";
    const uint64_t verifier = UINT64_C(0x19a27c4d58e630bf);
    memset(&ops, 0, sizeof(ops));
    memset(&config, 0, sizeof(config));
    ops.getattr = namedattr_test_getattr;
    ops.getxattr = namedattr_test_getxattr;
    ops.setxattr = namedattr_test_setxattr;
    ops.removexattr = namedattr_test_removexattr;
    config.ops = &ops;
    namedattr_test_inode = 301;
    if (!namedattr_exclusive_remember(&config, path, name, verifier))
        return 1;
    if (!namedattr_exclusive_recall(&config, path, name, verifier))
        return 2;
    if (namedattr_exclusive_recall(&config, path, "com.acyclic.other", verifier))
        return 3;
    namedattr_test_inode = 302;
    if (namedattr_exclusive_recall(&config, path, name, verifier))
        return 4;
    namedattr_test_inode = 301;
    namedattr_exclusive_forget(&config, path, name);
    if (!namedattr_exclusive_remember(&config, path, name, verifier + 1))
        return 5;
    if (namedattr_exclusive_recall(&config, path, name, verifier))
        return 6;
    if (!namedattr_exclusive_recall(&config, path, name, verifier + 1))
        return 7;

    const char *atomic_name = "com.acyclic.atomic";
    namedattr_test_present = 0;
    namedattr_test_create_count = 0;
    namedattr_test_open_t first = {&config, path, atomic_name, verifier + 2,
                                   NFS4ERR_SERVERFAULT, 0};
    namedattr_test_open_t second = first;
    pthread_t first_thread;
    pthread_t second_thread;
    if (pthread_create(&first_thread, NULL, namedattr_test_open, &first) != 0)
        return 8;
    if (pthread_create(&second_thread, NULL, namedattr_test_open, &second) != 0) {
        pthread_join(first_thread, NULL);
        return 9;
    }
    pthread_join(first_thread, NULL);
    pthread_join(second_thread, NULL);
    if (first.status != NFS4_OK || second.status != NFS4_OK)
        return 10;
    if (namedattr_test_create_count != 1 || first.created + second.created != 1)
        return 11;

    if (namedattr_remove(&config, path, atomic_name) != 0)
        return 12;
    int recreated = 0;
    if (namedattr_open_create(&config, path, atomic_name, EXCLUSIVE4,
                              verifier + 3, &recreated) != NFS4_OK ||
        !recreated)
        return 13;
    int replay_created = 0;
    if (namedattr_open_create(&config, path, atomic_name, EXCLUSIVE4,
                              verifier + 2, &replay_created) != NFS4ERR_EXIST)
        return 14;
    if (namedattr_open_create(&config, path, atomic_name, EXCLUSIVE4,
                              verifier + 3, &replay_created) != NFS4_OK)
        return 15;

    const char *renamed_name = "com.acyclic.renamed-owner";
    if (!namedattr_exclusive_remember(&config, "/owner-before-rename",
                                      renamed_name, verifier + 4))
        return 16;
    namedattr_exclusive_forget(&config, "/owner-after-rename", renamed_name);
    if (namedattr_exclusive_recall(&config, "/owner-before-rename",
                                   renamed_name, verifier + 4))
        return 17;
    return 0;
}

/* One attribute callback both proves an OPEN target exists and refuses
 * directories. */
static uint32_t open_target_status(const darwinfuse_config_t *config,
                                   const char *path)
{
    if (!config->ops->getattr) return NFS4_OK;
    struct stat st;
    memset(&st, 0, sizeof(st));
    int rc = config->ops->getattr(path, &st);
    if (rc != 0) return errno_to_nfs4(rc);
    return S_ISDIR(st.st_mode) ? NFS4ERR_ISDIR : NFS4_OK;
}

static uint32_t handle_open(const darwinfuse_config_t *config,
                             nfs4_conn_state_t *conn,
                             nfs4_request_ctx_t *ctx,
                             xdr_buf_t *req, xdr_buf_t *rep)
{
    uint64_t change_before = fresh_change(config);
    uint64_t change_after = change_before;
    /* The parent's change when this OPEN leaves it unchanged, else 0 */
    uint64_t unchanged_directory = 0;

    /* Decode OPEN4args */
    xdr_decode_uint32(req);    /* seqid — unused */
    uint32_t share_access  = xdr_decode_uint32(req);
    xdr_decode_uint32(req);    /* share_deny — unused */

    /* open_owner4: { clientid, owner } */
    xdr_decode_uint64(req);      /* clientid */
    xdr_skip_opaque(req);        /* owner (opaque) */

    /* openflag4: opentype */
    uint32_t opentype = xdr_decode_uint32(req);
    mode_t create_mode = 0644;
    uint32_t create_bitmap[2] = {0, 0};
    int create_nwords = 0;

    uint32_t createmode = UNCHECKED4;
    uint64_t createverf = 0;
    int exclusive_replay = 0;
    if (opentype == OPEN4_CREATE) {
        createmode = xdr_decode_uint32(req);
        if (createmode == EXCLUSIVE4) {
            /* createhow4 is a union: EXCLUSIVE4 carries an 8-byte
             * createverf and NO createattrs (RFC 7530 s16.16). Decoding
             * attrs here misread the verifier as a bitmap and every
             * O_CREAT|O_EXCL open (mkstemp, git index.lock, editor
             * atomic saves) failed with EIO. The client sends the mode
             * in a follow-up SETATTR, so 0644 is only a placeholder.
             * The verifier is kept: it is stored on the created file so
             * a retransmitted OPEN can be told from a real collision. */
            createverf = xdr_decode_uint64(req);
        } else {
            /* Decode createattrs (bitmap + attr data) */
            decode_bitmap(req, create_bitmap, &create_nwords);
            /* Decode attr data */
            uint8_t create_attr_data[1024];
            uint32_t cad_len = xdr_decode_opaque(req, create_attr_data, sizeof(create_attr_data));

            /* Extract mode from create attrs if present */
            if (bitmap_isset(create_bitmap, create_nwords, FATTR4_MODE)) {
                xdr_buf_t cad;
                xdr_init(&cad, create_attr_data, cad_len);
                /* Must skip any attrs before MODE (bit 33) that are set */
                if (bitmap_isset(create_bitmap, create_nwords, FATTR4_SIZE))
                    xdr_decode_uint64(&cad);  /* skip size */
                create_mode = (mode_t)xdr_decode_uint32(&cad);
            }
        }
    }

    /* open_claim4 */
    uint32_t claim_type = xdr_decode_uint32(req);

    char filename[256] = {0};
    dfuse_ino_t target_ino = 0;
    char *target_path = NULL;

    if (claim_type == CLAIM_NULL) {
        /* component name to open */
        xdr_decode_string(req, filename, sizeof(filename));

        /* Check if current FH is an attrdir (opening a named attribute) */
        dfuse_ino_t cur_ino = fh_get_ino(ctx->current_fh, ctx->current_fh_len);
        dfuse_ino_type_t cur_type = dfuse_itable_type(config->inode_table, cur_ino);

        if (cur_type == DFUSE_INO_ATTRDIR) {
            char *file_path = xattr_file_path(config, cur_ino);
            if (!file_path) return NFS4ERR_STALE;

            int created_namedattr = 0;
            if (opentype == OPEN4_CREATE) {
                uint32_t status = namedattr_open_create(
                    config, file_path, filename, createmode, createverf,
                    &created_namedattr);
                if (status != NFS4_OK) {
                    free(file_path);
                    return status;
                }
            } else {
                int namedattr_size = config->ops->getxattr
                    ? FUSE_GETXATTR(config->ops, file_path, filename, NULL, 0)
                    : -ENOTSUP;
                if (namedattr_size >= 0) {
                    /* Existing attribute opened without modification. */
                } else if (config->ops->getxattr) {
                    free(file_path);
                    return errno_to_nfs4(namedattr_size);
                } else {
                    free(file_path);
                    return NFS4ERR_NOTSUPP;
                }
            }

            free(file_path);

            target_ino = dfuse_itable_get_namedattr(
                config->inode_table, cur_ino, filename);
            if (target_ino == 0) return NFS4ERR_SERVERFAULT;

            /* No FUSE file handle backs a named attribute */
            nfs4_stateid_t sid;
            if (track_open(conn, target_ino, NULL, &sid) < 0)
                return NFS4ERR_RESOURCE;

            fh_set_ino(ctx->current_fh, &ctx->current_fh_len, target_ino);

            /* Encode OPEN4resok */
            xdr_encode_uint32(rep, sid.seqid);
            xdr_encode_opaque_fixed(rep, sid.other, 12);
            if (created_namedattr)
                change_after = namespace_changed(config);
            encode_change_info(rep, 0, change_before, change_after);
            xdr_encode_uint32(rep, OPEN4_RESULT_FLAGS);
            xdr_encode_uint32(rep, 0);
            xdr_encode_uint32(rep, OPEN_DELEGATE_NONE);
            return NFS4_OK;
        }

        /* Get parent directory path */
        char *dir_path = fh_to_path(config, ctx->current_fh, ctx->current_fh_len);
        if (!dir_path) return NFS4ERR_STALE;

        /* Build target path.  Sample the parent's change before anything
         * is created: if the parent then changes, the stale value only
         * makes the client revalidate it. */
        char path_buf[1024];
        build_child_path(path_buf, sizeof(path_buf), dir_path, filename);
        uint64_t directory = directory_change(config, dir_path);
        free(dir_path);

        if (opentype == OPEN4_CREATE) {
            /* Try to create the file first */
            struct fuse_file_info fi;
            memset(&fi, 0, sizeof(fi));
            fi.flags = O_CREAT | O_WRONLY;
            if (share_access & OPEN4_SHARE_ACCESS_READ)
                fi.flags = O_CREAT | O_RDWR;

            if (config->ops->create) {
                if (createmode == EXCLUSIVE4 && !config->ops->fgetattr)
                    return NFS4ERR_NOTSUPP;
                int rc = config->ops->create(path_buf, create_mode, &fi);
                if (rc == 0 && createmode == EXCLUSIVE4) {
                    /* If this reply is lost and the client retransmits
                     * with the same verifier, the EEXIST path below
                     * recognises the replay instead of failing O_EXCL
                     * on a file this very request created. */
                    if (!exclusive_remember(config, path_buf, createverf, &fi)) {
                        /* The pathname may already name a concurrent
                         * replacement. Close only the handle returned by
                         * this create; unlinking by pathname here could
                         * delete that replacement. */
                        if (config->ops->release)
                            config->ops->release(path_buf, &fi);
                        return NFS4ERR_IO;
                    }
                }
                if (rc == 0) {
                    change_after = namespace_changed(config);
                    /* Store the fuse_fh; an untracked handle could
                     * never be closed, so release it instead. */
                    target_ino = dfuse_itable_get_or_create(config->inode_table, path_buf);
                    nfs4_stateid_t sid;
                    if (target_ino == 0 || track_open(conn, target_ino, &fi, &sid) < 0) {
                        if (config->ops->release)
                            config->ops->release(path_buf, &fi);
                        return NFS4ERR_RESOURCE;
                    }

                    /* Set current FH */
                    fh_set_ino(ctx->current_fh, &ctx->current_fh_len, target_ino);

                    /* Encode OPEN4resok */
                    xdr_encode_uint32(rep, sid.seqid);
                    xdr_encode_opaque_fixed(rep, sid.other, 12);
                    encode_change_info(rep, 0, change_before, change_after);
                    xdr_encode_uint32(rep, OPEN4_RESULT_FLAGS);
                    xdr_encode_uint32(rep, 0);    /* attrset bitmap (empty) */
                    xdr_encode_uint32(rep, OPEN_DELEGATE_NONE);
                    return NFS4_OK;
                }
                /* create failed — fall through to try open if EEXIST,
                 * but only for UNCHECKED4: GUARDED4 and EXCLUSIVE4 must
                 * report the existing file, or O_EXCL would silently
                 * succeed on a file someone else created. The one
                 * exception is an EXCLUSIVE4 replay: the file exists
                 * because this very request created it and the reply
                 * was lost, which its remembered verifier proves. */
                if (rc != -EEXIST)
                    return errno_to_nfs4(rc);
                if (createmode == GUARDED4)
                    return NFS4ERR_EXIST;
                if (createmode == EXCLUSIVE4)
                    exclusive_replay = 1;
            } else {
                return NFS4ERR_NOTSUPP;
            }
            /* File exists or was just created, fall through to open it */
        }

        /* Allocate an inode only for a target that exists */
        uint32_t status = open_target_status(config, path_buf);
        if (status != NFS4_OK) return status;
        unchanged_directory = directory;

        target_ino = dfuse_itable_get_or_create(config->inode_table, path_buf);
        if (target_ino == 0) return NFS4ERR_SERVERFAULT;
        target_path = dfuse_itable_path_dup(config->inode_table, target_ino);

    } else if (claim_type == CLAIM_FH) {
        /* Open current filehandle */
        target_ino = fh_get_ino(ctx->current_fh, ctx->current_fh_len);
        if (target_ino == 0) return NFS4ERR_BADHANDLE;
        target_path = dfuse_itable_path_dup(config->inode_table, target_ino);
        if (!target_path) return NFS4ERR_STALE;
        uint32_t status = open_target_status(config, target_path);
        if (status != NFS4_OK) { free(target_path); return status; }
    } else {
        return NFS4ERR_NOTSUPP;
    }

    if (req->error) { free(target_path); return NFS4ERR_INVAL; }

    /* Call FUSE open callback */
    struct fuse_file_info fi;
    memset(&fi, 0, sizeof(fi));
    if (share_access & OPEN4_SHARE_ACCESS_WRITE)
        fi.flags = O_RDWR;
    else
        fi.flags = O_RDONLY;

    if (config->ops->open) {
        int rc = config->ops->open(target_path, &fi);
        if (rc != 0) { free(target_path); return errno_to_nfs4(rc); }
    }

    /* Validate a retransmit against the object that was actually opened, not
     * a path-based stat that can race an unlink/rename/replacement. The FUSE
     * handle remains bound to that object even if the path changes again. */
    if (exclusive_replay &&
        !exclusive_validate_replay(config, target_path, createverf, &fi)) {
        free(target_path);
        return NFS4ERR_EXIST;
    }

    /* Store open file tracking; an untracked handle could never be
     * closed, so release it instead. */
    nfs4_stateid_t sid;
    if (track_open(conn, target_ino, &fi, &sid) < 0) {
        if (config->ops->release)
            config->ops->release(target_path, &fi);
        free(target_path);
        return NFS4ERR_RESOURCE;
    }

    /* Set current FH to opened file */
    fh_set_ino(ctx->current_fh, &ctx->current_fh_len, target_ino);

    /* Encode OPEN4resok */
    /* stateid4 */
    xdr_encode_uint32(rep, sid.seqid);
    xdr_encode_opaque_fixed(rep, sid.other, 12);

    if (unchanged_directory != 0)
        encode_change_info(rep, 1, unchanged_directory, unchanged_directory);
    else
        encode_change_info(rep, 0, change_before, change_after);

    xdr_encode_uint32(rep, OPEN4_RESULT_FLAGS);

    /* attrset bitmap (empty) */
    xdr_encode_uint32(rep, 0);

    /* delegation: OPEN_DELEGATE_NONE */
    xdr_encode_uint32(rep, OPEN_DELEGATE_NONE);

    free(target_path);
    return NFS4_OK;
}

static uint32_t handle_open_confirm(const darwinfuse_config_t *config,
                                     nfs4_conn_state_t *conn,
                                     nfs4_request_ctx_t *ctx,
                                     xdr_buf_t *req, xdr_buf_t *rep)
{
    /* open_stateid */
    uint32_t sid_seqid = xdr_decode_uint32(req);
    uint8_t sid_other[12];
    xdr_decode_opaque_fixed(req, sid_other, 12);
    /* seqid */
    uint32_t seqid = xdr_decode_uint32(req);
    (void)sid_seqid;
    (void)seqid;

    /* Find the open file and "confirm" it */
    uint32_t confirmed_seqid;
    uint8_t confirmed_other[12];
    int found = 0;

    pthread_mutex_lock(&conn->lock);
    nfs4_open_file_t *of = find_open_file(conn, sid_other);
    if (of) {
        of->stateid.seqid++;
        confirmed_seqid = of->stateid.seqid;
        memcpy(confirmed_other, of->stateid.other, 12);
        found = 1;
    }
    pthread_mutex_unlock(&conn->lock);

    if (found) {
        xdr_encode_uint32(rep, confirmed_seqid);
        xdr_encode_opaque_fixed(rep, confirmed_other, 12);
        return NFS4_OK;
    }

    return NFS4ERR_BAD_STATEID;
}

/* Flush, then release, one open's FUSE handle.  Both callbacks act on the
 * handle, so an open whose name was removed meanwhile still releases. */
static void close_open_file(const darwinfuse_config_t *config,
                            const nfs4_open_file_t *of)
{
    char *path = dfuse_itable_path_dup(config->inode_table, of->ino);
    const char *name = path ? path : "";
    struct fuse_file_info fi;
    memset(&fi, 0, sizeof(fi));
    fi.fh = of->fuse_fh;
    fi.flags = of->flags;
    if (config->ops->flush)
        config->ops->flush(name, &fi);
    if (config->ops->release)
        config->ops->release(name, &fi);
    free(path);
}

void nfs4_release_open_files(const darwinfuse_config_t *config,
                             nfs4_conn_state_t *conn)
{
    pthread_mutex_lock(&conn->lock);
    nfs4_open_file_t *open_files = conn->open_files;
    int count = conn->open_file_count;
    conn->open_files = NULL;
    conn->open_file_count = 0;
    conn->open_file_cap = 0;
    pthread_mutex_unlock(&conn->lock);

    for (int i = 0; i < count; i++)
        close_open_file(config, &open_files[i]);
    free(open_files);
}

static uint32_t handle_close(const darwinfuse_config_t *config,
                              nfs4_conn_state_t *conn,
                              nfs4_request_ctx_t *ctx,
                              xdr_buf_t *req, xdr_buf_t *rep)
{
    uint32_t seqid = xdr_decode_uint32(req);
    (void)seqid;
    uint32_t sid_seqid = xdr_decode_uint32(req);
    uint8_t sid_other[12];
    xdr_decode_opaque_fixed(req, sid_other, 12);

    /* Find open file, copy state, and remove under lock */
    nfs4_open_file_t closed;
    int found = 0;

    pthread_mutex_lock(&conn->lock);
    for (int i = 0; i < conn->open_file_count; i++) {
        if (memcmp(conn->open_files[i].stateid.other, sid_other, 12) == 0) {
            closed = conn->open_files[i];
            found = 1;

            /* Remove from array */
            conn->open_file_count--;
            if (i < conn->open_file_count)
                conn->open_files[i] = conn->open_files[conn->open_file_count];
            break;
        }
    }
    pthread_mutex_unlock(&conn->lock);

    if (found) {
        /* Call FUSE flush + release callbacks WITHOUT holding lock */
        close_open_file(config, &closed);

        /* Return invalidated stateid */
        xdr_encode_uint32(rep, sid_seqid + 1);
        xdr_encode_opaque_fixed(rep, sid_other, 12);
        return NFS4_OK;
    }

    /* Unknown stateid — still return success with zeroed stateid */
    xdr_encode_uint32(rep, 0);
    uint8_t zero[12] = {0};
    xdr_encode_opaque_fixed(rep, zero, 12);
    return NFS4_OK;
}

static uint32_t handle_read(const darwinfuse_config_t *config,
                             nfs4_conn_state_t *conn,
                             nfs4_request_ctx_t *ctx,
                             xdr_buf_t *req, xdr_buf_t *rep)
{
    /* stateid4 */
    xdr_decode_uint32(req);  /* seqid */
    uint8_t sid_other[12];
    xdr_decode_opaque_fixed(req, sid_other, 12);

    uint64_t offset = xdr_decode_uint64(req);
    uint32_t count  = xdr_decode_uint32(req);
    if (req->error) return NFS4ERR_INVAL;

    dfuse_ino_t cur_ino = fh_get_ino(ctx->current_fh, ctx->current_fh_len);
    if (cur_ino == 0) return NFS4ERR_BADHANDLE;

    dfuse_ino_type_t ino_type = dfuse_itable_type(config->inode_table, cur_ino);

    if (ino_type == DFUSE_INO_NAMEDATTR) {
        /* Read a named attribute (xattr) value */
        char *file_path = xattr_file_path(config, cur_ino);
        char *attr_name = dfuse_itable_attr_name_dup(config->inode_table,
                                                        cur_ino);
        if (!file_path || !attr_name) {
            free(file_path);
            free(attr_name);
            return NFS4ERR_STALE;
        }
        if (!config->ops->getxattr) {
            free(file_path);
            free(attr_name);
            return NFS4ERR_NOTSUPP;
        }

        /* Read full xattr value, then slice at offset */
        if (count > 65536) count = 65536;
        char *val_buf = malloc(65536);
        if (!val_buf) {
            free(file_path);
            free(attr_name);
            return NFS4ERR_SERVERFAULT;
        }

        int total = FUSE_GETXATTR(config->ops, file_path, attr_name,
                                  val_buf, 65536);
        free(file_path);
        free(attr_name);
        if (total < 0) {
            free(val_buf);
            return errno_to_nfs4(total);
        }

        /* Compute slice */
        int avail = (offset < (uint64_t)total) ? total - (int)offset : 0;
        int n = (avail < (int)count) ? avail : (int)count;
        int eof = (offset + (uint64_t)n >= (uint64_t)total) ? 1 : 0;

        xdr_encode_bool(rep, eof);
        xdr_encode_opaque(rep, (uint8_t *)val_buf + offset, (uint32_t)n);

        free(val_buf);
        return NFS4_OK;
    }

    /* Regular file read */
    char *path = fh_to_path(config, ctx->current_fh, ctx->current_fh_len);
    if (!path) return NFS4ERR_STALE;

    if (!config->ops->read) {
        free(path);
        return NFS4ERR_NOTSUPP;
    }

    /* Read straight into the reply, after the eof flag and data length. */
    if (count > DFUSE_IO_SIZE) count = DFUSE_IO_SIZE;
    size_t room = xdr_remaining(rep);
    if (room < 8) {
        free(path);
        return NFS4ERR_RESOURCE;
    }
    room = (room - 8) & ~(size_t)3;  /* padded data the reply can carry */
    if (count > room) count = (uint32_t)room;
    uint8_t *data = rep->data + xdr_getpos(rep) + 8;

    struct fuse_file_info fi;
    int have_handle = open_file_info(conn, sid_other, cur_ino, &fi);

    int n = config->ops->read(path, (char *)data, count, (off_t)offset, &fi);
    if (n < 0) {
        DFUSE_LOG("  READ '%s' offset=%llu count=%u -> error %d",
                  path, (unsigned long long)offset, count, n);
        free(path);
        return errno_to_nfs4(n);
    }
    if ((uint32_t)n > count) {
        free(path);
        return NFS4ERR_IO;
    }

    /* A short read ends at EOF; only a full one needs the size. */
    int eof = (uint32_t)n < count;
    if (!eof) {
        struct stat st;
        memset(&st, 0, sizeof(st));
        int rc = -ENOTSUP;
        if (have_handle && config->ops->fgetattr)
            rc = config->ops->fgetattr(path, &st, &fi);
        else if (config->ops->getattr)
            rc = config->ops->getattr(path, &st);
        eof = rc == 0 &&
              (uint64_t)offset + (uint64_t)n >= (uint64_t)st.st_size;
    }
    free(path);

    size_t padded = ((size_t)n + 3) & ~(size_t)3;
    memset(data + n, 0, padded - (size_t)n);
    xdr_encode_bool(rep, eof);
    xdr_encode_uint32(rep, (uint32_t)n);
    rep->pos += padded;
    return NFS4_OK;
}

static uint32_t sync_file(const darwinfuse_config_t *config,
                          const char *path, struct fuse_file_info *fi)
{
    if (!config->ops->fsync)
        return NFS4ERR_IO;
    return errno_to_nfs4(config->ops->fsync(path, 0, fi));
}

/*
 * Whether a completed WRITE must still sync to reach the stability the
 * client asked for.  Writes that are durable once they return never do.
 */
static int write_needs_sync(const darwinfuse_config_t *config, uint32_t stable)
{
    return stable != UNSTABLE4 && !config->durable_writes;
}

/*
 * The stability a completed WRITE reached (RFC 7530 s16.36): FILE_SYNC4
 * when it synced or writes are durable once they return, so the client
 * never needs a COMMIT for it.
 */
static uint32_t write_committed(const darwinfuse_config_t *config,
                                uint32_t stable)
{
    return stable != UNSTABLE4 || config->durable_writes
        ? FILE_SYNC4 : UNSTABLE4;
}

static int sync_test_success(const char *path, int data_only,
                             struct fuse_file_info *fi)
{
    return strcmp(path, "/sync-test") == 0 && data_only == 0 && fi->fh == 0
        ? 0 : -EIO;
}

static int sync_test_failure(const char *path, int data_only,
                             struct fuse_file_info *fi)
{
    (void)path;
    (void)data_only;
    (void)fi;
    return -ENOSPC;
}

static int sync_test_write(const char *path, const char *data, size_t length,
                           off_t offset, struct fuse_file_info *fi)
{
    return strcmp(path, "/sync-test") == 0 && length == 3 &&
           memcmp(data, "abc", 3) == 0 && offset == 0 && fi->fh == 0
        ? (int)length : -EIO;
}

static uint32_t handle_write(const darwinfuse_config_t *config,
                              nfs4_conn_state_t *conn,
                              nfs4_request_ctx_t *ctx,
                              xdr_buf_t *req, xdr_buf_t *rep)
{
    /* stateid4 */
    xdr_decode_uint32(req);  /* seqid */
    uint8_t sid_other[12];
    xdr_decode_opaque_fixed(req, sid_other, 12);

    uint64_t offset = xdr_decode_uint64(req);
    uint32_t stable = xdr_decode_uint32(req);
    if (req->error || stable > FILE_SYNC4)
        return NFS4ERR_INVAL;

    /* data (opaque) */
    uint32_t data_len_raw = xdr_decode_uint32(req);
    if (req->error || data_len_raw > DFUSE_IO_SIZE)
        return NFS4ERR_INVAL;

    size_t padded = (data_len_raw + 3) & ~(size_t)3;
    if (xdr_remaining(req) < padded)
        return NFS4ERR_INVAL;

    uint8_t *data = req->data + req->pos;
    xdr_skip(req, padded);

    dfuse_ino_t cur_ino = fh_get_ino(ctx->current_fh, ctx->current_fh_len);
    if (cur_ino == 0) return NFS4ERR_BADHANDLE;

    dfuse_ino_type_t ino_type = dfuse_itable_type(config->inode_table, cur_ino);

    if (ino_type == DFUSE_INO_NAMEDATTR) {
        /* Write to a named attribute (xattr) */
        char *file_path = xattr_file_path(config, cur_ino);
        char *attr_name = dfuse_itable_attr_name_dup(config->inode_table,
                                                        cur_ino);
        if (!file_path || !attr_name) {
            free(file_path);
            free(attr_name);
            return NFS4ERR_STALE;
        }
        if (!config->ops->setxattr) {
            free(file_path);
            free(attr_name);
            return NFS4ERR_ROFS;
        }

        /*
         * Read-modify-write: read current value, patch at offset, write back.
         * For the common case (offset=0, single write), this is just setxattr.
         */
        char *val_buf = NULL;
        int cur_size = 0;

        if (offset > 0 && config->ops->getxattr) {
            val_buf = malloc(65536);
            if (!val_buf) {
                free(file_path);
                free(attr_name);
                return NFS4ERR_SERVERFAULT;
            }
            cur_size = FUSE_GETXATTR(config->ops, file_path, attr_name,
                                     val_buf, 65536);
            if (cur_size < 0) cur_size = 0;
        }

        size_t new_size = (size_t)offset + data_len_raw;
        if ((size_t)cur_size > new_size) new_size = (size_t)cur_size;

        char *new_val = calloc(1, new_size);
        if (!new_val) {
            free(val_buf);
            free(file_path);
            free(attr_name);
            return NFS4ERR_SERVERFAULT;
        }

        /* Copy existing data if any */
        if (val_buf && cur_size > 0)
            memcpy(new_val, val_buf, (size_t)cur_size);
        free(val_buf);

        /* Patch in the new data */
        memcpy(new_val + offset, data, data_len_raw);

        int rc = FUSE_SETXATTR(config->ops, file_path, attr_name,
                               new_val, new_size, 0);
        free(new_val);
        uint32_t sync_status = NFS4_OK;
        if (rc == 0 && write_needs_sync(config, stable)) {
            struct fuse_file_info fi;
            memset(&fi, 0, sizeof(fi));
            sync_status = sync_file(config, file_path, &fi);
        }
        free(file_path);
        free(attr_name);

        if (rc != 0)
            return errno_to_nfs4(rc);
        if (sync_status != NFS4_OK)
            return sync_status;

        xdr_encode_uint32(rep, data_len_raw);
        xdr_encode_uint32(rep, write_committed(config, stable));
        encode_write_verifier(config, rep);
        return NFS4_OK;
    }

    /* Regular file write */
    char *path = fh_to_path(config, ctx->current_fh, ctx->current_fh_len);
    if (!path) return NFS4ERR_STALE;

    if (!config->ops->write) {
        free(path);
        return NFS4ERR_ROFS;
    }

    struct fuse_file_info fi;
    open_file_info(conn, sid_other, cur_ino, &fi);

    int n = config->ops->write(path, (const char *)data, data_len_raw,
                                (off_t)offset, &fi);
    if (n < 0) {
        free(path);
        return errno_to_nfs4(n);
    }

    uint32_t sync_status = NFS4_OK;
    if (write_needs_sync(config, stable))
        sync_status = sync_file(config, path, &fi);
    free(path);
    if (sync_status != NFS4_OK)
        return sync_status;

    xdr_encode_uint32(rep, (uint32_t)n);
    xdr_encode_uint32(rep, write_committed(config, stable));

    encode_write_verifier(config, rep);

    return NFS4_OK;
}

static uint32_t handle_commit(const darwinfuse_config_t *config,
                               nfs4_conn_state_t *conn,
                               nfs4_request_ctx_t *ctx,
                               xdr_buf_t *req, xdr_buf_t *rep)
{
    /* Decode: offset (uint64), count (uint32) */
    xdr_decode_uint64(req);
    xdr_decode_uint32(req);
    if (req->error)
        return NFS4ERR_INVAL;

    char *path = fh_to_path(config, ctx->current_fh, ctx->current_fh_len);
    if (!path)
        return NFS4ERR_STALE;
    struct fuse_file_info fi;
    memset(&fi, 0, sizeof(fi));
    uint32_t status = sync_file(config, path, &fi);
    free(path);
    if (status != NFS4_OK)
        return status;

    encode_write_verifier(config, rep);

    return NFS4_OK;
}

/* Exercise the actual COMMIT response, not just its fsync callback helper. */
int nfs4_test_sync_acknowledgement(void)
{
    struct fuse_operations ops;
    darwinfuse_config_t config;
    nfs4_request_ctx_t ctx;
    nfs4_conn_state_t conn;
    uint8_t request_bytes[12] = {0};
    uint8_t write_bytes[40];
    uint8_t reply_bytes[16];
    uint8_t stateid[12] = {0};
    xdr_buf_t request, write_request, reply;
    uint32_t status;
    memset(&ops, 0, sizeof(ops));
    memset(&config, 0, sizeof(config));
    memset(&ctx, 0, sizeof(ctx));
    memset(&conn, 0, sizeof(conn));
    pthread_mutex_init(&conn.lock, NULL);
    config.ops = &ops;
    config.inode_table = dfuse_itable_create();
    if (!config.inode_table)
        return 1;
    dfuse_ino_t ino = dfuse_itable_get_or_create(config.inode_table,
                                                 "/sync-test");
    if (!ino) {
        dfuse_itable_destroy(config.inode_table);
        return 2;
    }
    fh_set_ino(ctx.current_fh, &ctx.current_fh_len, ino);

    xdr_init(&request, request_bytes, sizeof(request_bytes));
    xdr_init(&reply, reply_bytes, sizeof(reply_bytes));
    if (handle_commit(&config, NULL, &ctx, &request, &reply) != NFS4ERR_IO)
        status = 3;
    else {
        ops.fsync = sync_test_failure;
        xdr_reset(&request);
        xdr_reset(&reply);
        if (handle_commit(&config, NULL, &ctx, &request, &reply) != NFS4ERR_NOSPC)
            status = 4;
        else {
            ops.fsync = sync_test_success;
            xdr_reset(&request);
            xdr_reset(&reply);
            status = handle_commit(&config, NULL, &ctx, &request, &reply) == NFS4_OK
                     && reply.pos == 8 && !reply.error ? 0 : 5;
        }
    }

    if (status == 0) {
        ops.write = sync_test_write;
        xdr_init(&write_request, write_bytes, sizeof(write_bytes));
        xdr_encode_uint32(&write_request, 0);
        xdr_encode_opaque_fixed(&write_request, stateid, sizeof(stateid));
        xdr_encode_uint64(&write_request, 0);
        xdr_encode_uint32(&write_request, FILE_SYNC4);
        xdr_encode_opaque(&write_request, "abc", 3);
        xdr_reset(&write_request);
        xdr_reset(&reply);
        ops.fsync = sync_test_failure;
        if (handle_write(&config, &conn, &ctx, &write_request, &reply)
            != NFS4ERR_NOSPC || reply.pos != 0)
            status = 6;
        else {
            ops.fsync = sync_test_success;
            xdr_reset(&write_request);
            xdr_reset(&reply);
            if (handle_write(&config, &conn, &ctx, &write_request, &reply)
                != NFS4_OK || reply.pos != 16 || reply.error)
                status = 7;
            else {
                xdr_reset(&reply);
                status = xdr_decode_uint32(&reply) == 3 &&
                         xdr_decode_uint32(&reply) == FILE_SYNC4 ? 0 : 8;
            }
        }
    }
    pthread_mutex_destroy(&conn.lock);
    dfuse_itable_destroy(config.inode_table);
    return (int)status;
}

/* A write that is durable once it returns is reported FILE_SYNC4 without a
 * sync, so the client never commits it; otherwise an unstable write stays
 * unstable and a stable one syncs. */
static unsigned durable_test_syncs;

static int durable_test_fsync(const char *path, int data_only,
                              struct fuse_file_info *fi)
{
    (void)path;
    (void)data_only;
    (void)fi;
    durable_test_syncs++;
    return 0;
}

static uint32_t durable_test_committed(const darwinfuse_config_t *config,
                                       nfs4_conn_state_t *conn,
                                       nfs4_request_ctx_t *ctx,
                                       uint32_t stable)
{
    uint8_t request_bytes[40];
    uint8_t reply_bytes[16];
    uint8_t stateid[12] = {0};
    xdr_buf_t request, reply;
    xdr_init(&request, request_bytes, sizeof(request_bytes));
    xdr_encode_uint32(&request, 0);
    xdr_encode_opaque_fixed(&request, stateid, sizeof(stateid));
    xdr_encode_uint64(&request, 0);
    xdr_encode_uint32(&request, stable);
    xdr_encode_opaque(&request, "abc", 3);
    xdr_reset(&request);
    xdr_init(&reply, reply_bytes, sizeof(reply_bytes));
    if (handle_write(config, conn, ctx, &request, &reply) != NFS4_OK)
        return UINT32_MAX;
    xdr_reset(&reply);
    if (xdr_decode_uint32(&reply) != 3)
        return UINT32_MAX;
    return xdr_decode_uint32(&reply);
}

int nfs4_test_durable_writes(void)
{
    struct fuse_operations ops;
    darwinfuse_config_t config;
    nfs4_request_ctx_t ctx;
    nfs4_conn_state_t conn;
    memset(&ops, 0, sizeof(ops));
    memset(&config, 0, sizeof(config));
    memset(&ctx, 0, sizeof(ctx));
    memset(&conn, 0, sizeof(conn));
    pthread_mutex_init(&conn.lock, NULL);
    ops.write = sync_test_write;
    ops.fsync = durable_test_fsync;
    config.ops = &ops;
    config.inode_table = dfuse_itable_create();
    if (!config.inode_table)
        return 1;
    dfuse_ino_t ino = dfuse_itable_get_or_create(config.inode_table,
                                                 "/sync-test");
    fh_set_ino(ctx.current_fh, &ctx.current_fh_len, ino);
    durable_test_syncs = 0;

    int status = 0;
    if (durable_test_committed(&config, &conn, &ctx, UNSTABLE4) != UNSTABLE4 ||
        durable_test_syncs != 0)
        status = 2;
    else if (durable_test_committed(&config, &conn, &ctx, FILE_SYNC4) != FILE_SYNC4 ||
             durable_test_syncs != 1)
        status = 3;
    else {
        config.durable_writes = 1;
        if (durable_test_committed(&config, &conn, &ctx, UNSTABLE4) != FILE_SYNC4 ||
            durable_test_committed(&config, &conn, &ctx, FILE_SYNC4) != FILE_SYNC4 ||
            durable_test_syncs != 1)
            status = 4;
    }
    pthread_mutex_destroy(&conn.lock);
    dfuse_itable_destroy(config.inode_table);
    return status;
}

/* READ answers from the reply buffer itself: a short read reports EOF

 * without an attribute callback, and a full one asks for the size. */
static const char read_test_content[] = "0123456789";
static unsigned read_test_attribute_calls;

static int read_test_read(const char *path, char *buf, size_t size,
                          off_t offset, struct fuse_file_info *fi)
{
    (void)path;
    (void)fi;
    size_t length = sizeof(read_test_content) - 1;
    if ((size_t)offset >= length)
        return 0;
    size_t n = length - (size_t)offset;
    if (n > size)
        n = size;
    memcpy(buf, read_test_content + offset, n);
    return (int)n;
}

static int read_test_getattr(const char *path, struct stat *st)
{
    (void)path;
    memset(st, 0, sizeof(*st));
    st->st_mode = S_IFREG | 0644;
    st->st_size = (off_t)(sizeof(read_test_content) - 1);
    read_test_attribute_calls++;
    return 0;
}

/* Returns 0, or the number of the failed expectation. */
static int read_test_case(const darwinfuse_config_t *config,
                          nfs4_conn_state_t *conn, nfs4_request_ctx_t *ctx,
                          uint32_t count, unsigned attribute_calls,
                          int eof, const char *data)
{
    uint8_t request_bytes[32];
    uint8_t reply_bytes[64];
    uint8_t stateid[12] = {0};
    xdr_buf_t request, reply;
    xdr_init(&request, request_bytes, sizeof(request_bytes));
    xdr_encode_uint32(&request, 0);
    xdr_encode_opaque_fixed(&request, stateid, sizeof(stateid));
    xdr_encode_uint64(&request, 0);
    xdr_encode_uint32(&request, count);
    xdr_reset(&request);
    memset(reply_bytes, 0xA5, sizeof(reply_bytes));
    xdr_init(&reply, reply_bytes, sizeof(reply_bytes));
    read_test_attribute_calls = 0;

    if (handle_read(config, conn, ctx, &request, &reply) != NFS4_OK)
        return 1;
    if (read_test_attribute_calls != attribute_calls)
        return 2;
    size_t length = strlen(data);
    size_t padded = (length + 3) & ~(size_t)3;
    if (reply.error || reply.pos != 8 + padded)
        return 3;
    xdr_reset(&reply);
    if (xdr_decode_bool(&reply) != eof ||
        xdr_decode_uint32(&reply) != length)
        return 4;
    if (memcmp(reply.data + reply.pos, data, length) != 0)
        return 5;
    for (size_t i = length; i < padded; i++) {
        if (reply.data[reply.pos + i] != 0)
            return 6;
    }
    return 0;
}

int nfs4_test_read_reply(void)
{
    struct fuse_operations ops;
    darwinfuse_config_t config;
    nfs4_request_ctx_t ctx;
    nfs4_conn_state_t conn;
    memset(&ops, 0, sizeof(ops));
    memset(&config, 0, sizeof(config));
    memset(&ctx, 0, sizeof(ctx));
    memset(&conn, 0, sizeof(conn));
    pthread_mutex_init(&conn.lock, NULL);
    ops.read = read_test_read;
    ops.getattr = read_test_getattr;
    config.ops = &ops;
    config.inode_table = dfuse_itable_create();
    if (!config.inode_table)
        return 1;
    dfuse_ino_t ino = dfuse_itable_get_or_create(config.inode_table,
                                                 "/read-test");
    fh_set_ino(ctx.current_fh, &ctx.current_fh_len, ino);

    int status = 0;
    int failure;
    if ((failure = read_test_case(&config, &conn, &ctx, 64, 0, 1,
                                  "0123456789")) != 0)
        status = 10 + failure;
    else if ((failure = read_test_case(&config, &conn, &ctx, 10, 1, 1,
                                       "0123456789")) != 0)
        status = 20 + failure;
    else if ((failure = read_test_case(&config, &conn, &ctx, 3, 1, 0,
                                       "012")) != 0)
        status = 30 + failure;
    pthread_mutex_destroy(&conn.lock);
    dfuse_itable_destroy(config.inode_table);
    return status;
}

/* GETATTR reports the change label the filesystem carried with the
 * attributes, and an unlabeled object never reports the same value twice. */
static uint64_t change_test_label;

static int change_test_getattr(const char *path, struct stat *st)
{
    (void)path;
    memset(st, 0, sizeof(*st));
    st->st_mode = S_IFREG | 0644;
    st->st_qspare[0] = (int64_t)change_test_label;
    return 0;
}

static uint64_t change_test_getattr_change(const darwinfuse_config_t *config,
                                           nfs4_conn_state_t *conn,
                                           nfs4_request_ctx_t *ctx)
{
    uint8_t request_bytes[8];
    uint8_t reply_bytes[32];
    xdr_buf_t request, reply;
    xdr_init(&request, request_bytes, sizeof(request_bytes));
    xdr_encode_uint32(&request, 1);
    xdr_encode_uint32(&request, 1u << FATTR4_CHANGE);
    xdr_reset(&request);
    xdr_init(&reply, reply_bytes, sizeof(reply_bytes));
    if (handle_getattr(config, conn, ctx, &request, &reply) != NFS4_OK)
        return 0;
    xdr_reset(&reply);
    if (xdr_decode_uint32(&reply) != 1 ||
        xdr_decode_uint32(&reply) != (1u << FATTR4_CHANGE) ||
        xdr_decode_uint32(&reply) != 8)
        return 0;
    return xdr_decode_uint64(&reply);
}

int nfs4_test_change_attribute(void)
{
    struct fuse_operations ops;
    darwinfuse_config_t config;
    nfs4_request_ctx_t ctx;
    nfs4_conn_state_t conn;
    memset(&ops, 0, sizeof(ops));
    memset(&config, 0, sizeof(config));
    memset(&ctx, 0, sizeof(ctx));
    memset(&conn, 0, sizeof(conn));
    pthread_mutex_init(&conn.lock, NULL);
    ops.getattr = change_test_getattr;
    config.ops = &ops;
    config.inode_table = dfuse_itable_create();
    if (!config.inode_table)
        return 1;
    dfuse_ino_t ino = dfuse_itable_get_or_create(config.inode_table,
                                                 "/change-test");
    fh_set_ino(ctx.current_fh, &ctx.current_fh_len, ino);

    int status = 0;
    change_test_label = UINT64_C(0x1122334455667788);
    if (change_test_getattr_change(&config, &conn, &ctx) != change_test_label ||
        change_test_getattr_change(&config, &conn, &ctx) != change_test_label)
        status = 2;
    else {
        /* Unlabeled, and a label in the reserved half, are both fresh. */
        change_test_label = 0;
        uint64_t first = change_test_getattr_change(&config, &conn, &ctx);
        change_test_label = FUSE_CHANGE_UNLABELED | 1;
        uint64_t second = change_test_getattr_change(&config, &conn, &ctx);
        uint64_t third = change_test_getattr_change(&config, &conn, &ctx);
        if (first == 0 || first == second || second == third ||
            first < FUSE_CHANGE_UNLABELED)
            status = 3;
    }

    pthread_mutex_destroy(&conn.lock);
    dfuse_itable_destroy(config.inode_table);
    return status;
}

/* ---- CREATE (non-regular files: mkdir, symlink, mknod) ---- */

static uint32_t handle_create(const darwinfuse_config_t *config,
                               nfs4_conn_state_t *conn,
                               nfs4_request_ctx_t *ctx,
                               xdr_buf_t *req, xdr_buf_t *rep)
{
    uint64_t change_before = fresh_change(config);

    /* Current FH must be a directory */
    char *dir_path = fh_to_path(config, ctx->current_fh, ctx->current_fh_len);
    if (!dir_path) return NFS4ERR_STALE;

    /* Decode: objtype (ftype4) */
    uint32_t objtype = xdr_decode_uint32(req);

    /* Type-specific data */
    char linkdata[1024] = {0};
    dev_t dev = 0;

    switch (objtype) {
    case NF4LNK:
        /* symlink: linkdata is a utf8str_cs */
        xdr_decode_string(req, linkdata, sizeof(linkdata));
        break;
    case NF4BLK:
    case NF4CHR:
        /* specdata4: { specdata1, specdata2 } */
        {
            uint32_t major_num = xdr_decode_uint32(req);
            uint32_t minor_num = xdr_decode_uint32(req);
            dev = makedev(major_num, minor_num);
        }
        break;
    case NF4DIR:
    case NF4FIFO:
    case NF4SOCK:
        /* no type-specific data */
        break;
    default:
        free(dir_path);
        return NFS4ERR_BADTYPE;
    }

    /* Decode: objname (component4) */
    char name[256];
    xdr_decode_string(req, name, sizeof(name));

    /* Decode: createattrs (fattr4) */
    uint32_t cr_bitmap[2] = {0, 0};
    int cr_nwords = 0;
    decode_bitmap(req, cr_bitmap, &cr_nwords);

    uint8_t cr_attr_data[1024];
    uint32_t cr_len = xdr_decode_opaque(req, cr_attr_data, sizeof(cr_attr_data));
    if (req->error) { free(dir_path); return NFS4ERR_INVAL; }

    /* Extract mode from createattrs */
    mode_t mode = 0755;
    if (bitmap_isset(cr_bitmap, cr_nwords, FATTR4_MODE)) {
        xdr_buf_t cad;
        xdr_init(&cad, cr_attr_data, cr_len);
        if (bitmap_isset(cr_bitmap, cr_nwords, FATTR4_SIZE))
            xdr_decode_uint64(&cad);  /* skip size */
        mode = (mode_t)xdr_decode_uint32(&cad);
    }

    /* Build child path */
    char child_path[1024];
    build_child_path(child_path, sizeof(child_path), dir_path, name);
    free(dir_path);
    DFUSE_LOG("  CREATE type=%u path='%s'", objtype, child_path);

    int rc;
    switch (objtype) {
    case NF4DIR:
        if (!config->ops->mkdir) return NFS4ERR_NOTSUPP;
        rc = config->ops->mkdir(child_path, mode);
        break;
    case NF4LNK:
        if (!config->ops->symlink) return NFS4ERR_NOTSUPP;
        rc = config->ops->symlink(linkdata, child_path);
        break;
    case NF4BLK:
        if (!config->ops->mknod) return NFS4ERR_NOTSUPP;
        rc = config->ops->mknod(child_path, S_IFBLK | mode, dev);
        break;
    case NF4CHR:
        if (!config->ops->mknod) return NFS4ERR_NOTSUPP;
        rc = config->ops->mknod(child_path, S_IFCHR | mode, dev);
        break;
    case NF4FIFO:
        if (!config->ops->mknod) return NFS4ERR_NOTSUPP;
        rc = config->ops->mknod(child_path, S_IFIFO | mode, 0);
        break;
    case NF4SOCK:
        if (!config->ops->mknod) return NFS4ERR_NOTSUPP;
        rc = config->ops->mknod(child_path, S_IFSOCK | mode, 0);
        break;
    default:
        return NFS4ERR_BADTYPE;
    }

    if (rc != 0) {
        DFUSE_LOG("  CREATE '%s' -> error %d", child_path, rc);
        return errno_to_nfs4(rc);
    }
    uint64_t change_after = namespace_changed(config);

    /* Assign inode and set as current FH */
    dfuse_ino_t child_ino = dfuse_itable_get_or_create(config->inode_table, child_path);
    if (child_ino == 0) return NFS4ERR_SERVERFAULT;
    fh_set_ino(ctx->current_fh, &ctx->current_fh_len, child_ino);

    encode_change_info(rep, 0, change_before, change_after);

    /* attrset bitmap (empty) */
    xdr_encode_uint32(rep, 0);

    return NFS4_OK;
}

/* ---- REMOVE ---- */

static uint32_t handle_remove(const darwinfuse_config_t *config,
                               nfs4_conn_state_t *conn,
                               nfs4_request_ctx_t *ctx,
                               xdr_buf_t *req, xdr_buf_t *rep)
{
    uint64_t change_before = fresh_change(config);

    dfuse_ino_t dir_ino = fh_get_ino(ctx->current_fh, ctx->current_fh_len);
    if (dir_ino == 0) return NFS4ERR_BADHANDLE;

    dfuse_ino_type_t dir_type = dfuse_itable_type(config->inode_table, dir_ino);

    char name[256];
    xdr_decode_string(req, name, sizeof(name));
    if (req->error) return NFS4ERR_INVAL;

    if (dir_type == DFUSE_INO_ATTRDIR) {
        /* Remove a named attribute (xattr) */
        char *file_path = xattr_file_path(config, dir_ino);
        if (!file_path) return NFS4ERR_STALE;
        if (!config->ops->removexattr) { free(file_path); return NFS4ERR_NOTSUPP; }

        int rc = namedattr_remove(config, file_path, name);
        free(file_path);
        if (rc != 0) return errno_to_nfs4(rc);

        encode_change_info(rep, 0, change_before, namespace_changed(config));
        return NFS4_OK;
    }

    /* Regular directory remove */
    char *dir_path = dfuse_itable_path_dup(config->inode_table, dir_ino);
    if (!dir_path) return NFS4ERR_STALE;

    char child_path[1024];
    build_child_path(child_path, sizeof(child_path), dir_path, name);
    free(dir_path);

    struct stat st;
    memset(&st, 0, sizeof(st));
    if (config->ops->getattr) {
        int rc = config->ops->getattr(child_path, &st);
        if (rc != 0) return errno_to_nfs4(rc);
    }

    int rc;
    if (S_ISDIR(st.st_mode)) {
        if (!config->ops->rmdir) return NFS4ERR_NOTSUPP;
        rc = config->ops->rmdir(child_path);
    } else {
        if (!config->ops->unlink) return NFS4ERR_NOTSUPP;
        rc = config->ops->unlink(child_path);
    }

    if (rc != 0) return errno_to_nfs4(rc);

    dfuse_itable_remove(config->inode_table, child_path);

    encode_change_info(rep, 0, change_before, namespace_changed(config));

    return NFS4_OK;
}

/* ---- RENAME ---- */

static uint32_t handle_rename(const darwinfuse_config_t *config,
                               nfs4_conn_state_t *conn,
                               nfs4_request_ctx_t *ctx,
                               xdr_buf_t *req, xdr_buf_t *rep)
{
    uint64_t change_before = fresh_change(config);

    /* Saved FH = source directory, current FH = target directory */
    char *src_dir = dfuse_itable_path_dup(config->inode_table,
                          fh_get_ino(ctx->saved_fh, ctx->saved_fh_len));
    char *dst_dir = fh_to_path(config, ctx->current_fh, ctx->current_fh_len);

    if (!src_dir || !dst_dir) {
        free(src_dir);
        free(dst_dir);
        return NFS4ERR_STALE;
    }

    char old_name[256], new_name[256];
    xdr_decode_string(req, old_name, sizeof(old_name));
    xdr_decode_string(req, new_name, sizeof(new_name));
    if (req->error) {
        free(src_dir);
        free(dst_dir);
        return NFS4ERR_INVAL;
    }

    char old_path[1024], new_path[1024];
    build_child_path(old_path, sizeof(old_path), src_dir, old_name);
    build_child_path(new_path, sizeof(new_path), dst_dir, new_name);
    free(src_dir);
    free(dst_dir);

    if (!config->ops->rename) return NFS4ERR_NOTSUPP;
    int rc = config->ops->rename(old_path, new_path);
    if (rc != 0) return errno_to_nfs4(rc);

    /* Update inode table */
    dfuse_itable_rename(config->inode_table, old_path, new_path);

    /* Encode: source_cinfo, target_cinfo */
    uint64_t change_after = namespace_changed(config);
    encode_change_info(rep, 0, change_before, change_after);
    encode_change_info(rep, 0, change_before, change_after);

    return NFS4_OK;
}

/* ---- LINK ---- */

static uint32_t handle_link(const darwinfuse_config_t *config,
                             nfs4_conn_state_t *conn,
                             nfs4_request_ctx_t *ctx,
                             xdr_buf_t *req, xdr_buf_t *rep)
{
    uint64_t change_before = fresh_change(config);

    /* Saved FH = existing file, current FH = target directory */
    char *existing = dfuse_itable_path_dup(config->inode_table,
                           fh_get_ino(ctx->saved_fh, ctx->saved_fh_len));
    char *dir_path = fh_to_path(config, ctx->current_fh, ctx->current_fh_len);

    if (!existing || !dir_path) {
        free(existing);
        free(dir_path);
        return NFS4ERR_STALE;
    }

    char new_name[256];
    xdr_decode_string(req, new_name, sizeof(new_name));
    if (req->error) {
        free(existing);
        free(dir_path);
        return NFS4ERR_INVAL;
    }

    char new_path[1024];
    build_child_path(new_path, sizeof(new_path), dir_path, new_name);
    free(dir_path);

    if (!config->ops->link) { free(existing); return NFS4ERR_NOTSUPP; }
    int rc = config->ops->link(existing, new_path);
    free(existing);
    if (rc != 0) return errno_to_nfs4(rc);

    encode_change_info(rep, 0, change_before, namespace_changed(config));

    return NFS4_OK;
}

/* ---- READLINK ---- */

static uint32_t handle_readlink(const darwinfuse_config_t *config,
                                 nfs4_conn_state_t *conn,
                                 nfs4_request_ctx_t *ctx,
                                 xdr_buf_t *req, xdr_buf_t *rep)
{
    (void)req;

    char *path = fh_to_path(config, ctx->current_fh, ctx->current_fh_len);
    if (!path) return NFS4ERR_STALE;

    if (!config->ops->readlink) {
        free(path);
        return NFS4ERR_NOTSUPP;
    }

    char link_target[1024];
    int rc = config->ops->readlink(path, link_target, sizeof(link_target));
    free(path);
    if (rc != 0) return errno_to_nfs4(rc);

    /* Ensure null-terminated */
    link_target[sizeof(link_target) - 1] = '\0';

    /* Encode as utf8str_cs (string) */
    xdr_encode_string(rep, link_target);
    return NFS4_OK;
}

/* ---- Session management ---- */

static uint32_t handle_setclientid(const darwinfuse_config_t *config,
                                    nfs4_conn_state_t *conn,
                                    nfs4_request_ctx_t *ctx,
                                    xdr_buf_t *req, xdr_buf_t *rep)
{
    (void)config;

    /* client verifier (8 bytes) */
    uint8_t verifier[8];
    xdr_decode_opaque_fixed(req, verifier, 8);

    /* client id string (opaque) */
    xdr_skip_opaque(req);

    /* callback (cb_program, cb_location: netid + addr) */
    xdr_decode_uint32(req);    /* cb_program */
    xdr_skip_string(req);      /* r_netid */
    xdr_skip_string(req);      /* r_addr */

    /* callback_ident */
    xdr_decode_uint32(req);

    if (req->error) return NFS4ERR_INVAL;

    /* Generate clientid */
    uint64_t new_clientid = ((uint64_t)arc4random() << 32) | arc4random();
    uint64_t new_server_verifier = ((uint64_t)arc4random() << 32) | arc4random();

    pthread_mutex_lock(&conn->lock);
    conn->clientid = new_clientid;
    memcpy(&conn->client_verifier, verifier, 8);
    conn->server_verifier = new_server_verifier;
    conn->confirmed = 0;
    pthread_mutex_unlock(&conn->lock);

    /* Encode reply: clientid (uint64), verifier (8 bytes) */
    xdr_encode_uint64(rep, new_clientid);
    uint8_t sv[8];
    memcpy(sv, &new_server_verifier, 8);
    xdr_encode_opaque_fixed(rep, sv, 8);

    return NFS4_OK;
}

static uint32_t handle_setclientid_confirm(const darwinfuse_config_t *config,
                                            nfs4_conn_state_t *conn,
                                            nfs4_request_ctx_t *ctx,
                                            xdr_buf_t *req, xdr_buf_t *rep)
{
    (void)config; (void)rep;

    uint64_t clientid = xdr_decode_uint64(req);
    uint8_t verifier[8];
    xdr_decode_opaque_fixed(req, verifier, 8);

    pthread_mutex_lock(&conn->lock);
    if (clientid != conn->clientid) {
        pthread_mutex_unlock(&conn->lock);
        return NFS4ERR_STALE_CLIENTID;
    }

    uint64_t v;
    memcpy(&v, verifier, 8);
    if (v != conn->server_verifier) {
        pthread_mutex_unlock(&conn->lock);
        return NFS4ERR_CLID_INUSE;
    }

    conn->confirmed = 1;
    pthread_mutex_unlock(&conn->lock);
    return NFS4_OK;
}

static uint32_t handle_renew(const darwinfuse_config_t *config,
                              nfs4_conn_state_t *conn,
                              nfs4_request_ctx_t *ctx,
                              xdr_buf_t *req, xdr_buf_t *rep)
{
    (void)config; (void)rep;

    uint64_t clientid = xdr_decode_uint64(req);

    pthread_mutex_lock(&conn->lock);
    int match = (clientid == conn->clientid);
    pthread_mutex_unlock(&conn->lock);

    if (!match)
        return NFS4ERR_STALE_CLIENTID;

    /* No-op — we never expire localhost clients */
    return NFS4_OK;
}

/*
 * LOCK, LOCKT, LOCKU.  The only client is the local kernel, which checks
 * every lock against the locks all its processes hold before it asks the
 * server, so no request reaching this server can conflict: granting is exact.
 */
static uint32_t handle_lock(const darwinfuse_config_t *config,
                             nfs4_conn_state_t *conn,
                             nfs4_request_ctx_t *ctx,
                             xdr_buf_t *req, xdr_buf_t *rep)
{
    (void)config;

    uint32_t locktype = xdr_decode_uint32(req);
    uint32_t reclaim  = xdr_decode_uint32(req);
    (void)locktype; (void)reclaim;
    xdr_decode_uint64(req);  /* offset */
    xdr_decode_uint64(req);  /* length */

    uint32_t new_lock_owner = xdr_decode_uint32(req);
    if (new_lock_owner) {
        xdr_decode_uint32(req);  /* open_seqid */
        xdr_decode_uint32(req);  /* open_stateid.seqid */
        xdr_skip(req, 12);       /* open_stateid.other */
        xdr_decode_uint32(req);  /* lock_seqid */
        xdr_decode_uint64(req);  /* lock_owner.clientid */
        xdr_skip_opaque(req);    /* lock_owner.owner */
    } else {
        xdr_decode_uint32(req);  /* lock_stateid.seqid */
        xdr_skip(req, 12);       /* lock_stateid.other */
        xdr_decode_uint32(req);  /* lock_seqid */
    }
    if (req->error) return NFS4ERR_INVAL;

    /* Return a dummy lock stateid */
    xdr_encode_uint32(rep, 1);
    uint8_t zero[12] = {0};
    zero[0] = 0x4C;
    xdr_encode_opaque_fixed(rep, zero, 12);

    return NFS4_OK;
}

static uint32_t handle_lockt(const darwinfuse_config_t *config,
                              nfs4_conn_state_t *conn,
                              nfs4_request_ctx_t *ctx,
                              xdr_buf_t *req, xdr_buf_t *rep)
{
    (void)config; (void)conn; (void)rep;

    xdr_decode_uint32(req);  /* locktype */
    xdr_decode_uint64(req);  /* offset */
    xdr_decode_uint64(req);  /* length */
    xdr_decode_uint64(req);  /* lock_owner.clientid */
    xdr_skip_opaque(req);    /* lock_owner.owner */

    return NFS4_OK;
}

static uint32_t handle_locku(const darwinfuse_config_t *config,
                              nfs4_conn_state_t *conn,
                              nfs4_request_ctx_t *ctx,
                              xdr_buf_t *req, xdr_buf_t *rep)
{
    (void)config; (void)conn;

    xdr_decode_uint32(req);  /* locktype */
    xdr_decode_uint32(req);  /* seqid */
    xdr_decode_uint32(req);  /* lock_stateid.seqid */
    xdr_skip(req, 12);       /* lock_stateid.other */
    xdr_decode_uint64(req);  /* offset */
    xdr_decode_uint64(req);  /* length */
    if (req->error) return NFS4ERR_INVAL;

    xdr_encode_uint32(rep, 2);
    uint8_t zero[12] = {0};
    zero[0] = 0x4C;
    xdr_encode_opaque_fixed(rep, zero, 12);

    return NFS4_OK;
}

static uint32_t handle_release_lockowner(const darwinfuse_config_t *config,
                                          nfs4_conn_state_t *conn,
                                          nfs4_request_ctx_t *ctx,
                                          xdr_buf_t *req, xdr_buf_t *rep)
{
    (void)config; (void)conn; (void)rep;
    xdr_decode_uint64(req);  /* clientid */
    xdr_skip_opaque(req);    /* owner */
    return NFS4_OK;
}

static uint32_t handle_secinfo(const darwinfuse_config_t *config,
                                nfs4_conn_state_t *conn,
                                nfs4_request_ctx_t *ctx,
                                xdr_buf_t *req, xdr_buf_t *rep)
{
    (void)config; (void)conn;

    char name[256];
    xdr_decode_string(req, name, sizeof(name));

    xdr_encode_uint32(rep, 1);
    xdr_encode_uint32(rep, AUTH_SYS);
    return NFS4_OK;
}

/*
 * VERIFY and NVERIFY (RFC 7530 s16.35, s16.15): compare the client's
 * attribute values with the current object's, encoded the same way.
 * VERIFY succeeds when every value matches; NVERIFY when any differs.
 */
static uint32_t verify_attributes(const darwinfuse_config_t *config,
                                  nfs4_conn_state_t *conn,
                                  nfs4_request_ctx_t *ctx,
                                  xdr_buf_t *req, int expect_same)
{
    uint32_t bitmap[2] = {0, 0};
    uint32_t words = xdr_decode_uint32(req);
    for (uint32_t i = 0; i < words; i++) {
        uint32_t word = xdr_decode_uint32(req);
        if (i < 2)
            bitmap[i] = word;
        else if (word != 0)
            return NFS4ERR_ATTRNOTSUPP;
    }
    int nwords = words < 2 ? (int)words : 2;
    uint8_t expected[4096];
    uint32_t expected_len = xdr_decode_opaque(req, expected, sizeof(expected));
    if (req->error || expected_len > sizeof(expected)) return NFS4ERR_INVAL;
    for (int i = 0; i < nwords; i++) {
        if (bitmap[i] & ~supported_bitmap[i])
            return NFS4ERR_ATTRNOTSUPP;
    }
    if (bitmap_isset(bitmap, nwords, FATTR4_RDATTR_ERROR))
        return NFS4ERR_INVAL;

    struct stat st;
    uint32_t type_override;
    uint32_t status = current_attributes(config, conn, ctx, &st, &type_override);
    if (status != NFS4_OK) return status;

    uint8_t actual_buf[4096 + 16];
    xdr_buf_t actual;
    xdr_init(&actual, actual_buf, sizeof(actual_buf));
    encode_fattr4(&actual, &st, bitmap, nwords, ctx->current_fh,
                  ctx->current_fh_len, config, type_override);
    xdr_reset(&actual);
    uint32_t actual_words = xdr_decode_uint32(&actual);
    for (uint32_t i = 0; i < actual_words; i++)
        xdr_decode_uint32(&actual);
    uint32_t actual_len = xdr_decode_uint32(&actual);
    if (actual.error) return NFS4ERR_SERVERFAULT;

    int same = actual_len == expected_len &&
               memcmp(actual.data + actual.pos, expected, expected_len) == 0;
    if (expect_same)
        return same ? NFS4_OK : NFS4ERR_NOT_SAME;
    return same ? NFS4ERR_SAME : NFS4_OK;
}

/* ---- OPENATTR (named attributes / xattr) ---- */

static uint32_t handle_openattr(const darwinfuse_config_t *config,
                                 nfs4_conn_state_t *conn,
                                 nfs4_request_ctx_t *ctx,
                                 xdr_buf_t *req, xdr_buf_t *rep)
{
    (void)rep;

    /* Decode createdir (bool) */
    xdr_decode_uint32(req);

    /* Check if the filesystem supports xattrs */
    if (!config->ops->getxattr && !config->ops->listxattr)
        return NFS4ERR_NOTSUPP;

    dfuse_ino_t file_ino = fh_get_ino(ctx->current_fh, ctx->current_fh_len);
    if (file_ino == 0) return NFS4ERR_BADHANDLE;

    /* Verify the file exists */
    char *path = dfuse_itable_path_dup(config->inode_table, file_ino);
    if (!path) return NFS4ERR_STALE;
    free(path);

    /* Get or create an attribute directory inode */
    dfuse_ino_t attrdir_ino = dfuse_itable_get_attrdir(
        config->inode_table, file_ino);
    if (attrdir_ino == 0) return NFS4ERR_SERVERFAULT;

    /* Set current FH to the attribute directory */
    fh_set_ino(ctx->current_fh, &ctx->current_fh_len, attrdir_ino);

    return NFS4_OK;
}

/* ---- OPEN_DOWNGRADE ---- */

static uint32_t handle_open_downgrade(const darwinfuse_config_t *config,
                                       nfs4_conn_state_t *conn,
                                       nfs4_request_ctx_t *ctx,
                                       xdr_buf_t *req, xdr_buf_t *rep)
{
    (void)config;

    /* Decode: seqid */
    xdr_decode_uint32(req);

    /* Decode: stateid4 */
    xdr_decode_uint32(req);  /* seqid */
    uint8_t sid_other[12];
    xdr_decode_opaque_fixed(req, sid_other, 12);

    /* Decode: share_access, share_deny */
    uint32_t share_access = xdr_decode_uint32(req);
    xdr_decode_uint32(req);  /* share_deny */

    if (req->error) return NFS4ERR_INVAL;

    uint32_t updated_seqid;
    uint8_t updated_other[12];

    pthread_mutex_lock(&conn->lock);
    nfs4_open_file_t *of = find_open_file(conn, sid_other);
    if (!of) {
        pthread_mutex_unlock(&conn->lock);
        return NFS4ERR_BAD_STATEID;
    }

    /* Update access flags */
    if (share_access == OPEN4_SHARE_ACCESS_READ)
        of->flags = O_RDONLY;
    else if (share_access == OPEN4_SHARE_ACCESS_WRITE)
        of->flags = O_WRONLY;

    of->stateid.seqid++;
    updated_seqid = of->stateid.seqid;
    memcpy(updated_other, of->stateid.other, 12);
    pthread_mutex_unlock(&conn->lock);

    /* Return updated stateid */
    xdr_encode_uint32(rep, updated_seqid);
    xdr_encode_opaque_fixed(rep, updated_other, 12);

    return NFS4_OK;
}

/* ACCESS applies the caller's AUTH_SYS identity to the mode bits. */
static struct stat access_test_attributes;

static int access_test_getattr(const char *path, struct stat *st)
{
    (void)path;
    *st = access_test_attributes;
    return 0;
}

static uint32_t access_test_granted(const darwinfuse_config_t *config,
                                    nfs4_conn_state_t *conn,
                                    nfs4_request_ctx_t *ctx,
                                    mode_t mode, uid_t caller,
                                    unsigned ngroups, const gid_t *groups)
{
    uint8_t request_bytes[4];
    uint8_t reply_bytes[8];
    xdr_buf_t request, reply;
    access_test_attributes.st_mode = mode;
    access_test_attributes.st_uid = 501;
    access_test_attributes.st_gid = 20;
    darwinfuse_set_context(caller, 99, ngroups, groups);
    xdr_init(&request, request_bytes, sizeof(request_bytes));
    xdr_encode_uint32(&request, 0x3f);
    xdr_reset(&request);
    xdr_init(&reply, reply_bytes, sizeof(reply_bytes));
    if (handle_access(config, conn, ctx, &request, &reply) != NFS4_OK)
        return UINT32_MAX;
    xdr_reset(&reply);
    if (xdr_decode_uint32(&reply) != 0x3f)
        return UINT32_MAX;
    return xdr_decode_uint32(&reply);
}

int nfs4_test_access_rights(void)
{
    struct fuse_operations ops;
    darwinfuse_config_t config;
    nfs4_request_ctx_t ctx;
    nfs4_conn_state_t conn;
    memset(&ops, 0, sizeof(ops));
    memset(&config, 0, sizeof(config));
    memset(&ctx, 0, sizeof(ctx));
    memset(&conn, 0, sizeof(conn));
    memset(&access_test_attributes, 0, sizeof(access_test_attributes));
    ops.getattr = access_test_getattr;
    config.ops = &ops;
    config.inode_table = dfuse_itable_create();
    if (!config.inode_table)
        return 1;
    dfuse_ino_t ino = dfuse_itable_get_or_create(config.inode_table,
                                                 "/access-test");
    fh_set_ino(ctx.current_fh, &ctx.current_fh_len, ino);

    const gid_t staff[] = {20};
    const uint32_t write = ACCESS4_MODIFY | ACCESS4_EXTEND;
    int status = 0;
    if (access_test_granted(&config, &conn, &ctx, S_IFREG | 0644, 501, 0, NULL)
        != (ACCESS4_READ | write | ACCESS4_DELETE))
        status = 2;
    else if (access_test_granted(&config, &conn, &ctx, S_IFREG | 0644, 502, 0,
                                 NULL) != (ACCESS4_READ | ACCESS4_DELETE))
        status = 3;
    else if (access_test_granted(&config, &conn, &ctx, S_IFREG | 0070, 502, 1,
                                 staff) != (ACCESS4_READ | write |
                                            ACCESS4_EXECUTE | ACCESS4_DELETE))
        status = 4;
    else if (access_test_granted(&config, &conn, &ctx, S_IFREG | 0644, 0, 0,
                                 NULL) != (ACCESS4_READ | write | ACCESS4_DELETE))
        status = 5;
    else if (access_test_granted(&config, &conn, &ctx, S_IFREG | 0100, 0, 0,
                                 NULL) != (ACCESS4_READ | write |
                                           ACCESS4_EXECUTE | ACCESS4_DELETE))
        status = 6;
    else if (access_test_granted(&config, &conn, &ctx, S_IFDIR | 0555, 501, 0,
                                 NULL) != (ACCESS4_READ | ACCESS4_LOOKUP))
        status = 7;
    else if (access_test_granted(&config, &conn, &ctx, S_IFDIR | 0700, 501, 0,
                                 NULL) != 0x1f)
        status = 8;
    darwinfuse_set_context(0, 0, 0, NULL);
    dfuse_itable_destroy(config.inode_table);
    return status;
}

/* VERIFY matches exactly the current attribute values; NVERIFY inverts. */
static int verify_test_getattr(const char *path, struct stat *st)
{
    (void)path;
    memset(st, 0, sizeof(*st));
    st->st_mode = S_IFREG | 0640;
    st->st_size = 1234;
    return 0;
}

static uint32_t verify_test_run(const darwinfuse_config_t *config,
                                nfs4_conn_state_t *conn,
                                nfs4_request_ctx_t *ctx,
                                int expect_same, uint64_t size, uint32_t mode)
{
    uint8_t request_bytes[40];
    xdr_buf_t request;
    xdr_init(&request, request_bytes, sizeof(request_bytes));
    xdr_encode_uint32(&request, 2);
    xdr_encode_uint32(&request, 1u << FATTR4_SIZE);
    xdr_encode_uint32(&request, 1u << (FATTR4_MODE - 32));
    xdr_encode_uint32(&request, 12);
    xdr_encode_uint64(&request, size);
    xdr_encode_uint32(&request, mode);
    xdr_reset(&request);
    return verify_attributes(config, conn, ctx, &request, expect_same);
}

int nfs4_test_verify_attributes(void)
{
    struct fuse_operations ops;
    darwinfuse_config_t config;
    nfs4_request_ctx_t ctx;
    nfs4_conn_state_t conn;
    memset(&ops, 0, sizeof(ops));
    memset(&config, 0, sizeof(config));
    memset(&ctx, 0, sizeof(ctx));
    memset(&conn, 0, sizeof(conn));
    ops.getattr = verify_test_getattr;
    config.ops = &ops;
    config.inode_table = dfuse_itable_create();
    if (!config.inode_table)
        return 1;
    dfuse_ino_t ino = dfuse_itable_get_or_create(config.inode_table,
                                                 "/verify-test");
    fh_set_ino(ctx.current_fh, &ctx.current_fh_len, ino);

    int status = 0;
    if (verify_test_run(&config, &conn, &ctx, 1, 1234, 0640) != NFS4_OK)
        status = 2;
    else if (verify_test_run(&config, &conn, &ctx, 1, 1235, 0640) != NFS4ERR_NOT_SAME)
        status = 3;
    else if (verify_test_run(&config, &conn, &ctx, 0, 1234, 0640) != NFS4ERR_SAME)
        status = 4;
    else if (verify_test_run(&config, &conn, &ctx, 0, 1234, 0600) != NFS4_OK)
        status = 5;
    dfuse_itable_destroy(config.inode_table);
    return status;
}

/* Teardown releases every tracked open's handle exactly once, including an
 * open whose name was removed. */
static unsigned release_test_flushes;
static unsigned release_test_releases;
static uint64_t release_test_handles;

static int release_test_flush(const char *path, struct fuse_file_info *fi)
{
    (void)path;
    (void)fi;
    release_test_flushes++;
    return 0;
}

static int release_test_release(const char *path, struct fuse_file_info *fi)
{
    (void)path;
    release_test_releases++;
    release_test_handles += fi->fh;
    return 0;
}

int nfs4_test_release_open_files(void)
{
    struct fuse_operations ops;
    darwinfuse_config_t config;
    nfs4_conn_state_t conn;
    memset(&ops, 0, sizeof(ops));
    memset(&config, 0, sizeof(config));
    memset(&conn, 0, sizeof(conn));
    pthread_mutex_init(&conn.lock, NULL);
    ops.flush = release_test_flush;
    ops.release = release_test_release;
    config.ops = &ops;
    config.inode_table = dfuse_itable_create();
    if (!config.inode_table)
        return 1;
    dfuse_ino_t kept = dfuse_itable_get_or_create(config.inode_table, "/kept");
    dfuse_ino_t removed = dfuse_itable_get_or_create(config.inode_table,
                                                     "/removed");
    dfuse_itable_remove(config.inode_table, "/removed");
    struct fuse_file_info first = {.fh = 3};
    struct fuse_file_info second = {.fh = 4};
    nfs4_stateid_t sid;
    release_test_flushes = release_test_releases = 0;
    release_test_handles = 0;

    int status = 0;
    if (track_open(&conn, kept, &first, &sid) != 0 ||
        track_open(&conn, removed, &second, &sid) != 0)
        status = 2;
    else {
        nfs4_release_open_files(&config, &conn);
        nfs4_release_open_files(&config, &conn);
        if (release_test_releases != 2 || release_test_flushes != 2 ||
            release_test_handles != 7 || conn.open_file_count != 0)
            status = 3;
    }
    pthread_mutex_destroy(&conn.lock);
    dfuse_itable_destroy(config.inode_table);
    return status;
}

/* ---- COMPOUND Dispatcher ---- */


int nfs4_dispatch_compound(const darwinfuse_config_t *config,
                            nfs4_conn_state_t *conn,
                            nfs4_request_ctx_t *ctx,
                            xdr_buf_t *request,
                            xdr_buf_t *reply)
{
    /* Decode COMPOUND4args: { tag, minorversion, argarray<> } */
    char tag[256] = {0};
    xdr_decode_string(request, tag, sizeof(tag));

    uint32_t minorversion = xdr_decode_uint32(request);
    uint32_t numops = xdr_decode_uint32(request);

    if (request->error) {
        DFUSE_ERR("Failed to decode COMPOUND header");
        return -1;
    }

    /* Check minor version */
    if (minorversion != 0) {
        xdr_encode_uint32(reply, NFS4ERR_MINOR_VERS_MISMATCH);
        xdr_encode_string(reply, tag);
        xdr_encode_uint32(reply, 0);
        return 0;
    }

    /* Reserve space for COMPOUND4res header, fill in later */
    size_t status_pos = xdr_getpos(reply);
    xdr_encode_uint32(reply, NFS4_OK);
    xdr_encode_string(reply, tag);
    size_t numres_pos = xdr_getpos(reply);
    xdr_encode_uint32(reply, 0);

    uint32_t overall_status = NFS4_OK;
    uint32_t completed_ops = 0;

    for (uint32_t i = 0; i < numops; i++) {
        uint32_t opnum = xdr_decode_uint32(request);
        if (request->error) break;

        DFUSE_LOG("  op[%u] = %u", i, opnum);

        xdr_encode_uint32(reply, opnum);

        size_t op_status_pos = xdr_getpos(reply);
        xdr_encode_uint32(reply, NFS4_OK);

        uint32_t status;
        switch (opnum) {
        case OP_PUTROOTFH:
            status = handle_putrootfh(config, conn, ctx, request, reply);
            break;
        case OP_PUTFH:
            status = handle_putfh(config, conn, ctx, request, reply);
            break;
        case OP_GETFH:
            status = handle_getfh(config, conn, ctx, request, reply);
            break;
        case OP_SAVEFH:
            status = handle_savefh(config, conn, ctx, request, reply);
            break;
        case OP_RESTOREFH:
            status = handle_restorefh(config, conn, ctx, request, reply);
            break;
        case OP_LOOKUP:
            status = handle_lookup(config, conn, ctx, request, reply);
            break;
        case OP_LOOKUPP:
            status = handle_lookupp(config, conn, ctx, request, reply);
            break;
        case OP_GETATTR:
            status = handle_getattr(config, conn, ctx, request, reply);
            break;
        case OP_SETATTR:
            status = handle_setattr(config, conn, ctx, request, reply);
            break;
        case OP_ACCESS:
            status = handle_access(config, conn, ctx, request, reply);
            break;
        case OP_READDIR:
            status = handle_readdir(config, conn, ctx, request, reply);
            break;
        case OP_OPEN:
            status = handle_open(config, conn, ctx, request, reply);
            break;
        case OP_OPEN_CONFIRM:
            status = handle_open_confirm(config, conn, ctx, request, reply);
            break;
        case OP_CLOSE:
            status = handle_close(config, conn, ctx, request, reply);
            break;
        case OP_READ:
            status = handle_read(config, conn, ctx, request, reply);
            break;
        case OP_WRITE:
            status = handle_write(config, conn, ctx, request, reply);
            break;
        case OP_COMMIT:
            status = handle_commit(config, conn, ctx, request, reply);
            break;
        case OP_CREATE:
            status = handle_create(config, conn, ctx, request, reply);
            break;
        case OP_REMOVE:
            status = handle_remove(config, conn, ctx, request, reply);
            break;
        case OP_RENAME:
            status = handle_rename(config, conn, ctx, request, reply);
            break;
        case OP_LINK:
            status = handle_link(config, conn, ctx, request, reply);
            break;
        case OP_READLINK:
            status = handle_readlink(config, conn, ctx, request, reply);
            break;
        case OP_OPENATTR:
            status = handle_openattr(config, conn, ctx, request, reply);
            break;
        case OP_OPEN_DOWNGRADE:
            status = handle_open_downgrade(config, conn, ctx, request, reply);
            break;
        case OP_SETCLIENTID:
            status = handle_setclientid(config, conn, ctx, request, reply);
            break;
        case OP_SETCLIENTID_CONFIRM:
            status = handle_setclientid_confirm(config, conn, ctx, request, reply);
            break;
        case OP_RENEW:
            status = handle_renew(config, conn, ctx, request, reply);
            break;
        case OP_LOCK:
            status = handle_lock(config, conn, ctx, request, reply);
            break;
        case OP_LOCKT:
            status = handle_lockt(config, conn, ctx, request, reply);
            break;
        case OP_LOCKU:
            status = handle_locku(config, conn, ctx, request, reply);
            break;
        case OP_RELEASE_LOCKOWNER:
            status = handle_release_lockowner(config, conn, ctx, request, reply);
            break;
        case OP_SECINFO:
            status = handle_secinfo(config, conn, ctx, request, reply);
            break;
        case OP_VERIFY:
            status = verify_attributes(config, conn, ctx, request, 1);
            break;
        case OP_NVERIFY:
            status = verify_attributes(config, conn, ctx, request, 0);
            break;
        default:
            DFUSE_LOG("  unsupported op %u", opnum);
            status = NFS4ERR_NOTSUPP;
            break;
        }

        /* Backpatch this op's status */
        size_t saved_pos = xdr_getpos(reply);
        xdr_setpos(reply, op_status_pos);
        xdr_encode_uint32(reply, status);
        xdr_setpos(reply, saved_pos);

        completed_ops++;

        if (status != NFS4_OK) {
            overall_status = status;
            DFUSE_LOG("  op[%u]=%u failed with status %u", i, opnum, status);
            break;
        }
    }

    /* Backpatch overall status and numresults */
    size_t end_pos = xdr_getpos(reply);

    xdr_setpos(reply, status_pos);
    xdr_encode_uint32(reply, overall_status);

    xdr_setpos(reply, numres_pos);
    xdr_encode_uint32(reply, completed_ops);

    xdr_setpos(reply, end_pos);

    return reply->error ? -1 : 0;
}
