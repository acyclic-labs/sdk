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

#include <fuse.h>

#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <errno.h>
#include <arpa/inet.h>

/* xattr calling conventions — macOS adds a position parameter */
#ifdef __APPLE__
#define FUSE_GETXATTR(ops, p, n, b, s)     (ops)->getxattr((p), (n), (b), (s), 0)
#define FUSE_SETXATTR(ops, p, n, v, s, f)  (ops)->setxattr((p), (n), (v), (s), (f), 0)
#else
#define FUSE_GETXATTR(ops, p, n, b, s)     (ops)->getxattr((p), (n), (b), (s))
#define FUSE_SETXATTR(ops, p, n, v, s, f)  (ops)->setxattr((p), (n), (v), (s), (f))
#endif

/* ---- errno to NFS4 status mapping ---- */

static uint32_t errno_to_nfs4(int fuse_rc)
{
    if (fuse_rc == 0) return NFS4_OK;
    int e = (fuse_rc < 0) ? -fuse_rc : fuse_rc;
    switch (e) {
    case EPERM:        return NFS4ERR_PERM;
    case ENOENT:       return NFS4ERR_NOENT;
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
    /* Backward compat: accept 4-byte FH from old clients */
    if (fh_len == 4) {
        uint32_t net;
        memcpy(&net, fh, 4);
        return (dfuse_ino_t)ntohl(net);
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
        /* Use mtime as change attribute */
        uint64_t change = (uint64_t)st->st_mtime * 1000000000ULL;
#ifdef __APPLE__
        change += (uint64_t)st->st_mtimespec.tv_nsec;
#endif
        xdr_encode_uint64(&attr, change);
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
        xdr_encode_uint64(&attr, 65536);
    }

    if (ATTR_SET(FATTR4_MAXWRITE)) {
        xdr_encode_uint64(&attr, 65536);
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
    char *path = dfuse_itable_path_dup(config->inode_table, ino);
    if (!path) return NFS4ERR_STALE;
    free(path);

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

    char child_path[1024];
    build_child_path(child_path, sizeof(child_path), dir_path, name);

    DFUSE_LOG("  LOOKUP dir='%s' name='%s' -> '%s'", dir_path, name, child_path);

    free(dir_path);

    struct stat child_st;
    memset(&child_st, 0, sizeof(child_st));
    if (config->ops->getattr) {
        int rc = config->ops->getattr(child_path, &child_st);
        if (rc != 0) {
            DFUSE_LOG("  LOOKUP '%s' -> error %d", child_path, rc);
            return errno_to_nfs4(rc);
        }
    } else {
        return NFS4ERR_IO;
    }

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

    dfuse_ino_t ino = fh_get_ino(ctx->current_fh, ctx->current_fh_len);
    if (ino == 0) return NFS4ERR_BADHANDLE;

    dfuse_ino_type_t ino_type = dfuse_itable_type(config->inode_table, ino);

    struct stat st;
    memset(&st, 0, sizeof(st));
    uint32_t type_override = 0;

    if (ino_type == DFUSE_INO_ATTRDIR) {
        /* Named attribute directory — synthetic stat */
        st.st_mode = S_IFDIR | 0755;
        st.st_nlink = 2;
        st.st_uid = config->uid;
        st.st_gid = config->gid;
        type_override = NF4ATTRDIR;
    } else if (ino_type == DFUSE_INO_NAMEDATTR) {
        /* Named attribute — get size via getxattr */
        char *file_path = xattr_file_path(config, ino);
        char *attr_name = dfuse_itable_attr_name_dup(config->inode_table, ino);
        if (!file_path || !attr_name) {
            free(file_path);
            free(attr_name);
            return NFS4ERR_STALE;
        }

        st.st_mode = S_IFREG | 0644;
        st.st_nlink = 1;
        st.st_uid = config->uid;
        st.st_gid = config->gid;

        if (config->ops->getxattr) {
            int sz = FUSE_GETXATTR(config->ops, file_path, attr_name, NULL, 0);
            if (sz >= 0)
                st.st_size = sz;
        }
        free(file_path);
        free(attr_name);
        type_override = NF4NAMEDATTR;
    } else {
        /* Regular file/directory */
        char *path = dfuse_itable_path_dup(config->inode_table, ino);
        if (!path) return NFS4ERR_STALE;

        /* Try fgetattr if the file is open */
        int rc = -1;
        if (config->ops->fgetattr) {
            uint64_t local_fh = 0;
            int local_flags = 0;
            int found = 0;

            pthread_mutex_lock(&conn->lock);
            nfs4_open_file_t *of = find_open_file_by_ino(conn, ino);
            if (of) {
                local_fh = of->fuse_fh;
                local_flags = of->flags;
                found = 1;
            }
            pthread_mutex_unlock(&conn->lock);

            if (found) {
                struct fuse_file_info fi;
                memset(&fi, 0, sizeof(fi));
                fi.fh = local_fh;
                fi.flags = local_flags;
                rc = config->ops->fgetattr(path, &st, &fi);
            }
        }
        /* Fallback to getattr */
        if (rc != 0) {
            if (config->ops->getattr) {
                rc = config->ops->getattr(path, &st);
                if (rc != 0) { free(path); return errno_to_nfs4(rc); }
            } else {
                free(path);
                return NFS4ERR_IO;
            }
        }
        free(path);
    }

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
    /* Decode stateid (ignore) */
    xdr_decode_uint32(req);  /* seqid */
    xdr_skip(req, 12);       /* other */

    /* Decode attribute bitmap + data */
    uint32_t bitmap[2] = {0, 0};
    int nwords = 0;
    decode_bitmap(req, bitmap, &nwords);

    /* Decode attr data as opaque */
    uint8_t attr_data[4096];
    uint32_t attr_data_len = xdr_decode_opaque(req, attr_data, sizeof(attr_data));
    if (req->error) return NFS4ERR_INVAL;

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
        if (config->ops->fsetattr_x) {
            dfuse_ino_t ino = fh_get_ino(ctx->current_fh, ctx->current_fh_len);
            uint64_t local_fh = 0;
            int local_flags = 0;
            int found = 0;

            pthread_mutex_lock(&conn->lock);
            nfs4_open_file_t *of = find_open_file_by_ino(conn, ino);
            if (of) {
                local_fh = of->fuse_fh;
                local_flags = of->flags;
                found = 1;
            }
            pthread_mutex_unlock(&conn->lock);

            if (found) {
                struct fuse_file_info fi;
                memset(&fi, 0, sizeof(fi));
                fi.fh = local_fh;
                fi.flags = local_flags;
                rc = config->ops->fsetattr_x(path, &sa, &fi);
            }
        }

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
        int rc = -1;
        /* Try ftruncate if the file is open */
        if (config->ops->ftruncate) {
            dfuse_ino_t ino = fh_get_ino(ctx->current_fh, ctx->current_fh_len);
            uint64_t local_fh = 0;
            int local_flags = 0;
            int found = 0;

            pthread_mutex_lock(&conn->lock);
            nfs4_open_file_t *of = find_open_file_by_ino(conn, ino);
            if (of) {
                local_fh = of->fuse_fh;
                local_flags = of->flags;
                found = 1;
            }
            pthread_mutex_unlock(&conn->lock);

            if (found) {
                struct fuse_file_info fi;
                memset(&fi, 0, sizeof(fi));
                fi.fh = local_fh;
                fi.flags = local_flags;
                rc = config->ops->ftruncate(path, (off_t)set_size, &fi);
            }
        }
        /* Fallback to truncate */
        if (rc != 0 && config->ops->truncate) {
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

static uint32_t handle_access(const darwinfuse_config_t *config,
                               nfs4_conn_state_t *conn,
                               nfs4_request_ctx_t *ctx,
                               xdr_buf_t *req, xdr_buf_t *rep)
{
    uint32_t requested = xdr_decode_uint32(req);
    if (req->error) return NFS4ERR_INVAL;

    char *path = fh_to_path(config, ctx->current_fh, ctx->current_fh_len);
    if (!path) return NFS4ERR_STALE;

    uint32_t granted = requested;  /* default: grant everything */

    if (config->ops->access) {
        int mask = 0;
        if (requested & ACCESS4_READ)    mask |= R_OK;
        if (requested & ACCESS4_MODIFY)  mask |= W_OK;
        if (requested & ACCESS4_EXECUTE) mask |= X_OK;

        int rc = config->ops->access(path, mask);
        if (rc != 0)
            granted = 0;
    }

    free(path);

    /* Encode: supported, access */
    xdr_encode_uint32(rep, requested);  /* supported */
    xdr_encode_uint32(rep, granted);    /* access */
    return NFS4_OK;
}

/* ---- READDIR ---- */

typedef struct {
    char     name[256];
    uint64_t cookie;
} readdir_entry_t;

typedef struct {
    readdir_entry_t *entries;
    int count;
    int cap;
} readdir_collector_t;

static int readdir_filler(void *buf, const char *name,
                           const struct stat *stbuf, off_t off)
{
    (void)stbuf; (void)off;
    readdir_collector_t *col = (readdir_collector_t *)buf;

    /* Grow if needed */
    if (col->count >= col->cap) {
        int new_cap = col->cap ? col->cap * 2 : 128;
        readdir_entry_t *new_arr = realloc(col->entries,
                                            (size_t)new_cap * sizeof(*new_arr));
        if (!new_arr) return 1;  /* stop filling on OOM */
        col->entries = new_arr;
        col->cap = new_cap;
    }

    readdir_entry_t *e = &col->entries[col->count];
    strncpy(e->name, name, sizeof(e->name) - 1);
    e->name[sizeof(e->name) - 1] = '\0';
    e->cookie = (uint64_t)(col->count + 1);
    col->count++;
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

    (void)dircount;
    (void)maxcount;

    /* Encode cookieverf */
    uint8_t reply_verf[8] = {0};
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

        char list_buf[8192];
        int list_size = config->ops->listxattr(file_path, list_buf,
                                                sizeof(list_buf));
        if (list_size <= 0) {
            free(file_path);
            xdr_encode_bool(rep, 0);
            xdr_encode_bool(rep, 1);
            return NFS4_OK;
        }

        /* Parse null-terminated list and emit entries */
        const char *p = list_buf;
        uint64_t entry_cookie = 0;
        while (p < list_buf + list_size && *p != '\0') {
            entry_cookie++;
            size_t name_len = strlen(p);

            if (entry_cookie > cookie) {
                /* Get or create namedattr inode */
                dfuse_ino_t na_ino = dfuse_itable_get_namedattr(
                    config->inode_table, dir_ino, p);

                /* value_follows = TRUE */
                xdr_encode_bool(rep, 1);
                xdr_encode_uint64(rep, entry_cookie);
                xdr_encode_string(rep, p);

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

                encode_fattr4(rep, &na_st, attr_bitmap, attr_nwords,
                              efh, efh_len, config, NF4NAMEDATTR);
            }

            p += name_len + 1;
        }

        free(file_path);
        xdr_encode_bool(rep, 0);  /* no more entries */
        xdr_encode_bool(rep, 1);  /* eof */
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

    struct fuse_file_info dir_fi;
    memset(&dir_fi, 0, sizeof(dir_fi));

    if (config->ops->opendir)
        config->ops->opendir(dir_path, &dir_fi);

    if (config->ops->readdir)
        config->ops->readdir(dir_path, &collector, readdir_filler, 0, &dir_fi);

    if (config->ops->releasedir)
        config->ops->releasedir(dir_path, &dir_fi);

    for (int i = 0; i < collector.count; i++) {
        readdir_entry_t *e = &collector.entries[i];
        if (e->cookie <= cookie) continue;

        if (strcmp(e->name, ".") == 0 || strcmp(e->name, "..") == 0)
            continue;

        xdr_encode_bool(rep, 1);
        xdr_encode_uint64(rep, e->cookie);
        xdr_encode_string(rep, e->name);

        char child_path[1024];
        build_child_path(child_path, sizeof(child_path), dir_path, e->name);

        struct stat st;
        memset(&st, 0, sizeof(st));
        if (config->ops->getattr)
            config->ops->getattr(child_path, &st);

        dfuse_ino_t entry_ino = dfuse_itable_get_or_create(config->inode_table,
                                                            child_path);
        uint8_t efh[8];
        uint32_t efh_len = 0;
        if (entry_ino)
            fh_set_ino(efh, &efh_len, entry_ino);

        encode_fattr4(rep, &st, attr_bitmap, attr_nwords, efh, efh_len,
                      config, 0);
    }

    free(collector.entries);
    free(dir_path);

    xdr_encode_bool(rep, 0);
    xdr_encode_bool(rep, 1);

    return NFS4_OK;
}

/* ---- OPEN ---- */

static uint32_t handle_open(const darwinfuse_config_t *config,
                             nfs4_conn_state_t *conn,
                             nfs4_request_ctx_t *ctx,
                             xdr_buf_t *req, xdr_buf_t *rep)
{
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

    if (opentype == OPEN4_CREATE) {
        uint32_t createmode = xdr_decode_uint32(req);
        (void)createmode;
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

            /* Create the xattr if OPEN4_CREATE */
            if (opentype == OPEN4_CREATE && config->ops->setxattr) {
                FUSE_SETXATTR(config->ops, file_path, filename, "", 0, 0);
            }

            /* Verify xattr exists */
            if (config->ops->getxattr) {
                int sz = FUSE_GETXATTR(config->ops, file_path, filename,
                                       NULL, 0);
                if (sz < 0 && opentype != OPEN4_CREATE) {
                    free(file_path);
                    return errno_to_nfs4(sz);
                }
            }

            free(file_path);

            target_ino = dfuse_itable_get_namedattr(
                config->inode_table, cur_ino, filename);
            if (target_ino == 0) return NFS4ERR_SERVERFAULT;

            /* Generate stateid for namedattr */
            nfs4_stateid_t sid;

            pthread_mutex_lock(&conn->lock);
            conn->open_seqid++;
            sid.seqid = conn->open_seqid;
            arc4random_buf(sid.other, sizeof(sid.other));

            if (ensure_open_file_capacity(conn) == 0) {
                nfs4_open_file_t *of =
                    &conn->open_files[conn->open_file_count++];
                of->stateid = sid;
                of->ino = target_ino;
                of->fuse_fh = 0;  /* no FUSE file handle for xattrs */
                of->flags = 0;
            }
            pthread_mutex_unlock(&conn->lock);

            fh_set_ino(ctx->current_fh, &ctx->current_fh_len, target_ino);

            /* Encode OPEN4resok */
            xdr_encode_uint32(rep, sid.seqid);
            xdr_encode_opaque_fixed(rep, sid.other, 12);
            xdr_encode_bool(rep, 1);
            xdr_encode_uint64(rep, 0);
            xdr_encode_uint64(rep, 1);
            xdr_encode_uint32(rep, 0x00000004);
            xdr_encode_uint32(rep, 0);
            xdr_encode_uint32(rep, OPEN_DELEGATE_NONE);
            return NFS4_OK;
        }

        /* Get parent directory path */
        char *dir_path = fh_to_path(config, ctx->current_fh, ctx->current_fh_len);
        if (!dir_path) return NFS4ERR_STALE;

        /* Build target path */
        char path_buf[1024];
        build_child_path(path_buf, sizeof(path_buf), dir_path, filename);
        free(dir_path);

        if (opentype == OPEN4_CREATE) {
            /* Try to create the file first */
            struct fuse_file_info fi;
            memset(&fi, 0, sizeof(fi));
            fi.flags = O_CREAT | O_WRONLY;
            if (share_access & OPEN4_SHARE_ACCESS_READ)
                fi.flags = O_CREAT | O_RDWR;

            if (config->ops->create) {
                int rc = config->ops->create(path_buf, create_mode, &fi);
                if (rc == 0) {
                    /* Store the fuse_fh */
                    target_ino = dfuse_itable_get_or_create(config->inode_table, path_buf);
                    if (target_ino == 0) return NFS4ERR_SERVERFAULT;
                    target_path = dfuse_itable_path_dup(config->inode_table, target_ino);

                    /* Generate a stateid */
                    nfs4_stateid_t sid;

                    pthread_mutex_lock(&conn->lock);
                    conn->open_seqid++;
                    sid.seqid = conn->open_seqid;
                    arc4random_buf(sid.other, sizeof(sid.other));

                    /* Store open file */
                    if (ensure_open_file_capacity(conn) == 0) {
                        nfs4_open_file_t *of = &conn->open_files[conn->open_file_count++];
                        of->stateid = sid;
                        of->ino = target_ino;
                        of->fuse_fh = fi.fh;
                        of->flags = fi.flags;
                    }
                    pthread_mutex_unlock(&conn->lock);

                    /* Set current FH */
                    fh_set_ino(ctx->current_fh, &ctx->current_fh_len, target_ino);

                    /* Encode OPEN4resok */
                    xdr_encode_uint32(rep, sid.seqid);
                    xdr_encode_opaque_fixed(rep, sid.other, 12);
                    xdr_encode_bool(rep, 1);      /* atomic */
                    xdr_encode_uint64(rep, 0);    /* before */
                    xdr_encode_uint64(rep, 1);    /* after */
                    xdr_encode_uint32(rep, 0x00000004);  /* OPEN4_RESULT_CONFIRM */
                    xdr_encode_uint32(rep, 0);    /* attrset bitmap (empty) */
                    xdr_encode_uint32(rep, OPEN_DELEGATE_NONE);
                    free(target_path);
                    return NFS4_OK;
                }
                /* create failed — fall through to try open if EEXIST */
                if (rc != -EEXIST)
                    return errno_to_nfs4(rc);
            } else if (config->ops->mknod) {
                /* Fallback: use mknod + open */
                int rc = config->ops->mknod(path_buf, S_IFREG | create_mode, 0);
                if (rc != 0 && rc != -EEXIST)
                    return errno_to_nfs4(rc);
            }
            /* File exists or was just created, fall through to open it */
        }

        /* Verify file exists */
        struct stat st;
        memset(&st, 0, sizeof(st));
        if (config->ops->getattr) {
            int rc = config->ops->getattr(path_buf, &st);
            if (rc != 0) return errno_to_nfs4(rc);
        }

        target_ino = dfuse_itable_get_or_create(config->inode_table, path_buf);
        if (target_ino == 0) return NFS4ERR_SERVERFAULT;
        target_path = dfuse_itable_path_dup(config->inode_table, target_ino);

    } else if (claim_type == CLAIM_FH) {
        /* Open current filehandle */
        target_ino = fh_get_ino(ctx->current_fh, ctx->current_fh_len);
        if (target_ino == 0) return NFS4ERR_BADHANDLE;
        target_path = dfuse_itable_path_dup(config->inode_table, target_ino);
        if (!target_path) return NFS4ERR_STALE;
    } else {
        return NFS4ERR_NOTSUPP;
    }

    if (req->error) { free(target_path); return NFS4ERR_INVAL; }

    /* Don't open directories */
    struct stat tst;
    memset(&tst, 0, sizeof(tst));
    if (config->ops->getattr) {
        config->ops->getattr(target_path, &tst);
        if (S_ISDIR(tst.st_mode)) {
            free(target_path);
            return NFS4ERR_ISDIR;
        }
    }

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

    /* Generate a stateid */
    nfs4_stateid_t sid;

    pthread_mutex_lock(&conn->lock);
    conn->open_seqid++;
    sid.seqid = conn->open_seqid;
    arc4random_buf(sid.other, sizeof(sid.other));

    /* Store open file tracking */
    if (ensure_open_file_capacity(conn) == 0) {
        nfs4_open_file_t *of = &conn->open_files[conn->open_file_count++];
        of->stateid = sid;
        of->ino = target_ino;
        of->fuse_fh = fi.fh;
        of->flags = fi.flags;
    }
    pthread_mutex_unlock(&conn->lock);

    /* Set current FH to opened file */
    fh_set_ino(ctx->current_fh, &ctx->current_fh_len, target_ino);

    /* Encode OPEN4resok */
    /* stateid4 */
    xdr_encode_uint32(rep, sid.seqid);
    xdr_encode_opaque_fixed(rep, sid.other, 12);

    /* cinfo: change_info4 { atomic=true, before=0, after=1 } */
    xdr_encode_bool(rep, 1);      /* atomic */
    xdr_encode_uint64(rep, 0);    /* before */
    xdr_encode_uint64(rep, 1);    /* after */

    /* rflags: OPEN4_RESULT_CONFIRM (need open_confirm for v4.0) */
    xdr_encode_uint32(rep, 0x00000004);

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
    uint64_t local_fh = 0;
    int local_flags = 0;
    dfuse_ino_t local_ino = 0;
    int found = 0;

    pthread_mutex_lock(&conn->lock);
    for (int i = 0; i < conn->open_file_count; i++) {
        if (memcmp(conn->open_files[i].stateid.other, sid_other, 12) == 0) {
            nfs4_open_file_t *of = &conn->open_files[i];
            local_fh = of->fuse_fh;
            local_flags = of->flags;
            local_ino = of->ino;
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
        char *path = dfuse_itable_path_dup(config->inode_table, local_ino);
        if (path) {
            struct fuse_file_info fi;
            memset(&fi, 0, sizeof(fi));
            fi.fh = local_fh;
            fi.flags = local_flags;

            if (config->ops->flush)
                config->ops->flush(path, &fi);
            if (config->ops->release)
                config->ops->release(path, &fi);
            free(path);
        }

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

    if (count > 65536) count = 65536;
    uint8_t *buf = malloc(count);
    if (!buf) return NFS4ERR_SERVERFAULT;

    struct fuse_file_info fi;
    memset(&fi, 0, sizeof(fi));

    pthread_mutex_lock(&conn->lock);
    nfs4_open_file_t *of = find_open_file(conn, sid_other);
    if (of) fi.fh = of->fuse_fh;
    pthread_mutex_unlock(&conn->lock);

    int n = config->ops->read(path, (char *)buf, count, (off_t)offset, &fi);
    if (n < 0) {
        DFUSE_LOG("  READ '%s' offset=%llu count=%u -> error %d",
                  path, (unsigned long long)offset, count, n);
        free(path);
        free(buf);
        return errno_to_nfs4(n);
    }

    struct stat st;
    memset(&st, 0, sizeof(st));
    int eof = 0;
    if (config->ops->getattr) {
        config->ops->getattr(path, &st);
        eof = ((uint64_t)offset + (uint64_t)n >= (uint64_t)st.st_size) ? 1 : 0;
    }

    xdr_encode_bool(rep, eof);
    xdr_encode_opaque(rep, buf, (uint32_t)n);

    free(path);
    free(buf);
    return NFS4_OK;
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
    (void)stable;

    /* data (opaque) */
    uint32_t data_len_raw = xdr_decode_uint32(req);
    if (req->error || data_len_raw > DFUSE_XDR_MAXBUF)
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
        free(file_path);
        free(attr_name);

        if (rc != 0)
            return errno_to_nfs4(rc);

        xdr_encode_uint32(rep, data_len_raw);
        xdr_encode_uint32(rep, FILE_SYNC4);
        uint8_t writeverf[8] = {'D','F','U','S','E','v','0','2'};
        xdr_encode_opaque_fixed(rep, writeverf, 8);
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
    memset(&fi, 0, sizeof(fi));

    pthread_mutex_lock(&conn->lock);
    nfs4_open_file_t *of = find_open_file(conn, sid_other);
    if (of) fi.fh = of->fuse_fh;
    pthread_mutex_unlock(&conn->lock);

    int n = config->ops->write(path, (const char *)data, data_len_raw,
                                (off_t)offset, &fi);
    if (n < 0) {
        free(path);
        return errno_to_nfs4(n);
    }

    free(path);

    xdr_encode_uint32(rep, (uint32_t)n);
    xdr_encode_uint32(rep, FILE_SYNC4);

    uint8_t writeverf[8] = {'D','F','U','S','E','v','0','2'};
    xdr_encode_opaque_fixed(rep, writeverf, 8);

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

    /* Call fsync if available */
    char *path = fh_to_path(config, ctx->current_fh, ctx->current_fh_len);
    if (path && config->ops->fsync) {
        struct fuse_file_info fi;
        memset(&fi, 0, sizeof(fi));
        config->ops->fsync(path, 0, &fi);
    }
    free(path);

    /* Reply: writeverf */
    uint8_t writeverf[8] = {'D','F','U','S','E','v','0','2'};
    xdr_encode_opaque_fixed(rep, writeverf, 8);

    return NFS4_OK;
}

/* ---- CREATE (non-regular files: mkdir, symlink, mknod) ---- */

static uint32_t handle_create(const darwinfuse_config_t *config,
                               nfs4_conn_state_t *conn,
                               nfs4_request_ctx_t *ctx,
                               xdr_buf_t *req, xdr_buf_t *rep)
{
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

    if (rc != 0) return errno_to_nfs4(rc);

    /* Assign inode and set as current FH */
    dfuse_ino_t child_ino = dfuse_itable_get_or_create(config->inode_table, child_path);
    if (child_ino == 0) return NFS4ERR_SERVERFAULT;
    fh_set_ino(ctx->current_fh, &ctx->current_fh_len, child_ino);

    /* Encode: cinfo { atomic=true, before=0, after=1 } */
    xdr_encode_bool(rep, 1);
    xdr_encode_uint64(rep, 0);
    xdr_encode_uint64(rep, 1);

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

        int rc = config->ops->removexattr(file_path, name);
        free(file_path);
        if (rc != 0) return errno_to_nfs4(rc);

        /* Encode: cinfo */
        xdr_encode_bool(rep, 1);
        xdr_encode_uint64(rep, 0);
        xdr_encode_uint64(rep, 1);
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

    xdr_encode_bool(rep, 1);
    xdr_encode_uint64(rep, 0);
    xdr_encode_uint64(rep, 1);

    return NFS4_OK;
}

/* ---- RENAME ---- */

static uint32_t handle_rename(const darwinfuse_config_t *config,
                               nfs4_conn_state_t *conn,
                               nfs4_request_ctx_t *ctx,
                               xdr_buf_t *req, xdr_buf_t *rep)
{
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
    xdr_encode_bool(rep, 1);      /* atomic */
    xdr_encode_uint64(rep, 0);
    xdr_encode_uint64(rep, 1);
    xdr_encode_bool(rep, 1);      /* atomic */
    xdr_encode_uint64(rep, 0);
    xdr_encode_uint64(rep, 1);

    return NFS4_OK;
}

/* ---- LINK ---- */

static uint32_t handle_link(const darwinfuse_config_t *config,
                             nfs4_conn_state_t *conn,
                             nfs4_request_ctx_t *ctx,
                             xdr_buf_t *req, xdr_buf_t *rep)
{
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

    /* Encode: cinfo { atomic=true, before=0, after=1 } */
    xdr_encode_bool(rep, 1);
    xdr_encode_uint64(rep, 0);
    xdr_encode_uint64(rep, 1);

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
 * LOCK — grant all byte-range locks immediately (single-user server).
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

static uint32_t handle_verify(const darwinfuse_config_t *config,
                               nfs4_conn_state_t *conn,
                               nfs4_request_ctx_t *ctx,
                               xdr_buf_t *req, xdr_buf_t *rep)
{
    (void)config; (void)conn; (void)rep;
    uint32_t bm[2] = {0};
    int nw = 0;
    decode_bitmap(req, bm, &nw);
    xdr_skip_opaque(req);
    return NFS4_OK;
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
        case OP_NVERIFY:
            status = handle_verify(config, conn, ctx, request, reply);
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
