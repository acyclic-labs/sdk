/*
 * DarwinFUSE — NFSv4 COMPOUND operation dispatcher and handlers
 *
 * Copyright (c) 2026 Marcel Cotta. All rights reserved.
 * Licensed under the MIT License.
 */

#ifndef DARWINFUSE_NFS4_OPS_H
#define DARWINFUSE_NFS4_OPS_H

#include "nfs4_xdr.h"
#include "nfs4_server.h"
#include "darwinfuse_internal.h"
#include "inode_table.h"
#include <stdint.h>
#include <pthread.h>

/* ---- NFSv4 operation numbers (RFC 7530 §16) ---- */

#define OP_ACCESS               3
#define OP_CLOSE                4
#define OP_COMMIT               5
#define OP_CREATE               6
#define OP_GETATTR              9
#define OP_GETFH                10
#define OP_LINK                 11
#define OP_LOCK                 12
#define OP_LOCKT                13
#define OP_LOCKU                14
#define OP_LOOKUP               15
#define OP_LOOKUPP              16
#define OP_NVERIFY              17
#define OP_OPEN                 18
#define OP_OPENATTR             19
#define OP_OPEN_CONFIRM         20
#define OP_OPEN_DOWNGRADE       21
#define OP_PUTFH                22
#define OP_PUTPUBFH             23
#define OP_PUTROOTFH            24
#define OP_READ                 25
#define OP_READDIR              26
#define OP_READLINK             27
#define OP_REMOVE               28
#define OP_RENAME               29
#define OP_RENEW                30
#define OP_RESTOREFH            31
#define OP_SAVEFH               32
#define OP_SECINFO              33
#define OP_SETATTR              34
#define OP_SETCLIENTID          35
#define OP_SETCLIENTID_CONFIRM  36
#define OP_VERIFY               37
#define OP_WRITE                38
#define OP_RELEASE_LOCKOWNER    39

/* ---- NFSv4 status codes (RFC 7530 §13) ---- */

#define NFS4_OK                 0
#define NFS4ERR_PERM            1
#define NFS4ERR_NOENT           2
#define NFS4ERR_IO              5
#define NFS4ERR_NXIO            6
#define NFS4ERR_ACCESS          13
#define NFS4ERR_EXIST           17
#define NFS4ERR_XDEV            18
#define NFS4ERR_NOTDIR          20
#define NFS4ERR_ISDIR           21
#define NFS4ERR_INVAL           22
#define NFS4ERR_FBIG            27
#define NFS4ERR_NOSPC           28
#define NFS4ERR_ROFS            30
#define NFS4ERR_NAMETOOLONG     63
#define NFS4ERR_NOTEMPTY        66
#define NFS4ERR_STALE           70
#define NFS4ERR_BADHANDLE       10001
#define NFS4ERR_BAD_STATEID     10026
#define NFS4ERR_NOTSUPP         10004
#define NFS4ERR_SERVERFAULT     10006
#define NFS4ERR_BADTYPE         10007
#define NFS4ERR_DELAY           10008
#define NFS4ERR_SAME            10009
#define NFS4ERR_DENIED          10010
#define NFS4ERR_EXPIRED         10011
#define NFS4ERR_LOCKED          10012
#define NFS4ERR_GRACE           10013
#define NFS4ERR_FHEXPIRED       10014
#define NFS4ERR_WRONGSEC        10016
#define NFS4ERR_CLID_INUSE      10017
#define NFS4ERR_MOVED           10019
#define NFS4ERR_RESOURCE        10018
#define NFS4ERR_MINOR_VERS_MISMATCH 10021
#define NFS4ERR_STALE_CLIENTID  10022
#define NFS4ERR_STALE_STATEID   10023
#define NFS4ERR_OLD_STATEID     10024
#define NFS4ERR_BAD_SEQID       10026
#define NFS4ERR_NOT_SAME        10027
#define NFS4ERR_RESTOREFH       10030
#define NFS4ERR_ATTRNOTSUPP     10032
#define NFS4ERR_OPENMODE        10038

/* ---- NFSv4 file types (RFC 7530 §4.2.3) ---- */

#define NF4REG      1
#define NF4DIR      2
#define NF4BLK      3
#define NF4CHR      4
#define NF4LNK      5
#define NF4SOCK     6
#define NF4FIFO     7
#define NF4ATTRDIR  8
#define NF4NAMEDATTR 9

/* ---- NFSv4 attribute bit positions (RFC 7530 §5) ---- */

/* Word 0 (bits 0-31) — mandatory/recommended attributes */
#define FATTR4_SUPPORTED_ATTRS  0
#define FATTR4_TYPE             1
#define FATTR4_FH_EXPIRE_TYPE   2
#define FATTR4_CHANGE           3
#define FATTR4_SIZE             4
#define FATTR4_LINK_SUPPORT     5
#define FATTR4_SYMLINK_SUPPORT  6
#define FATTR4_NAMED_ATTR       7
#define FATTR4_FSID             8
#define FATTR4_UNIQUE_HANDLES   9
#define FATTR4_LEASE_TIME       10
#define FATTR4_RDATTR_ERROR     11
#define FATTR4_FILEHANDLE       19

/* Word 1 (bits 32-63) — recommended attributes */
#define FATTR4_ACL              12  /* bit 12 in linear, word0 bit 12 */
#define FATTR4_ACLSUPPORT      13
#define FATTR4_ARCHIVE          14
#define FATTR4_CANSETTIME       15
#define FATTR4_CASE_INSENSITIVE 16
#define FATTR4_CASE_PRESERVING  17
#define FATTR4_CHOWN_RESTRICTED 18
/* FATTR4_FILEHANDLE is 19 (word 0) */
#define FATTR4_FILEID           20
#define FATTR4_FILES_AVAIL      21
#define FATTR4_FILES_FREE       22
#define FATTR4_FILES_TOTAL      23
/* 24 = fs_locations */
#define FATTR4_HIDDEN           25
#define FATTR4_HOMOGENEOUS      26
#define FATTR4_MAXFILESIZE      27
#define FATTR4_MAXLINK          28
#define FATTR4_MAXNAME          29
#define FATTR4_MAXREAD          30
#define FATTR4_MAXWRITE         31

/* Word 1 continues (bits 32+) */
#define FATTR4_MIMETYPE         32
#define FATTR4_MODE             33
#define FATTR4_NO_TRUNC         34
#define FATTR4_NUMLINKS         35
#define FATTR4_OWNER            36
#define FATTR4_OWNER_GROUP      37
#define FATTR4_QUOTA_AVAIL_HARD 38
#define FATTR4_QUOTA_AVAIL_SOFT 39
#define FATTR4_QUOTA_USED       40
#define FATTR4_RAWDEV           41
#define FATTR4_SPACE_AVAIL      42
#define FATTR4_SPACE_FREE       43
#define FATTR4_SPACE_TOTAL      44
#define FATTR4_SPACE_USED       45
#define FATTR4_SYSTEM           46
#define FATTR4_TIME_ACCESS      47
#define FATTR4_TIME_ACCESS_SET  48
#define FATTR4_TIME_BACKUP      49
#define FATTR4_TIME_CREATE      50
#define FATTR4_TIME_DELTA       51
#define FATTR4_TIME_METADATA    52
#define FATTR4_TIME_MODIFY      53
#define FATTR4_TIME_MODIFY_SET  54
#define FATTR4_MOUNTED_ON_FILEID 55

/* Misc constants */
#define ACCESS4_READ            0x00000001
#define ACCESS4_LOOKUP          0x00000002
#define ACCESS4_MODIFY          0x00000004
#define ACCESS4_EXTEND          0x00000008
#define ACCESS4_DELETE          0x00000010
#define ACCESS4_EXECUTE         0x00000020

/* Open share access */
#define OPEN4_SHARE_ACCESS_READ    0x00000001
#define OPEN4_SHARE_ACCESS_WRITE   0x00000002
#define OPEN4_SHARE_ACCESS_BOTH    0x00000003

/* Open share deny */
#define OPEN4_SHARE_DENY_NONE      0x00000000

/* Open create */
#define OPEN4_NOCREATE             0
#define OPEN4_CREATE               1

/* Open claim types */
#define CLAIM_NULL                 0
#define CLAIM_PREVIOUS             1
#define CLAIM_DELEGATE_CUR         2
#define CLAIM_DELEGATE_PREV        3
#define CLAIM_FH                   4

/* Delegation types */
#define OPEN_DELEGATE_NONE         0
#define OPEN_DELEGATE_READ         1
#define OPEN_DELEGATE_WRITE        2

/* Write stable how */
#define UNSTABLE4                  0
#define DATA_SYNC4                 1
#define FILE_SYNC4                 2

/* FH expire type */
#define FH4_PERSISTENT             0x00000000
#define FH4_VOLATILE_ANY           0x00000001

/* ---- Per-request NFSv4 state (stack-allocated per COMPOUND) ---- */

typedef struct {
    uint8_t  current_fh[128];
    uint32_t current_fh_len;
    uint8_t  saved_fh[128];
    uint32_t saved_fh_len;
} nfs4_request_ctx_t;

/* ---- Per-connection NFSv4 state ---- */

typedef struct {
    uint32_t seqid;
    uint32_t other[3];   /* 12-byte opaque identifier */
} nfs4_stateid_t;

#define OPEN_FILES_INITIAL_CAP  32

/* Open file tracking — stores FUSE file handle from open() callback */
typedef struct {
    nfs4_stateid_t stateid;
    dfuse_ino_t    ino;        /* inode of opened file */
    uint64_t       fuse_fh;   /* fi.fh from FUSE open() callback */
    int            flags;      /* open flags (O_RDONLY, O_RDWR, etc.) */
} nfs4_open_file_t;

typedef struct {
    /* Mutex for shared state (open_files, clientid, etc.) */
    pthread_mutex_t lock;

    /* Client identification */
    uint64_t clientid;
    uint64_t client_verifier;
    uint64_t server_verifier;
    int      confirmed;

    /* Open file tracking (dynamic array) */
    nfs4_open_file_t *open_files;
    int               open_file_count;
    int               open_file_cap;

    /* Sequence counter for open_confirm */
    uint32_t open_seqid;
} nfs4_conn_state_t;

/*
 * Dispatch a COMPOUND request. Decodes the compound header, iterates
 * through operations, and encodes the compound reply.
 *
 * request: XDR buffer positioned at start of COMPOUND body (after RPC header)
 * reply:   XDR buffer to write COMPOUND reply body
 *
 * Returns 0 on success, -1 on fatal encoding error.
 */
int nfs4_dispatch_compound(const darwinfuse_config_t *config,
                           nfs4_conn_state_t *conn,
                           nfs4_request_ctx_t *ctx,
                           xdr_buf_t *request,
                           xdr_buf_t *reply);

#endif /* DARWINFUSE_NFS4_OPS_H */
