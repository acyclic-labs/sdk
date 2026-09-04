/*
 * DarwinFUSE — internal shared definitions
 *
 * Copyright (c) 2026 Marcel Cotta. All rights reserved.
 * Licensed under the MIT License.
 */

#ifndef DARWINFUSE_INTERNAL_H
#define DARWINFUSE_INTERNAL_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <sys/types.h>
#include <stdio.h>

/* ---------- NFS constants (RFC 7530) ---------- */

#define NFS_PROGRAM         100003
#define NFS_V4              4
#define NFSPROC4_NULL       0
#define NFSPROC4_COMPOUND   1

/* ---------- ONC RPC constants (RFC 5531) ---------- */

#define RPC_MSG_VERSION     2
#define RPC_CALL            0
#define RPC_REPLY           1

/* Reply stat */
#define MSG_ACCEPTED        0
#define MSG_DENIED          1

/* Accept stat */
#define ACCEPT_SUCCESS      0
#define ACCEPT_PROG_UNAVAIL 1
#define ACCEPT_PROG_MISMATCH 2
#define ACCEPT_PROC_UNAVAIL 3
#define ACCEPT_GARBAGE_ARGS 4

/* Auth flavors */
#define AUTH_NONE           0
#define AUTH_SYS            1  /* AUTH_UNIX */

/* ---------- DarwinFUSE filehandle scheme ---------- */

/*
 * Filehandles are 8-byte inode numbers in network byte order.
 * Root inode is always 1; other inodes are allocated dynamically
 * by the inode table (monotonically increasing, never reused).
 */
#define DFUSE_FH_LEN        8

/* ---------- Buffer sizes ---------- */

#define DFUSE_XDR_MAXBUF    (512 * 1024)
#define DFUSE_MAX_CLIENTS   8
#define DFUSE_READ_BUFSIZE  (256 * 1024)
/* ---------- Thread pool ---------- */

#define DFUSE_DEFAULT_THREADS   4
#define DFUSE_MAX_THREADS       16
#define DFUSE_WORK_QUEUE_MAX    64

/* ---------- Logging ---------- */

/*
 * Operational logging is opt-in.  The NFS client issues several protocol
 * operations for one filesystem call, so unconditional stderr writes turn
 * this diagnostic path into the dominant mount cost.
 */
#define DFUSE_LOG(fmt, ...) do { \
    const char *_lp = getenv("DARWINFUSE_LOG"); \
    if (_lp) { FILE *_f = fopen(_lp, "a"); \
        if (_f) { fprintf(_f, "[DarwinFUSE] " fmt "\n", ##__VA_ARGS__); fclose(_f); } } \
} while (0)

#define DFUSE_ERR(fmt, ...) do { \
    fprintf(stderr, "[DarwinFUSE ERROR] " fmt "\n", ##__VA_ARGS__); \
    const char *_lp = getenv("DARWINFUSE_LOG"); \
    if (_lp) { FILE *_f = fopen(_lp, "a"); \
        if (_f) { fprintf(_f, "[DarwinFUSE ERROR] " fmt "\n", ##__VA_ARGS__); fclose(_f); } } \
} while (0)

#endif /* DARWINFUSE_INTERNAL_H */
