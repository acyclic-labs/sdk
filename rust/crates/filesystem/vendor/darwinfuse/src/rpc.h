/*
 * DarwinFUSE — ONC RPC message framing (RFC 5531)
 *
 * Copyright (c) 2026 Marcel Cotta. All rights reserved.
 * Licensed under the MIT License.
 */

#ifndef DARWINFUSE_RPC_H
#define DARWINFUSE_RPC_H

#include "nfs4_xdr.h"
#include <stdint.h>
#include <sys/types.h>

/* Parsed ONC RPC call header */
typedef struct {
    uint32_t xid;
    uint32_t program;
    uint32_t version;
    uint32_t procedure;
    /* Credentials extracted from AUTH_SYS */
    uint32_t cred_flavor;
    uid_t    cred_uid;
    gid_t    cred_gid;
    /* Additional AUTH_SYS groups (we store count but skip them) */
    uint32_t cred_ngroups;
} rpc_call_header_t;

/*
 * Parse an ONC RPC call from xdr buffer.
 * Advances xdr->pos past the complete call header (including auth).
 * Returns 0 on success, -1 on error.
 */
int rpc_parse_call(xdr_buf_t *xdr, rpc_call_header_t *hdr);

/*
 * Encode an ONC RPC accepted reply header (MSG_ACCEPTED, SUCCESS).
 * Caller should encode the reply body after this.
 */
void rpc_encode_reply_accepted(xdr_buf_t *xdr, uint32_t xid);

/*
 * Encode an ONC RPC accept_stat error reply.
 * stat: ACCEPT_PROG_UNAVAIL, ACCEPT_PROG_MISMATCH, etc.
 * For PROG_MISMATCH, also encodes lo=4, hi=4 (only NFSv4).
 */
void rpc_encode_reply_error(xdr_buf_t *xdr, uint32_t xid, uint32_t stat);

/*
 * Encode a MSG_DENIED reply (e.g., RPC version mismatch).
 */
void rpc_encode_reply_denied(xdr_buf_t *xdr, uint32_t xid);

/* ---- TCP Record Marking (RFC 5531 §11) ---- */

/*
 * Write the 4-byte record-marking header.
 * Sets bit 31 for last_fragment, bits 0..30 for payload_len.
 * buf must have room for at least 4 bytes.
 */
void rpc_encode_record_mark(uint8_t *buf, uint32_t payload_len, int last_fragment);

/*
 * Parse a 4-byte record-marking header.
 * Returns the payload length; sets *last_fragment.
 */
uint32_t rpc_parse_record_mark(const uint8_t *buf, int *last_fragment);

#endif /* DARWINFUSE_RPC_H */
