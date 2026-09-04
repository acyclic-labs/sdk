/*
 * DarwinFUSE — ONC RPC message framing (RFC 5531)
 *
 * Copyright (c) 2026 Marcel Cotta. All rights reserved.
 * Licensed under the MIT License.
 */

#include "rpc.h"
#include "darwinfuse_internal.h"
#include <arpa/inet.h>
#include <string.h>

int rpc_parse_call(xdr_buf_t *xdr, rpc_call_header_t *hdr)
{
    memset(hdr, 0, sizeof(*hdr));

    /* xid */
    hdr->xid = xdr_decode_uint32(xdr);

    /* msg_type — must be CALL (0) */
    uint32_t msg_type = xdr_decode_uint32(xdr);
    if (xdr->error || msg_type != RPC_CALL)
        return -1;

    /* rpcvers — must be 2 */
    uint32_t rpcvers = xdr_decode_uint32(xdr);
    if (xdr->error || rpcvers != RPC_MSG_VERSION)
        return -1;

    /* prog, vers, proc */
    hdr->program   = xdr_decode_uint32(xdr);
    hdr->version   = xdr_decode_uint32(xdr);
    hdr->procedure = xdr_decode_uint32(xdr);

    if (xdr->error)
        return -1;

    /* ---- Credentials ---- */
    hdr->cred_flavor = xdr_decode_uint32(xdr);
    uint32_t cred_len = xdr_decode_uint32(xdr);

    if (xdr->error)
        return -1;

    if (hdr->cred_flavor == AUTH_SYS && cred_len >= 20) {
        /* AUTH_SYS body:
         * stamp (uint32), machinename (string), uid, gid, gids[] */
        size_t cred_start = xdr_getpos(xdr);

        xdr_decode_uint32(xdr);  /* stamp — ignore */
        xdr_skip_string(xdr);    /* machinename */

        hdr->cred_uid = (uid_t)xdr_decode_uint32(xdr);
        hdr->cred_gid = (gid_t)xdr_decode_uint32(xdr);

        /* aux gids array */
        hdr->cred_ngroups = xdr_decode_uint32(xdr);
        for (uint32_t i = 0; i < hdr->cred_ngroups && !xdr->error; i++)
            xdr_decode_uint32(xdr);  /* skip each gid */

        /* Ensure we consumed exactly cred_len bytes (padded) */
        size_t consumed = xdr_getpos(xdr) - cred_start;
        if (consumed < cred_len)
            xdr_skip(xdr, cred_len - consumed);
    } else if (hdr->cred_flavor == AUTH_NONE) {
        /* AUTH_NONE: cred body should be empty (len=0) */
        if (cred_len > 0)
            xdr_skip(xdr, cred_len);
    } else {
        /* Unknown auth flavor — skip cred body */
        xdr_skip(xdr, ((cred_len + 3) & ~(size_t)3));
    }

    /* ---- Verifier ---- */
    uint32_t verf_flavor = xdr_decode_uint32(xdr);
    (void)verf_flavor;
    uint32_t verf_len = xdr_decode_uint32(xdr);
    if (verf_len > 0)
        xdr_skip(xdr, ((verf_len + 3) & ~(size_t)3));

    return xdr->error ? -1 : 0;
}

void rpc_encode_reply_accepted(xdr_buf_t *xdr, uint32_t xid)
{
    xdr_encode_uint32(xdr, xid);
    xdr_encode_uint32(xdr, RPC_REPLY);      /* msg_type = REPLY */
    xdr_encode_uint32(xdr, MSG_ACCEPTED);   /* reply_stat */

    /* Verifier: AUTH_NONE, length 0 */
    xdr_encode_uint32(xdr, AUTH_NONE);
    xdr_encode_uint32(xdr, 0);

    /* accept_stat = SUCCESS */
    xdr_encode_uint32(xdr, ACCEPT_SUCCESS);
}

void rpc_encode_reply_error(xdr_buf_t *xdr, uint32_t xid, uint32_t stat)
{
    xdr_encode_uint32(xdr, xid);
    xdr_encode_uint32(xdr, RPC_REPLY);
    xdr_encode_uint32(xdr, MSG_ACCEPTED);

    /* Verifier: AUTH_NONE */
    xdr_encode_uint32(xdr, AUTH_NONE);
    xdr_encode_uint32(xdr, 0);

    /* accept_stat */
    xdr_encode_uint32(xdr, stat);

    /* For PROG_MISMATCH, encode version range */
    if (stat == ACCEPT_PROG_MISMATCH) {
        xdr_encode_uint32(xdr, NFS_V4);  /* low */
        xdr_encode_uint32(xdr, NFS_V4);  /* high */
    }
}

void rpc_encode_reply_denied(xdr_buf_t *xdr, uint32_t xid)
{
    xdr_encode_uint32(xdr, xid);
    xdr_encode_uint32(xdr, RPC_REPLY);
    xdr_encode_uint32(xdr, MSG_DENIED);

    /* RPC_MISMATCH, version range 2..2 */
    xdr_encode_uint32(xdr, 0);  /* RPC_MISMATCH */
    xdr_encode_uint32(xdr, RPC_MSG_VERSION);
    xdr_encode_uint32(xdr, RPC_MSG_VERSION);
}

/* ---- TCP Record Marking ---- */

void rpc_encode_record_mark(uint8_t *buf, uint32_t payload_len, int last_fragment)
{
    uint32_t rm = payload_len & 0x7FFFFFFF;
    if (last_fragment)
        rm |= 0x80000000;
    uint32_t net = htonl(rm);
    memcpy(buf, &net, 4);
}

uint32_t rpc_parse_record_mark(const uint8_t *buf, int *last_fragment)
{
    uint32_t net;
    memcpy(&net, buf, 4);
    uint32_t rm = ntohl(net);
    if (last_fragment)
        *last_fragment = (rm & 0x80000000) ? 1 : 0;
    return rm & 0x7FFFFFFF;
}
