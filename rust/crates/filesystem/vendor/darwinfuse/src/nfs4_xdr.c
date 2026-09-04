/*
 * DarwinFUSE — minimal XDR encoder/decoder
 *
 * Copyright (c) 2026 Marcel Cotta. All rights reserved.
 * Licensed under the MIT License.
 */

#include "nfs4_xdr.h"
#include <string.h>
#include <arpa/inet.h>  /* htonl, ntohl */

/* ---- Helpers ---- */

/* Round up to next multiple of 4 */
static inline size_t xdr_pad(size_t len)
{
    return (len + 3) & ~(size_t)3;
}

static inline int xdr_check_encode(xdr_buf_t *xdr, size_t needed)
{
    if (xdr->error || xdr->pos + needed > xdr->capacity) {
        xdr->error = 1;
        return 0;
    }
    return 1;
}

static inline int xdr_check_decode(xdr_buf_t *xdr, size_t needed)
{
    if (xdr->error || xdr->pos + needed > xdr->capacity) {
        xdr->error = 1;
        return 0;
    }
    return 1;
}

/* ---- Init / Reset ---- */

void xdr_init(xdr_buf_t *xdr, uint8_t *buf, size_t capacity)
{
    xdr->data = buf;
    xdr->capacity = capacity;
    xdr->pos = 0;
    xdr->error = 0;
}

void xdr_reset(xdr_buf_t *xdr)
{
    xdr->pos = 0;
    xdr->error = 0;
}

/* ---- Encode ---- */

void xdr_encode_uint32(xdr_buf_t *xdr, uint32_t val)
{
    if (!xdr_check_encode(xdr, 4)) return;
    uint32_t net = htonl(val);
    memcpy(xdr->data + xdr->pos, &net, 4);
    xdr->pos += 4;
}

void xdr_encode_int32(xdr_buf_t *xdr, int32_t val)
{
    xdr_encode_uint32(xdr, (uint32_t)val);
}

void xdr_encode_uint64(xdr_buf_t *xdr, uint64_t val)
{
    xdr_encode_uint32(xdr, (uint32_t)(val >> 32));
    xdr_encode_uint32(xdr, (uint32_t)(val & 0xFFFFFFFF));
}

void xdr_encode_int64(xdr_buf_t *xdr, int64_t val)
{
    xdr_encode_uint64(xdr, (uint64_t)val);
}

void xdr_encode_opaque(xdr_buf_t *xdr, const void *data, uint32_t len)
{
    size_t padded = xdr_pad(len);
    if (!xdr_check_encode(xdr, 4 + padded)) return;

    xdr_encode_uint32(xdr, len);
    if (len > 0)
        memcpy(xdr->data + xdr->pos, data, len);

    /* Zero padding bytes */
    size_t pad_bytes = padded - len;
    if (pad_bytes > 0)
        memset(xdr->data + xdr->pos + len, 0, pad_bytes);

    xdr->pos += padded;
}

void xdr_encode_opaque_fixed(xdr_buf_t *xdr, const void *data, uint32_t len)
{
    size_t padded = xdr_pad(len);
    if (!xdr_check_encode(xdr, padded)) return;

    if (len > 0)
        memcpy(xdr->data + xdr->pos, data, len);

    size_t pad_bytes = padded - len;
    if (pad_bytes > 0)
        memset(xdr->data + xdr->pos + len, 0, pad_bytes);

    xdr->pos += padded;
}

void xdr_encode_string(xdr_buf_t *xdr, const char *str)
{
    uint32_t len = str ? (uint32_t)strlen(str) : 0;
    xdr_encode_opaque(xdr, str, len);
}

void xdr_encode_bool(xdr_buf_t *xdr, int val)
{
    xdr_encode_uint32(xdr, val ? 1 : 0);
}

/* ---- Decode ---- */

uint32_t xdr_decode_uint32(xdr_buf_t *xdr)
{
    if (!xdr_check_decode(xdr, 4)) return 0;
    uint32_t net;
    memcpy(&net, xdr->data + xdr->pos, 4);
    xdr->pos += 4;
    return ntohl(net);
}

int32_t xdr_decode_int32(xdr_buf_t *xdr)
{
    return (int32_t)xdr_decode_uint32(xdr);
}

uint64_t xdr_decode_uint64(xdr_buf_t *xdr)
{
    uint64_t hi = xdr_decode_uint32(xdr);
    uint64_t lo = xdr_decode_uint32(xdr);
    return (hi << 32) | lo;
}

int64_t xdr_decode_int64(xdr_buf_t *xdr)
{
    return (int64_t)xdr_decode_uint64(xdr);
}

uint32_t xdr_decode_opaque(xdr_buf_t *xdr, void *out, uint32_t maxlen)
{
    uint32_t len = xdr_decode_uint32(xdr);
    if (xdr->error) return 0;

    size_t padded = xdr_pad(len);
    if (!xdr_check_decode(xdr, padded)) return 0;

    uint32_t copy_len = len < maxlen ? len : maxlen;
    if (copy_len > 0 && out)
        memcpy(out, xdr->data + xdr->pos, copy_len);

    xdr->pos += padded;
    return len;
}

void xdr_decode_opaque_fixed(xdr_buf_t *xdr, void *out, uint32_t len)
{
    size_t padded = xdr_pad(len);
    if (!xdr_check_decode(xdr, padded)) return;

    if (len > 0 && out)
        memcpy(out, xdr->data + xdr->pos, len);

    xdr->pos += padded;
}

void xdr_decode_string(xdr_buf_t *xdr, char *out, uint32_t maxlen)
{
    uint32_t len = xdr_decode_uint32(xdr);
    if (xdr->error) return;

    size_t padded = xdr_pad(len);
    if (!xdr_check_decode(xdr, padded)) return;

    uint32_t copy_len = len < (maxlen - 1) ? len : (maxlen - 1);
    if (copy_len > 0 && out)
        memcpy(out, xdr->data + xdr->pos, copy_len);
    if (out)
        out[copy_len] = '\0';

    xdr->pos += padded;
}

int xdr_decode_bool(xdr_buf_t *xdr)
{
    return xdr_decode_uint32(xdr) != 0;
}

void xdr_skip(xdr_buf_t *xdr, size_t nbytes)
{
    if (!xdr_check_decode(xdr, nbytes)) return;
    xdr->pos += nbytes;
}

void xdr_skip_opaque(xdr_buf_t *xdr)
{
    uint32_t len = xdr_decode_uint32(xdr);
    if (xdr->error) return;
    size_t padded = xdr_pad(len);
    xdr_skip(xdr, padded);
}

void xdr_skip_string(xdr_buf_t *xdr)
{
    xdr_skip_opaque(xdr);
}

/* ---- Position ---- */

size_t xdr_getpos(const xdr_buf_t *xdr)
{
    return xdr->pos;
}

void xdr_setpos(xdr_buf_t *xdr, size_t pos)
{
    if (pos <= xdr->capacity)
        xdr->pos = pos;
    else
        xdr->error = 1;
}

size_t xdr_remaining(const xdr_buf_t *xdr)
{
    if (xdr->pos >= xdr->capacity) return 0;
    return xdr->capacity - xdr->pos;
}
