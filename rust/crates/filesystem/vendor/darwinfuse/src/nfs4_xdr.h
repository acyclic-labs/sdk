/*
 * DarwinFUSE — minimal XDR encoder/decoder
 *
 * Hand-written for the small subset of NFSv4 types we need.
 * All integers are big-endian, all data is 4-byte aligned per RFC 4506.
 *
 * Copyright (c) 2026 Marcel Cotta. All rights reserved.
 * Licensed under the MIT License.
 */

#ifndef DARWINFUSE_NFS4_XDR_H
#define DARWINFUSE_NFS4_XDR_H

#include <stdint.h>
#include <stddef.h>

/* Cursor-based XDR buffer — no dynamic allocation during encode/decode */
typedef struct {
    uint8_t *data;       /* buffer start */
    size_t   capacity;   /* total capacity in bytes */
    size_t   pos;        /* current read/write offset */
    int      error;      /* 1 if overflow/underflow occurred */
} xdr_buf_t;

/* Initialize / reset */
void xdr_init(xdr_buf_t *xdr, uint8_t *buf, size_t capacity);
void xdr_reset(xdr_buf_t *xdr);

/* ---- Encode primitives (big-endian, 4-byte aligned) ---- */

void     xdr_encode_uint32(xdr_buf_t *xdr, uint32_t val);
void     xdr_encode_int32(xdr_buf_t *xdr, int32_t val);
void     xdr_encode_uint64(xdr_buf_t *xdr, uint64_t val);
void     xdr_encode_int64(xdr_buf_t *xdr, int64_t val);
void     xdr_encode_opaque(xdr_buf_t *xdr, const void *data, uint32_t len);
void     xdr_encode_opaque_fixed(xdr_buf_t *xdr, const void *data, uint32_t len);
void     xdr_encode_string(xdr_buf_t *xdr, const char *str);
void     xdr_encode_bool(xdr_buf_t *xdr, int val);

/* ---- Decode primitives ---- */

uint32_t xdr_decode_uint32(xdr_buf_t *xdr);
int32_t  xdr_decode_int32(xdr_buf_t *xdr);
uint64_t xdr_decode_uint64(xdr_buf_t *xdr);
int64_t  xdr_decode_int64(xdr_buf_t *xdr);

/* Decode variable-length opaque; copies up to maxlen bytes into out.
 * Returns actual decoded length (may exceed maxlen — data is skipped). */
uint32_t xdr_decode_opaque(xdr_buf_t *xdr, void *out, uint32_t maxlen);

/* Decode fixed-length opaque (no length prefix in stream) */
void     xdr_decode_opaque_fixed(xdr_buf_t *xdr, void *out, uint32_t len);

/* Decode string into out (null-terminated); maxlen includes NUL. */
void     xdr_decode_string(xdr_buf_t *xdr, char *out, uint32_t maxlen);

int      xdr_decode_bool(xdr_buf_t *xdr);

/* Skip nbytes of raw data (no alignment adjustment) */
void     xdr_skip(xdr_buf_t *xdr, size_t nbytes);

/* Skip a variable-length opaque (decode length, skip data+padding) */
void     xdr_skip_opaque(xdr_buf_t *xdr);

/* Skip a string (same wire format as opaque) */
void     xdr_skip_string(xdr_buf_t *xdr);

/* ---- Position management (for backpatching) ---- */

size_t   xdr_getpos(const xdr_buf_t *xdr);
void     xdr_setpos(xdr_buf_t *xdr, size_t pos);
size_t   xdr_remaining(const xdr_buf_t *xdr);

#endif /* DARWINFUSE_NFS4_XDR_H */
