#include <stdint.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>

#define __STDC_VERSION__ 202311L
extern "C" {
#include "acyclic_embedded_prototype.h"
}
#undef __STDC_VERSION__

static_assert(sizeof(AcyclicBuffer) == 32, "embedded ABI layout mismatch: AcyclicBuffer");
static_assert(offsetof(AcyclicBuffer, id) == 0, "embedded ABI layout mismatch: buffer id");
static_assert(offsetof(AcyclicBuffer, ptr) == 8, "embedded ABI layout mismatch: buffer pointer");
static_assert(offsetof(AcyclicBuffer, len) == 16, "embedded ABI layout mismatch: buffer length");
static_assert(offsetof(AcyclicBuffer, capacity) == 24, "embedded ABI layout mismatch: buffer capacity");
static_assert(sizeof(AcyclicAppendResult) == 64, "embedded ABI layout mismatch: append result");
static_assert(sizeof(AcyclicOpenResult) == 48, "embedded ABI layout mismatch: open result");
static_assert(sizeof(AcyclicNextResult) == 80, "embedded ABI layout mismatch: next result");
static_assert(sizeof(AcyclicWireResult) == 72, "embedded ABI layout mismatch: wire result");
static_assert(offsetof(AcyclicWireResult, response) == 8, "embedded ABI layout mismatch: wire response");
static_assert(offsetof(AcyclicWireResult, message) == 40, "embedded ABI layout mismatch: wire message");

namespace {

void require(bool condition, const char* message) {
  if (!condition) {
    fputs(message, stderr);
    fputc('\n', stderr);
    exit(EXIT_FAILURE);
  }
}

const uint8_t kPath[] = "cpp/lifetime";
const uint8_t kValue[] = "value";

}  // namespace

int main() {
  const uint64_t engine = acyclic_embedded_engine_open();
  require(engine != 0, "engine open failed");
  const AcyclicAppendResult append = acyclic_embedded_engine_append(
      engine, kPath, sizeof(kPath) - 1, kValue, sizeof(kValue) - 1);
  require(append.status == Ok, "append failed");
  acyclic_append_result_release(append);

  const AcyclicOpenResult opened = acyclic_embedded_reader_open(
      engine, kPath, sizeof(kPath) - 1, 0, 1, 0);
  require(opened.status == Ok, "reader open failed");
  const uint64_t reader = opened.reader;

  // Closing the engine must not invalidate an owned reader reference.
  acyclic_embedded_engine_close(engine);
  const AcyclicNextResult next = acyclic_embedded_reader_next(reader);
  require(next.status == Ok, "reader did not retain its engine ownership");
  acyclic_next_result_release(next);

  // Reader close is idempotent, and stale reader operations are rejected.
  acyclic_embedded_reader_close(reader);
  acyclic_embedded_reader_close(reader);
  const AcyclicNextResult stale_reader = acyclic_embedded_reader_next(reader);
  require(stale_reader.status == InvalidArgument, "stale reader was accepted");
  acyclic_next_result_release(stale_reader);

  // Engine close is idempotent and stale engine operations are rejected.
  acyclic_embedded_engine_close(engine);
  const AcyclicAppendResult stale_engine = acyclic_embedded_engine_append(
      engine, kPath, sizeof(kPath) - 1, kValue, sizeof(kValue) - 1);
  require(stale_engine.status == InvalidArgument, "stale engine was accepted");
  acyclic_append_result_release(stale_engine);

  const uint64_t live_engine = acyclic_embedded_engine_open();
  require(live_engine != 0, "second engine open failed");
  const AcyclicAppendResult live_seed = acyclic_embedded_engine_append(
      live_engine, kPath, sizeof(kPath) - 1, kValue, sizeof(kValue) - 1);
  require(live_seed.status == Ok, "live reader seed append failed");
  acyclic_append_result_release(live_seed);
  // A cancelled live reader must report cancellation and remain safely closable.
  const AcyclicOpenResult follow = acyclic_embedded_reader_open(
      live_engine, kPath, sizeof(kPath) - 1, 0, 0, 1);
  require(follow.status == Ok, "follow reader open failed");
  acyclic_embedded_reader_cancel(follow.reader);
  const AcyclicNextResult cancelled = acyclic_embedded_reader_next(follow.reader);
  require(cancelled.status == Cancelled, "cancelled reader was not observable");
  acyclic_next_result_release(cancelled);
  acyclic_open_result_release(follow);

  // Release the same owned diagnostic buffer twice; the second release is a no-op.
  const AcyclicOpenResult invalid = acyclic_embedded_reader_open(
      live_engine, kPath, sizeof(kPath) - 1, 0, 0, 99);
  require(invalid.status == InvalidArgument && invalid.message.id != 0,
          "invalid mode did not produce an owned diagnostic");
  const AcyclicBuffer duplicate = invalid.message;
  require(acyclic_buffer_release(invalid.message) == Ok,
          "first owned-buffer release failed");
  require(acyclic_buffer_release(duplicate) == InvalidArgument,
          "duplicate owned-buffer release was accepted");
  acyclic_open_result_release(invalid);

  const uint8_t unknown_operation[] = "unknown_operation";
  const AcyclicWireResult invalid_wire = acyclic_embedded_engine_wire_call(
      live_engine, unknown_operation, sizeof(unknown_operation) - 1, nullptr, 0);
  require(invalid_wire.status == InvalidArgument,
          "unknown wire operation was not rejected");
  acyclic_wire_result_release(invalid_wire);
  // Releasing the same result again must be harmless because Rust checks buffer identity.
  acyclic_wire_result_release(invalid_wire);

  acyclic_embedded_engine_close(live_engine);
  puts("C++ embedded lifetime negative smoke passed");
  return EXIT_SUCCESS;
}
