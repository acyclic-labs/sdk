#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define __STDC_VERSION__ 202311L
extern "C" {
#include "acyclic_embedded_prototype.h"
}
#undef __STDC_VERSION__

namespace {

void require(bool condition, const char* message) {
  if (!condition) {
    fputs(message, stderr);
    fputc('\n', stderr);
    exit(EXIT_FAILURE);
  }
}

}  // namespace

int main() {
  require(acyclic_embedded_abi_version() == 1, "unexpected ABI version");

  const char path[] = "cpp/embedded";
  const char value[] = "hello from C++";
  const uint64_t engine = acyclic_embedded_engine_open();
  require(engine != 0, "engine open failed");

  const AcyclicAppendResult append = acyclic_embedded_engine_append(
      engine, reinterpret_cast<const uint8_t*>(path), sizeof(path) - 1,
      reinterpret_cast<const uint8_t*>(value), sizeof(value) - 1);
  require(append.status == Ok, "append failed");
  acyclic_append_result_release(append);

  AcyclicOpenResult opened = acyclic_embedded_reader_open(\n      engine, reinterpret_cast<const uint8_t*>(path), sizeof(path) - 1,\n      0, 1, 0);\n  require(opened.status == Ok && opened.reader != 0,\n          "reader open failed");\n  const uint64_t reader = acyclic_open_result_take_reader(&opened);\n  require(reader != 0 && opened.reader == 0, "reader ownership transfer failed");\n\n  const AcyclicNextResult next = acyclic_embedded_reader_next(reader);
  require(next.status == Ok, "reader next failed");
  require(next.value.len == sizeof(value) - 1, "payload length mismatch");
  require(memcmp(next.value.ptr, value, sizeof(value) - 1) == 0,
          "payload mismatch");
  acyclic_next_result_release(next);

  acyclic_open_result_release(opened);
  acyclic_embedded_engine_close(engine);
  puts("C++ embedded ABI smoke passed");
  return EXIT_SUCCESS;
}
