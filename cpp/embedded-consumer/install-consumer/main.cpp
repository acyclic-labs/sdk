#include <acyclic/embedded.hpp>

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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
  acyclic::Engine engine = acyclic::Engine::open();
  require(engine.valid(), "installed facade engine open failed");
  const uint8_t path[] = "cpp/installed";
  const uint8_t value[] = "installed facade";
  acyclic::AppendResult append = engine.append(
      path, sizeof(path) - 1, value, sizeof(value) - 1);
  require(append.status() == Ok, "installed facade append failed");
  acyclic::Reader reader = engine.read(path, sizeof(path) - 1, 0, 1);
  require(reader.valid(), "installed facade reader open failed");
  acyclic::NextResult next = reader.next();
  require(next.status() == Ok, "installed facade read failed");
  require(next.size() == sizeof(value) - 1 &&
              memcmp(next.data(), value, sizeof(value) - 1) == 0,
          "installed facade payload mismatch");
  puts("installed C++ facade smoke passed");
  return EXIT_SUCCESS;
}

