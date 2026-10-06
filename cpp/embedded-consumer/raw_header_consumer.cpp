#include "acyclic_embedded_prototype.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>

static_assert(sizeof(AcyclicBuffer) == sizeof(uint64_t) + sizeof(void*) +
                  2 * sizeof(size_t),
              "AcyclicBuffer layout changed");
static_assert(Ok == 0 && End == 1 && Pending == 2 && Cancelled == 3 &&
                  InvalidArgument == 4 && ProviderError == 5 && Capacity == 6 &&
                  Panic == 7,
              "AcyclicStatus values changed");

int main() {
  const uint8_t path[] = "cpp/raw-header";
  const uint8_t value[] = "direct generated header";
  constexpr auto path_size = sizeof(path) - 1;
  constexpr auto value_size = sizeof(value) - 1;

  const auto engine = acyclic_embedded_engine_open();
  if (engine == 0) return 1;
  const auto append = acyclic_embedded_engine_append(
      engine, path, path_size, value, value_size);
  if (append.status != Ok || append.start != 0 || append.end != 1) return 2;
  acyclic_append_result_release(append);

  auto opened = acyclic_embedded_reader_open(
      engine, path, path_size, 0, 1, 0);
  if (opened.status != Ok || opened.reader == 0) return 3;
  const auto reader = acyclic_open_result_take_reader(&opened);
  if (reader == 0 || opened.reader != 0) return 4;
  const auto next = acyclic_embedded_reader_next(reader);
  if (next.status != Ok || next.sequence != 0 || next.value.len != value_size ||
      memcmp(next.value.ptr, value, value_size) != 0) return 5;
  acyclic_next_result_release(next);

  const auto end = acyclic_embedded_reader_next(reader);
  if (end.status != End) return 6;
  acyclic_next_result_release(end);
  acyclic_embedded_reader_cancel(reader);
  acyclic_embedded_reader_close(reader);
  acyclic_open_result_release(opened);
  acyclic_embedded_engine_close(engine);
  return 0;
}
