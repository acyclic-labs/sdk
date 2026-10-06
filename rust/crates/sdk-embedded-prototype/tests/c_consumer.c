#include "acyclic_embedded_prototype.h"

#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

_Static_assert(sizeof(AcyclicBuffer) == sizeof(uint64_t) + sizeof(void *) +
                   2 * sizeof(size_t),
               "AcyclicBuffer layout changed");
_Static_assert(offsetof(AcyclicBuffer, id) == 0, "AcyclicBuffer id moved");
_Static_assert(Ok == 0 && End == 1 && Pending == 2 && Cancelled == 3 &&
                   InvalidArgument == 4 && ProviderError == 5 && Capacity == 6 && Panic == 7,
               "AcyclicStatus values changed");

int main(void) {
    const uint8_t path[] = "c/consumer";
    const uint8_t value[] = "hello from c";
    const size_t path_len = sizeof(path) - 1;
    const size_t value_len = sizeof(value) - 1;

    uint64_t engine = acyclic_embedded_engine_open();
    assert(engine != 0);
    AcyclicAppendResult append = acyclic_embedded_engine_append(
        engine, path, path_len, value, value_len);
    assert(append.status == Ok);
    assert(append.start == 0 && append.end == 1 && append.tail == 1);
    acyclic_append_result_release(append);

    AcyclicOpenResult open = acyclic_embedded_reader_open(
        engine, path, path_len, 0, 8, 0);
    assert(open.status == Ok && open.reader != 0);
    uint64_t reader = acyclic_open_result_take_reader(&open);
    assert(reader != 0 && open.reader == 0);
    AcyclicNextResult next = acyclic_embedded_reader_next(reader);
    assert(next.status == Ok && next.sequence == 0);
    assert(next.value.len == value_len);
    assert(memcmp(next.value.ptr, value, value_len) == 0);
    assert(acyclic_buffer_release(next.value) == Ok);
    assert(acyclic_buffer_release(next.value) == InvalidArgument);
    acyclic_buffer_release(next.message);

    AcyclicNextResult end = acyclic_embedded_reader_next(reader);
    assert(end.status == End);
    acyclic_next_result_release(end);
    acyclic_embedded_reader_cancel(reader);
    acyclic_embedded_reader_cancel(reader); /* idempotent */
    acyclic_embedded_reader_close(reader);
    acyclic_embedded_reader_close(reader); /* stale close is a no-op */
    assert(acyclic_embedded_reader_next(reader).status == InvalidArgument);
    acyclic_open_result_release(open); /* transferred reader is not closed */
    acyclic_embedded_engine_close(engine);
    acyclic_embedded_engine_close(engine); /* stale close is a no-op */
    return 0;
}
