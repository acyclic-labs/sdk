"""ctypes smoke test for the generated embedded ABI."""

import ctypes
import sys


class Buffer(ctypes.Structure):
    _fields_ = [
        ("id", ctypes.c_uint64),
        ("ptr", ctypes.POINTER(ctypes.c_uint8)),
        ("len", ctypes.c_size_t),
        ("capacity", ctypes.c_size_t),
    ]


class AppendResult(ctypes.Structure):
    _fields_ = [
        ("status", ctypes.c_uint32),
        ("start", ctypes.c_uint64),
        ("end", ctypes.c_uint64),
        ("tail", ctypes.c_uint64),
        ("message", Buffer),
    ]


class OpenResult(ctypes.Structure):
    _fields_ = [("status", ctypes.c_uint32), ("reader", ctypes.c_uint64), ("message", Buffer)]


class NextResult(ctypes.Structure):
    _fields_ = [
        ("status", ctypes.c_uint32),
        ("sequence", ctypes.c_uint64),
        ("value", Buffer),
        ("message", Buffer),
    ]


class WireResult(ctypes.Structure):
    _fields_ = [
        ("status", ctypes.c_uint32),
        ("response", Buffer),
        ("message", Buffer),
    ]


OK, END, PENDING, CANCELLED, INVALID, PROVIDER, CAPACITY, PANIC = range(8)

REQUIRED_EXPORTS = (
    "acyclic_embedded_abi_version",
    "acyclic_embedded_engine_open",
    "acyclic_embedded_engine_close",
    "acyclic_embedded_engine_append",
    "acyclic_embedded_reader_open",
    "acyclic_embedded_reader_next",
    "acyclic_embedded_reader_cancel",
    "acyclic_embedded_reader_close",
    "acyclic_buffer_release",
    "acyclic_append_result_release",
    "acyclic_open_result_release",
    "acyclic_open_result_take_reader",
    "acyclic_next_result_release",
    "acyclic_embedded_engine_wire_call",
    "acyclic_wire_result_release",
)


def assert_abi_layout() -> None:
    assert ctypes.sizeof(ctypes.c_void_p) == 8, "the embedded ABI requires a 64-bit process"
    assert ctypes.sizeof(Buffer) == 32
    assert Buffer.id.offset == 0
    assert Buffer.ptr.offset == 8
    assert Buffer.len.offset == 16
    assert Buffer.capacity.offset == 24
    assert ctypes.sizeof(AppendResult) == 64
    assert ctypes.sizeof(OpenResult) == 48
    assert ctypes.sizeof(NextResult) == 80
    assert ctypes.sizeof(WireResult) == 72
    assert WireResult.response.offset == 8
    assert WireResult.message.offset == 40


def bytes_arg(value: bytes):
    storage = ctypes.create_string_buffer(value)
    return storage, ctypes.cast(storage, ctypes.POINTER(ctypes.c_uint8)), len(value)


def clone_buffer(source: Buffer) -> Buffer:
    clone = Buffer()
    clone.id = source.id
    clone.ptr = source.ptr
    clone.len = source.len
    clone.capacity = source.capacity
    return clone


def main() -> None:
    assert_abi_layout()
    dll = ctypes.CDLL(sys.argv[1])
    missing = [name for name in REQUIRED_EXPORTS if not hasattr(dll, name)]
    assert not missing, f"installed runtime is missing Rust ABI exports: {missing}"
    dll.acyclic_embedded_abi_version.restype = ctypes.c_uint32
    assert dll.acyclic_embedded_abi_version() == 1
    dll.acyclic_embedded_engine_open.restype = ctypes.c_uint64
    dll.acyclic_embedded_engine_close.argtypes = [ctypes.c_uint64]
    dll.acyclic_embedded_engine_append.argtypes = [
        ctypes.c_uint64,
        ctypes.POINTER(ctypes.c_uint8),
        ctypes.c_size_t,
        ctypes.POINTER(ctypes.c_uint8),
        ctypes.c_size_t,
    ]
    dll.acyclic_embedded_engine_append.restype = AppendResult
    dll.acyclic_embedded_engine_wire_call.argtypes = [
        ctypes.c_uint64,
        ctypes.POINTER(ctypes.c_uint8),
        ctypes.c_size_t,
        ctypes.POINTER(ctypes.c_uint8),
        ctypes.c_size_t,
    ]
    dll.acyclic_embedded_engine_wire_call.restype = WireResult
    dll.acyclic_wire_result_release.argtypes = [WireResult]
    dll.acyclic_embedded_reader_open.argtypes = [
        ctypes.c_uint64,
        ctypes.POINTER(ctypes.c_uint8),
        ctypes.c_size_t,
        ctypes.c_uint64,
        ctypes.c_uint32,
        ctypes.c_uint32,
    ]
    dll.acyclic_embedded_reader_open.restype = OpenResult
    dll.acyclic_embedded_reader_next.argtypes = [ctypes.c_uint64]
    dll.acyclic_embedded_reader_next.restype = NextResult
    dll.acyclic_embedded_reader_cancel.argtypes = [ctypes.c_uint64]
    dll.acyclic_embedded_reader_close.argtypes = [ctypes.c_uint64]
    dll.acyclic_buffer_release.argtypes = [Buffer]
    dll.acyclic_buffer_release.restype = ctypes.c_uint32
    dll.acyclic_append_result_release.argtypes = [AppendResult]
    dll.acyclic_next_result_release.argtypes = [NextResult]
    dll.acyclic_open_result_release.argtypes = [OpenResult]
    dll.acyclic_open_result_take_reader.argtypes = [ctypes.POINTER(OpenResult)]
    dll.acyclic_open_result_take_reader.restype = ctypes.c_uint64

    engine = dll.acyclic_embedded_engine_open()
    assert engine
    unknown, unknown_ptr, unknown_len = bytes_arg(b"unknown_operation")
    empty, empty_ptr, empty_len = bytes_arg(b"")
    wire = dll.acyclic_embedded_engine_wire_call(
        engine, unknown_ptr, unknown_len, empty_ptr, empty_len
    )
    assert wire.status == INVALID
    dll.acyclic_wire_result_release(wire)
    path, path_ptr, path_len = bytes_arg(b"python/consumer")
    initial, initial_ptr, initial_len = bytes_arg(b"initial")
    append = dll.acyclic_embedded_engine_append(engine, path_ptr, path_len, initial_ptr, initial_len)
    assert append.status == OK and (append.start, append.end, append.tail) == (0, 1, 1)
    dll.acyclic_append_result_release(append)

    opened = dll.acyclic_embedded_reader_open(engine, path_ptr, path_len, 0, 0, 1)
    assert opened.status == OK and opened.reader
    reader = dll.acyclic_open_result_take_reader(ctypes.byref(opened))
    assert reader and opened.reader == 0
    live, live_ptr, live_len = bytes_arg(b"live")
    append = dll.acyclic_embedded_engine_append(engine, path_ptr, path_len, live_ptr, live_len)
    assert append.status == OK
    dll.acyclic_append_result_release(append)

    first = dll.acyclic_embedded_reader_next(reader)
    assert first.status == OK and first.sequence == 0
    assert ctypes.string_at(first.value.ptr, first.value.len) == b"initial"
    assert dll.acyclic_buffer_release(first.value) == OK
    assert dll.acyclic_buffer_release(first.value) == INVALID
    dll.acyclic_buffer_release(first.message)
    second = dll.acyclic_embedded_reader_next(reader)
    assert second.status == OK and second.sequence == 1
    assert ctypes.string_at(second.value.ptr, second.value.len) == b"live"
    dll.acyclic_next_result_release(second)

    dll.acyclic_embedded_reader_cancel(reader)
    dll.acyclic_embedded_reader_cancel(reader)
    cancelled = dll.acyclic_embedded_reader_next(reader)
    assert cancelled.status == CANCELLED
    dll.acyclic_next_result_release(cancelled)
    dll.acyclic_embedded_reader_close(reader)
    dll.acyclic_embedded_reader_close(reader)
    assert dll.acyclic_embedded_reader_next(reader).status == INVALID
    invalid = dll.acyclic_embedded_reader_open(engine, path_ptr, path_len, 0, 0, 99)
    assert invalid.status == INVALID and invalid.message.id != 0
    wrong_length = clone_buffer(invalid.message)
    wrong_length.len += 1
    assert dll.acyclic_buffer_release(wrong_length) == INVALID
    wrong_capacity = clone_buffer(invalid.message)
    wrong_capacity.capacity += 1
    assert dll.acyclic_buffer_release(wrong_capacity) == INVALID
    wrong_pointer = clone_buffer(invalid.message)
    wrong_pointer.ptr = ctypes.cast(ctypes.c_void_p(1), ctypes.POINTER(ctypes.c_uint8))
    assert dll.acyclic_buffer_release(wrong_pointer) == INVALID
    assert dll.acyclic_buffer_release(invalid.message) == OK
    assert dll.acyclic_buffer_release(invalid.message) == INVALID
    dll.acyclic_open_result_release(invalid)
    dll.acyclic_open_result_release(opened)
    dll.acyclic_embedded_engine_close(engine)
    dll.acyclic_embedded_engine_close(engine)


if __name__ == "__main__":
    main()
