# Durability test-kit refinement

`testing::durability` is the shared deterministic fault-injection test kit. Its
crash-latch model executes the actual `begin` and `Call::before` implementation
with Loom synchronization. It proves that two callers retain the first injected
crash and cannot admit a later write. It does not model an OS crash, filesystem
recovery, a physical durability barrier or native worker retirement.

The ordinary `every_write_cut_preserves_its_exact_prefix_and_cause` check uses a
real temporary file through `write_file_all`. It compares bytes read from that
same file, transferred prefixes, original error causes and the operation trace
for every enumerated cut. This is local file-effect evidence; it does not prove
power-loss persistence.

Run the retained source-defect controls in disposable checkouts:

```sh
python3 rust/crates/native-runtime/tests/signal-control.py /tmp/durability-crash --durability
python3 rust/crates/native-runtime/tests/signal-control.py /tmp/durability-write --durability-io
```

The first omits the actual crash retention assignment and requires the unchanged
concurrent model to reject a write admitted after the crash. The second replaces
the actual physical write with a successful byte count and requires the unchanged
file-content assertion to fail. Each control then restores the source and runs
the original check again. The shared controller retains source/oracle hashes,
mutated source, exact commands and original diagnostics, and verifies restoration.
Neither control adds a test-only failure branch to the implementation.

The concurrent model is included in the existing native progress qualification.
The file-effect check remains an ordinary library test. These checks accept no
wire format and require no captured wire corpus. Decoder and OS durability
validation have separate obligations.
