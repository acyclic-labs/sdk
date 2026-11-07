# Native owner-envelope validation

`native_owner_record` links the public native-runtime writer, decoder and platform
protection implementation. Its oracle is shared with ordinary corpus replay and
the original writer capture, inside the published native-runtime crate.

Run the retained control in a disposable checkout:

```sh
bun rust/fuzz/native-codec-control.mjs owner-envelope /tmp/native-owner-control
```

The producer captures three deliberately nonsecret SDK writer outputs. No real
signing key or owner file is read. The controller records the original emitted
bytes and hashes, plants an actual integrity-guard bypass and requires the SAME
oracle to report `corrupt record decoded`. It restores the source, runs 1,000
deterministic fuzz iterations, requires the ordinary corpus test to pass and
exports native source coverage with its actual corpus. Compiler failures do not
satisfy the negative control. The original source, oracle and complete lockfile
semantics are checked and the original bytes restored on every exit.
Capture and fuzzing write only into the evidence directory's `corpus` directory;
the retained source corpus must remain unchanged.

The existing nightly-2026-07-01 toolchain, llvm-tools component and cargo-fuzz
must be installed. Coverage export currently selects Linux x86_64 tools.
Append `--routine` to qualify only the ordinary retained-corpus test with the
same production defect, without repeating the campaign.
Append `--coverage-only` to measure the retained corpus without repeating a
qualified campaign or mutation. Coverage builds stay below `CARGO_TARGET_DIR`
(or the workspace's `target` directory when that variable is absent).
Relative target directories are resolved from the workspace root, including
when the controller is invoked from another directory.

The oracle checks arbitrary wire bytes, current-owner writer round trips,
corruption, partial records, caller bounds and the original copy refusal.
Fuzz-generated writer payloads use at most one 4 KiB chunk per input; arbitrary
wire input still reaches the decoder's actual configured 65,536-byte bound.
Generated writer payloads retain the native owner's nonempty-payload premise;
empty arbitrary wire still reaches the reader. The cross-platform empty-plaintext
encoder contract is not established by this target.
Exhaustive retained-seed prefix and corruption checks stay outside the fuzz hot
path. No second checksum, parser, meter or protection adapter is implemented.

Unix corpus bytes are portable parser inputs. They do not qualify another user's
Windows DPAPI decryption. Ordinary replay separately creates current-owner
outputs through the real native protection API; randomized Windows ciphertext
is not compared to retained Unix writer bytes. Protected-file permissions,
replacement/retirement, metered admission and Windows error/unwind branches
remain separate native conformance obligations. Coverage is a measured subset,
not a claim that every platform or branch is proved.
