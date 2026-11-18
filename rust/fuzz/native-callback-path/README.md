# Native callback path validation

`callback_path` includes the production ProjFS component reader and writer
directly. It checks arbitrary byte input and constructs an independently valid
component that both codecs must admit unchanged. Conditional roundtrips alone
would let both codecs reject every input.

From the repository root, run the retained corpus and a finite campaign:

```sh
target=//rust/fuzz/native-callback-path:callback_path
rust_flag=--@rules_rust//rust/settings:per_crate_rustc_flag
instrumentation=(
  "$rust_flag=$target@-Cpasses=sancov-module"
  "$rust_flag=$target@-Cllvm-args=-sanitizer-coverage-level=4"
  "$rust_flag=$target@-Cllvm-args=-sanitizer-coverage-inline-8bit-counters"
  "$rust_flag=$target@-Cllvm-args=-sanitizer-coverage-pc-table"
  "$rust_flag=$target@-Cllvm-args=-sanitizer-coverage-trace-compares"
)
bazel build --remote_download_toplevel "${instrumentation[@]}" "$target"
binary="$(bazel info "${instrumentation[@]}" bazel-bin)/rust/fuzz/native-callback-path/callback_path"
if [ ! -f "$binary" ]; then binary="$binary.exe"; fi
node scripts/fuzz-smoke.mjs "$binary" \
  rust/fuzz/native-callback-path/corpus/callback_path -- -seed=1 -runs=4096
```

The per-crate flags keep coverage instrumentation out of host build scripts,
which do not link the libFuzzer runtime. LibFuzzer feedback is not a
source-branch coverage report.

The [shared smoke adapter](../../../scripts/fuzz-smoke.md) publishes an explicit
campaign terminal and reuses only an exact recorded PASS. It keeps this target's
4096-run budget, instrumentation and entire retained corpus. The external
coordinator supplies the shared runner; a missing runner fails explicitly.

The two `windows-enumerated-*` seeds were captured on Windows build 26200
using `System.IO.Directory.EnumerateFiles` on actual temporary files. Their
relative paths were obtained with `System.IO.Path.GetRelativePath`, then encoded
with `System.Text.Encoding.Unicode.GetBytes` (UTF-16LE without a terminating NUL).
The temporary tree contained `name with spaces` and `nested-Ω-🦀\leaf`.

| Seed | SHA-256 |
| --- | --- |
| `windows-enumerated-spaces` | `3a65359ba8bac2c1350267993b1526c261842b1783a5ca08bd48f7927eae5c96` |
| `windows-enumerated-nested` | `80fa1b2859d7bae2a8a7c7f07a496c4316e84748240f18942d1c48e8740c4d16` |

These are OS-enumerated path inputs, not a recorded ProjFS callback session.
The other seeds exercise adversarial path syntax; their external capture
provenance is unqualified. Neither corpus establishes callback ABI, mount
lifetime or physical I/O retirement guarantees.

The kept `mutations/refuse-components.patch` and
`mutations/refuse-wide-components.patch` plant over-refusal in each production
codec. Apply each separately with `git apply`, then run the same target on
`rust/fuzz/native-callback-path/corpus/callback_path/windows-enumerated-nested` with `-runs=1`. The unchanged
oracle must fail respectively with `valid callback component must be admitted
unchanged` or `valid callback component must be encodable`. Restore the source
with `git apply -R` before testing the other mutation. A compiler failure or an
unrelated panic is not a detected mutation.
