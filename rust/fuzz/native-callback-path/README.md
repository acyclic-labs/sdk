# Native callback path validation

`callback_path` includes the production ProjFS component reader and writer
directly. It checks arbitrary byte input and constructs an independently valid
component that both codecs must admit unchanged. Conditional roundtrips alone
would let both codecs reject every input.

From the repository root, run the retained corpus and a finite campaign:

```sh
cargo run --locked -p native-callback-path-fuzz --bin callback_path -- \
  -seed=1 -runs=4096 rust/fuzz/native-callback-path/corpus/callback_path
```

Use the normal sanitizer-coverage compiler flags for an instrumented campaign;
libFuzzer feedback is not a source-branch coverage report.

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
