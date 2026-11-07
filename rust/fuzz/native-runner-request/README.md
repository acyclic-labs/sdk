# Native runner request validation

`native_runner_request` includes the production static runner's decoder and
native instruction type unchanged. Its backing adapter uses the host allocator
in place of anonymous mappings. The oracle checks descriptor custody, flags,
original string and instruction ranges, and aligned, terminated argument and
environment vectors without starting a process.

The three corpus entries are complete original requests captured from the Linux
runtime's actual sender during the following native controls on 2026-11-07. Each
entry starts with its original descriptor count, followed by the original request
body. No bytes were rewritten, and only explicitly cleared environments were
captured.

| Entry | Original operation | SHA-256 |
| --- | --- | --- |
| `explicit-environment` | `a_custodied_program_receives_exactly_its_environment_and_directory`, `/usr/bin/env` with `ACYCLIC_ONLY=1` | `51c15b292af7978e75d11ed385e524c994e99a27965dfe60b1e3668ae5e71def` |
| `confined-command` | `kernel_boundary_confines_descendants_and_preserves_host_authority`, original confined shell command and native instruction policy | `4db9ca9d828a5d68decde9727123f416ad41532c1473e99e26eb5010d88aa5b8` |
| `owner-exit` | `linux_server_loss_and_owner_death_retire_the_original_connection_and_namespace`, original unmount action | `fd58083cb26247c19474e7bf7ae6391b23533486d661542e1565742c1987be5f` |

From the repository root, build the instrumented target and run a finite campaign:

```sh
RUSTFLAGS="-Cpasses=sancov-module -Cllvm-args=-sanitizer-coverage-level=4 \
-Cllvm-args=-sanitizer-coverage-inline-8bit-counters \
-Cllvm-args=-sanitizer-coverage-pc-table \
-Cllvm-args=-sanitizer-coverage-trace-compares" \
cargo run --locked --target x86_64-unknown-linux-gnu \
  -p acyclic-sdk-fuzz --no-default-features --features runner-request \
  --bin native_runner_request -- \
  -seed=1 -runs=100000 rust/fuzz/corpus/native_runner_request
```

The explicit target keeps coverage instrumentation out of host build scripts.
LibFuzzer feedback is not a source branch coverage report. These inputs and this
decoder oracle do not establish process startup, physical mount retirement or
cross-platform runtime guarantees; the native controls establish those separately.

The retained `mutations/surplus-descriptor.patch` removes the production decoder's
exact descriptor count check. Apply it with `git apply`, copy the original
`explicit-environment` entry to a temporary file, and increment only its first
byte. Running the instrumented target on that file with `-runs=1` must fail with
`descriptor roles consumed every original descriptor`. Restore the decoder with
`git apply -R` before running the unchanged corpus and campaign. A compiler error
or unrelated panic is not evidence that the oracle detected the mutation.
