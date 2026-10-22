# bb2 development compile costs

Measured on 2026-10-22, comparing source `e9b3a0f92` with the dev/test profile
and Linux linker change in `80edcecdf`. The compiler was stable Rust 1.98.1.
Dev and inherited test optimization change from level 1 to level 0; SHA-2 and
BLAKE3 retain level 3 for executable hashing. Release profiles retain LLVM.
The measurements used clang with `-fuse-ld=mold` in Cargo rustflags. The final
configuration passes the same option through a linker script to preserve it
when qualification sets `RUSTFLAGS`; that routing change is checked separately.

## Method

All commands ran through `Q:/lanes/bs.ps1` on bb2, with one lane target,
`CARGO_BUILD_JOBS=16`, incremental compilation enabled, compiler wrappers unset,
and the build-slot dev/test debug setting of 0. Cargo wall times below exclude
worktree synchronization, SSH recovery and report copy-back.

The first invocation compiles the selected crate, retaining shared dependency
artifacts between packages. These are **not pristine-target cold builds**.
Dependency feature sets and host contention differ across commands, so the
ratios are observations rather than isolated compiler speedup estimates.
The edit invocation appends `const _: () = ();` to the target's root source and
restores the source afterward. It measures a small added item, not every possible
function-body edit.

```sh
cargo build --locked --timings -p CRATE
# Add the one-line item, repeat the command, then restore the source.
cargo test --locked --no-run --timings -p CRATE --lib
# Graphcoder uses --bin graphcoder instead of --lib.
```

## Profile and linker results

Whole `cargo build` command wall time, seconds:

| Package | First before | First after | Edit before | Edit after |
|---|---:|---:|---:|---:|
| acyclic-fs | 61.93 | 48.37 | 5.91 | 5.76 |
| acyclic-harness-core | 51.17 | 41.05 | 4.15 | 3.87 |
| graphcoder | 208.77 | 144.02 | 1.91 | 1.29 |
| acyclic-plugin binary | 270.43 | 136.19 | 60.79 | 3.01 |

Whole `cargo test --no-run` command wall time, seconds:

| Package and target | First before | First after | Edit before | Edit after |
|---|---:|---:|---:|---:|
| acyclic-fs --lib | 349.52 | 102.29 | 18.40 | 15.13 |
| acyclic-harness-core --lib | 90.56 | 60.75 | 6.25 | 6.92 |
| graphcoder --bin graphcoder | 223.13 | 123.66 | 1.57 | 1.60 |
| acyclic-plugin --lib | 157.67 | 112.86 | 1.22 | 0.29 |

Plugin's library has no tests and little code; its first command mostly builds
dependencies. The binary row measures the actual CLI. Core and Graphcoder test
edit times did not improve, despite lower first-build costs.

The original combined test-baseline wrapper timed out during plugin dependency
compilation. Completed filesystem/core numbers above are from that original
run; resumed plugin compilation reused partial dependency output. Warm recovery
runs of filesystem/core were 36.99/18.71 and 21.87/6.83 seconds respectively and
are not substituted for the original measurements. The plugin build baseline
also saw host load 134.55 versus about 19–22 for the other build baselines;
Graphcoder's after build saw load 73.46.

Compiler user CPU time for filesystem's first test build fell from 2443.19 to
178.56 seconds; Graphcoder's first build fell from 1085.98 to 295.15 seconds.
This reduces work on the shared box as well as observed wall time.

## Remaining costs

Cargo timing reports identify these large units:

- Filesystem library test: 332.80 before, 90.70 seconds after.
- Filesystem normal library: front end 30.37 before, 31.83 seconds after;
  code generation 14.54 before, 2.66 seconds after.
- Graphcoder's harness-core dependency: 81.77 before, 57.54 seconds after;
  harness: 58.45 before, 28.04 seconds after; filesystem: 50.81 before,
  40.89 seconds after.
- Plugin's original build: harness-core 82.07, CLI binary 73.08, filesystem
  58.53 and harness 54.91 seconds. Build scripts and individual proc-macro
  crates were small relative to these units.

Front-end work remains substantial. Extracting independent modules can reduce
the code a small change must process; changing codegen alone cannot remove it.
For routine validation, select the touched package and relevant reverse
dependencies together in one Cargo test invocation, with named test filters,
so their dependencies share one feature resolution and build. A separate
`cargo build` before `cargo test` is unnecessary when only tests are needed.

## Backend and qualification

The installed `nightly-2026-07-01` distributed Cranelift component failed this
independent recovery test:

```rust
#[test]
fn catches_panic() {
    assert!(std::panic::catch_unwind(|| panic!("caught")).is_err());
}
```

The panic escaped the inner handler and failed the test. Ordinary success and
`#[should_panic]` controls passed (2 passed, 1 failed). The fixture used
`[unstable] codegen-backend = true` and dev/test `codegen-backend = "cranelift"`,
opt-level 0 and mold, through the pinned nightly with a separate target
subdirectory. No bootstrap was used. Cranelift rollout remains deferred until
an unwind-capable backend passes this test and existing cancellation coverage.
See the [upstream unwinding issue](https://github.com/rust-lang/rustc_codegen_cranelift/issues/1567).

CI's Rust-toolchain action and Windows setup read the exact stable release in
`rust-toolchain.toml`; the merge gate invokes Cargo using that pin. The action
also installs clang and mold on x64 Linux for the configured GNU Linux linker.
Other target triples retain their linkers.
The linker script carries the mold option so qualification's explicit
`RUSTFLAGS` cannot replace it; Cargo gives environment flags priority over
configured rustflags ([Cargo configuration](https://doc.rust-lang.org/cargo/reference/config.html#buildrustflags)).

`node scripts/check-dev-build.mjs` builds a dependency-free fixture using the
workspace profiles. It checks effective dev/test optimization, panic recovery,
mold's executable comment and fresh release LLVM IR. Its initial bb2 run took
0.81 seconds. Restoring the original optimization profile rejects the fixture
at the effective-profile assertion; restoring only the original linker is a
separate negative control.
