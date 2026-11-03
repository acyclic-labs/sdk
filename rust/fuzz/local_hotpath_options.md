# Native benchmark argument validation

The benchmark and `local_hotpath_options` target call the same `Options::parse_from`
implementation. The benchmark supplies `std::env::args().skip(1)`; the target
supplies NUL-separated UTF-8 argument tokens. Neither the target nor corpus replay
runs calibration or creates a measurement directory.

The 21 retained corpus entries are invocation inputs: four accepted configurations
and seventeen refusal cases. Their names state the independent admission oracle.
They cover every option arm, missing values, invalid/overflowing numbers, zero
counts, unknown names, duplicate names, unsorted populations, proof mode and quoted
path/source strings. These inputs are authored command arguments; they contain no
provider response or subprocess output.

The fuzz entry always checks admission of the minimum valid invocation. Every
accepted input must retain positive counts, known unique operations and increasing
known populations. This catches both reject-all behavior and weakened admission.
The ordinary corpus check and fuzzer share this oracle.

From the repository root, replay the corpus with:

```sh
cargo test --locked -p acyclic-sdk-fuzz --no-default-features --features local-hotpath-options --test local_hotpath_options
```

From the repository root, run instrumented mutation with an installed nightly:

```sh
cargo +nightly fuzz run --dev --fuzz-dir rust/fuzz local_hotpath_options --no-default-features --features local-hotpath-options -- -runs=4096
```

The retained negative patch removes the production duplicate-operation guard.
Apply it with `git apply rust/fuzz/local_hotpath_options/omit-duplicate-operation.patch`
from the repository root and run the same corpus check; the unchanged oracle must
fail. Restore the parser with `git apply -R rust/fuzz/local_hotpath_options/omit-duplicate-operation.patch`
before running the positive check.

The retained corpus passes with the original parser. Removing its duplicate guard
fails the shared oracle on duplicate operations. Instrumented source coverage and
the instrumented mutant result remain pending. This target covers argument parsing; native shell execution of recovery
commands and non-UTF-8 OS argument conversion require their own controls.
