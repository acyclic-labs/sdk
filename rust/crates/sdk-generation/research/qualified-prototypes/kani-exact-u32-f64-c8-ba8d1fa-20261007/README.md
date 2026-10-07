# Pinned source-bound Kani proof

This artifact verifies the exported acyclic_fs::exact_u32_from_f64 function from
the actual filesystem crate through a path dependency. The harness contains no
copied conversion function or model.

The source-bound receipt uses revision
f775b8465d836cf69b358f9fa9ebd8edbfa819db from
work/filesystem-main-port-latest265. Before accepting a new receipt, verify
the production source revision and hashes in verification-manifest.json.

With Kani 0.68.0, CBMC 6.11.0, and nightly-2026-08-21 installed, run:

  ./run-proof.sh
  ./run-negative.sh

The first command must end with TERMINAL_EXIT=0 and a successful one-harness
summary. The second is an intentional control and must end with
TERMINAL_EXIT=1, failing on the assertion that production accepts 0.5.
Use a task-local Linux ext4 target directory and offline cached dependencies.
Do not interpret either receipt as a proof of a different source revision.

The source path in Cargo.toml is deliberately explicit and source-bound. A
clean checkout must preserve that source checkout relation or update the path
and refresh the source hashes and receipt.


## c8 source identity

The authoritative receipts for this copied runner are eceipt.c8-ba8d1fa.json and eceipt.c8-ba8d1fa.negative-control.json, bound to filesystem commit a8d1fa931912e69a9c6e591423552b58fe57557. Files named eceipt.json, eceipt.negative-control.json, and their latest265 raw logs are retained historical evidence from the prior 775b8465d836cf69b358f9fa9ebd8edbfa819db source and are not relabeled.
