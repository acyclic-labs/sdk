# Swift Windows/Mac runtime lane — 2026-10-07

Receipt: `swift-current-working-tree-win-mac-20261007-receipt.json` (SHA-256 `4402A57948B07F2892ED94328D63F94B11D77CFD21236C65F497A87A7315B801`).

## Windows

- Installed toolchain: Swift 6.4 (`x86_64-unknown-windows-msvc`).
- Producer workspace: `Q:\sdk\work\sdkgen-actors-c8-minimal`, revision `1ac5b398644d9d7056a95cd579c6a5315dd98db8`; maintained Rust source SHA-256 `CC4706F8DDAD3A85CC2E35A433C07282C5B4E801E770F1E16BE3AA8A7608618E`; loaded DLL SHA-256 `8601E37B58020E7C5F2E6ADF6445D59679002D78018FA95B14E3F7B09DCFEDE9`.
- Maintained Swift generator: UniFFI 0.31 source revision `c3d82376d5b5e32a7886a4af7f8a65de8fe60795`; generated Swift hashes are `699903753C4E4C378E6D05F36917F7BD51D797A9AB0F9D30033F24324DA35140` and `CC419003CCB5A1BA006B2298C52A37FB3955299C41573AB6B3C9255C4D496855`.
- Boundary executable output: `PASS c8 source-pinned Swift all19 roots, cursor 0/2^63/u64MAX, PositiveU64.max, presence, immutable records, CurrentHead marker, all eight operations`.
- Cancellation executable output: `baseline started=0 aborted=0 active=0`, `active-before-cancel started=1 aborted=0 active=1`, `task-cancel-outcome=error=CancellationError()`, `final started=1 aborted=1 active=0`, `PASS generated Swift Task.cancel -> Rust future cancel; cancellation argument=nil; no manual handle`.
- Negative SwiftPM build failed as intended with readonly `let` assignment and `ActorId(rawValue:)` label errors.
- Task-owned Windows fixture processes were stopped after these runs and verified absent.

A clean detached git worktree at `77997805785e8a61f1bf7580ca273e992caee740` was also built. Its Rust source SHA differs from the maintained nominal source (`116288CF44F352BDC5C1B395AF66B7F230AEAC25F4F9E8C7C466F38375BF4A98`), and binding generation produced `CurrentHeadMarker = Bool` with no nominal custom wrappers. Running that clean DLL against the nominal package exited `0xC0000139`; it is retained as mismatch evidence and is not a qualification claim.

## macOS ivar

- Independently bounded SSH reached `ivar`; Swift is Apple Swift 6.1.2.
- Existing fixture processes were observed, not restarted: all8 fixture PID 1568 and pending fixture PID 1708. Existing Swift package/native hashes are recorded in the JSON receipt.
- Existing installed Mac Swift all8 consumer output included all stage markers through wrong-token handling and ended with `PASS post-patch ordinary completion + all eight typed operations/u64 cursor + typed service error`.
- A task-owned copy of the installed package was built for cancellation. The pending fixture actor prefix was matched in the task-owned source. Output: `active-before-cancel started=7 aborted=6 active=1`, `task-cancel-outcome=error=CancellationError()`, `final started=7 aborted=7 active=0`, `PASS generated Swift Task.cancel -> Rust future cancel; cancellation argument=nil; no manual handle`.
- No remote fixture was stopped or restarted.

Linux Swift 6.4 remains pending because the official archive CDN repeatedly reset/throttled; this does not exclude Swift generally. The original Linux receipt remains unchanged.
