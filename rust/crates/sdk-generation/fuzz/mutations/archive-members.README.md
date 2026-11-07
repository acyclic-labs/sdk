# Archive inventory and body controls

Apply one patch at a time and run:

```sh
cargo test --locked -p sdk-generation --lib historical::tests::archive_codec::archive_members_preserve_original_inventory_and_bodies -- --exact --nocapture
```

Revert that patch before applying the next. Each mutation changes the actual
production map decoder and must fail at `archive admission lost original member
bytes`. One drops all members; one erases their bodies. The old fuzz target only
checked accepted paths and therefore accepted both mutations. The replacement
property and target share one original-byte oracle and the captured Cargo input.
Compiler/setup errors do not count as semantic detections. Restored code must
pass the changed archive conformance controls and the fuzz target must compile.
