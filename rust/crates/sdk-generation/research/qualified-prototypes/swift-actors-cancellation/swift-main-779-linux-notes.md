# Swift current 779 Linux cohort (in progress)

Producer revision: `77997805785e8a61f1bf7580ca273e992caee740`
Producer `actors-uniffi/src/lib.rs`: `CC4706F8DDAD3A85CC2E35A433C07282C5B4E801E770F1E16BE3AA8A7608618E`
Linux producer library: `755B758CF1C128C0764B08C5E419989E0EE096C4CAA7E17FF94C56CFA244A231`

Maintained UniFFI Swift 0.31 source:
- `uniffi-swift-oss-prototype/source`
- `CustomType.swift`: `10A58FB8890C0D2DF102F45E1330BB2ABBB20938A9C8FC2803652C7A0C9B4035`
- `EnumTemplate.swift`: `C0E66A925F2DB14035C3C80BB3C84E8E2E7F9E2ACD9D41D5EA6469CC47082E81`

Generated Linux Swift artifacts were produced from the 779 library with nominal custom types, immutable records, and `Start.CurrentHead` Rust-backed validation. The generated nominal wrappers keep raw storage and trusted lifting private; public factories call Rust validator exports. Static inspection confirms no public raw-value initializers, no public raw storage, all generated records use `let` fields, and `Start.currentHead()` is fallible.

Linux qualification remains pending Swift toolchain acquisition. Historical 84a evidence remains in `swift-c8-runtime-20261007-receipt.json` and is not relabeled.