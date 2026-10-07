# exact_u32_from_f64 source identity review

The all-bit production theorem receipt is bound to launch revision `f775b8465d836cf69b358f9fa9ebd8edbfa819db` and production symbol `acyclic_fs::exact_u32_from_f64` in `rust/crates/filesystem/src/numeric.rs`.

A later canonical Filesystem observation recorded revision `42874b98e8079c14a32981216b0b2d19e365dcf6` (`Derive filesystem boundary metadata from Rust`). The relevant production files remained byte-identical:

- `rust/crates/filesystem/src/numeric.rs`: `51276AA1FF76487DF226F6C6000F951842DBF0F2FEE7948796B4FBDE8FF05551`
- `rust/crates/filesystem/src/lib.rs`: `1BD52C28F32698CC7EF0D1C1F81616060461A531C7522BC2D2FAA90AC9F5B1DB`

The current `filesystem-main-port` and `filesystem-main-port-latest265` checkouts also hash `numeric.rs` to `51276...`; `latest265` has the receipt-bound `lib.rs` hash. Therefore no numeric derive or implementation change invalidates the theorem. This is a byte-identity observation, not a relabeling of the launch receipt to the later revision.

The theorem quantifies all `u64` IEEE-754 bit patterns via `f64::from_bits`: production accepts exactly finite integral nonnegative values no greater than `u32::MAX`, preserves numeric value on success, and returns `Err` otherwise. The expected negative control asserts that production accepts `0.5`; it fails (`1 of 10 failed`) against the same production symbol, with unchanged production hashes.

Evidence is `../receipt.json`, `../receipt.negative-control.json`, `exact-u32-f64-kani068.raw.txt`, and `exact-u32-f64-negative-control-kani068.raw.txt`. No copied production function or proof-only replacement is used.