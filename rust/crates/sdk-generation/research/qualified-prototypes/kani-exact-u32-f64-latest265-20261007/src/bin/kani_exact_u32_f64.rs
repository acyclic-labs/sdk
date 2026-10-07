//! Kani harness bound directly to the filesystem production export.
//!
//! The harness calls acyclic_fs::exact_u32_from_f64; it does not reproduce
//! the admission algorithm. The symbolic input ranges over every u64 IEEE-754
//! bit pattern, including NaNs, infinities, signed zero, subnormals, and
//! finite values. The branch predicate below is the theorem specification,
//! not an implementation substitute.

use acyclic_fs::exact_u32_from_f64;

#[kani::proof]
fn exact_u32_from_f64_all_bit_patterns() {
    let bits: u64 = kani::any();
    let number = f64::from_bits(bits);
    let result = exact_u32_from_f64(number);

    let admissible = number.is_finite()
        && number.fract() == 0.0
        && number >= 0.0
        && number <= u32::MAX as f64;

    if admissible {
        match result {
            Ok(converted) => kani::assert(
                f64::from(converted) == number,
                "accepted value preserves the exact numeric u32 value",
            ),
            Err(_) => kani::assert(
                false,
                "every admissible number must be accepted",
            ),
        }
    } else {
        kani::assert(
            result.is_err(),
            "every non-admissible bit pattern is rejected",
        );
    }
}

fn main() {}
