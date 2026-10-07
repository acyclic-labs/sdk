//! Kani harnesses for the public exact-u32 production conversion.

use crate::exact_u32_from_f64;

#[kani::proof]
#[kani::unwind(1)]
fn exact_u32_from_f64_all_bits() {
    let bits: u64 = kani::any();
    let number = f64::from_bits(bits);
    let admissible = number.is_finite()
        && number.fract() == 0.0
        && number >= 0.0
        && number <= f64::from(u32::MAX);

    match exact_u32_from_f64(number) {
        Ok(value) => {
            assert!(admissible);
            assert_eq!(f64::from(value), number);
        }
        Err(_) => assert!(!admissible),
    }
}

#[kani::proof]
#[kani::unwind(1)]
fn exact_u32_from_f64_negative_control_accepts_fraction() {
    assert!(exact_u32_from_f64(0.5).is_ok());
}
