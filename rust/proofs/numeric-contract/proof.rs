#[path = "../../crates/filesystem-base/src/numeric.rs"]
mod production;

#[kani::proof]
fn admission_preserves_all_numbers_and_accepts_every_u32() {
    let number = f64::from_bits(kani::any());
    if let Ok(value) = production::exact_u32_from_f64(number) {
        kani::assert(f64::from(value) == number, "admission preserves the number");
    }
    let value: u32 = kani::any();
    kani::assert(
        production::exact_u32_from_f64(f64::from(value)) == Ok(value),
        "every u32 is admitted exactly",
    );
}
