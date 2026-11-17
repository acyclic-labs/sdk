//! Exact JavaScript numeric admission shared by the generated bindings.

/// Converts a JavaScript `number` to a `u32` without coercion or precision loss.
pub const fn exact_u32_from_f64(number: f64) -> Result<u32, &'static str> {
    let value = number as u32;
    if value as f64 != number {
        return Err("expected a finite integer in the u32 range");
    }
    Ok(value)
}

// The original boundary vectors are checked in every target's compilation.
const _: () = {
    assert!(matches!(exact_u32_from_f64(0.0), Ok(0)));
    assert!(matches!(exact_u32_from_f64(u32::MAX as f64), Ok(u32::MAX)));
    let invalid = [
        -1.0,
        0.5,
        u32::MAX as f64 + 1.0,
        f64::NAN,
        f64::INFINITY,
        f64::NEG_INFINITY,
    ];
    let mut index = 0;
    while index < invalid.len() {
        assert!(exact_u32_from_f64(invalid[index]).is_err());
        index += 1;
    }
};
