//! Numeric admission shared by language bindings.

/// Converts a JavaScript-number representation to an exact [`u32`].
///
/// Binding ABIs commonly represent unsigned integers as `f64`/JavaScript
/// `number` values. This conversion admits only finite integral values in the
/// complete `u32` range. It deliberately accepts zero; operation-specific
/// minimums remain enforced by the canonical filesystem policy.
pub fn exact_u32_from_f64(number: f64) -> Result<u32, &'static str> {
    if !number.is_finite() || number.fract() != 0.0 || number < 0.0 || number > u32::MAX as f64 {
        return Err("expected a finite integer in the u32 range");
    }
    Ok(number as u32)
}

#[cfg(test)]
mod tests {
    use super::exact_u32_from_f64;

    #[test]
    fn accepts_complete_u32_range_including_zero() {
        assert_eq!(exact_u32_from_f64(0.0), Ok(0));
        assert_eq!(exact_u32_from_f64(u32::MAX as f64), Ok(u32::MAX));
    }

    #[test]
    fn rejects_non_exact_numbers() {
        for value in [
            -1.0,
            0.5,
            (u32::MAX as f64) + 1.0,
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
        ] {
            assert!(
                exact_u32_from_f64(value).is_err(),
                "accepted invalid value {value}"
            );
        }
    }
}
