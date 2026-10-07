//! Exact JavaScript numeric admission shared by the generated bindings.

/// Converts a JavaScript `number` to a `u32` without coercion or precision loss.
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "validated range checks make these casts lossless"
)]
pub fn exact_u32_from_f64(number: f64) -> Result<u32, &'static str> {
    if !number.is_finite() || number.fract() != 0.0 || number < 0.0 || number > f64::from(u32::MAX)
    {
        return Err("expected a finite integer in the u32 range");
    }
    Ok(number as u32)
}

#[cfg(test)]
mod tests {
    use super::exact_u32_from_f64;

    #[test]
    fn accepts_exact_endpoints() {
        assert_eq!(exact_u32_from_f64(0.0), Ok(0));
        assert_eq!(exact_u32_from_f64(f64::from(u32::MAX)), Ok(u32::MAX));
    }

    #[test]
    fn rejects_non_integer_and_out_of_range_values() {
        for value in [
            -1.0,
            0.5,
            f64::from(u32::MAX) + 1.0,
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
