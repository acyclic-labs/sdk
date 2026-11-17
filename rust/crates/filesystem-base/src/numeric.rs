//! Exact JavaScript numeric admission shared by the generated bindings.

/// Converts a JavaScript `number` to a `u32` without coercion or precision loss.
pub const fn exact_u32_from_f64(number: f64) -> Result<u32, &'static str> {
    let bits = number.to_bits();
    if bits << 1 == 0 {
        return Ok(0);
    }
    // Including the sign bit rejects negative numbers. Binary64's exponent
    // bias is 1023; a u32 has at most 31 bits above its leading one.
    let exponent = bits >> 52;
    if exponent < 1023 || exponent > 1054 {
        return Err("expected a finite integer in the u32 range");
    }
    let shift = 1075 - exponent;
    let significand = (bits & 0x000f_ffff_ffff_ffff) | (1 << 52);
    let integer = significand >> shift;
    if integer << shift != significand {
        return Err("expected a finite integer in the u32 range");
    }
    let [a, b, c, d, _, _, _, _] = integer.to_le_bytes();
    Ok(u32::from_le_bytes([a, b, c, d]))
}
