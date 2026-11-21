//! Conversion at Number-valued browser interfaces.

pub(super) fn u64_to_f64(value: u64) -> f64 {
    let [a, b, c, d, e, f, g, h] = value.to_le_bytes();
    let low = u32::from_le_bytes([a, b, c, d]);
    let high = u32::from_le_bytes([e, f, g, h]);
    f64::from(high) * 4_294_967_296.0 + f64::from(low)
}

/// Whole nonnegative units, saturating at the u64 limit.
pub(super) fn saturating_u64(value: f64) -> u64 {
    if value.is_nan() || value < 1.0 {
        return 0;
    }
    let bits = value.to_bits();
    // Binary64's exponent is biased by 1023. At exponent 64 the leading
    // one cannot fit into a u64. This includes positive infinity.
    let exponent = bits >> 52;
    if exponent >= 1087 {
        return u64::MAX;
    }
    let significand = (bits & 0x000f_ffff_ffff_ffff) | (1 << 52);
    // The stored significand has 52 bits below the leading one.
    if exponent < 1075 {
        significand >> (1075 - exponent)
    } else {
        significand << (exponent - 1075)
    }
}
