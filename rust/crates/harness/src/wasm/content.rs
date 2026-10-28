use super::*;

pub(super) fn exact_nonnegative_u64(value: f64, field: &str) -> Result<u64, JsValue> {
    if !crate::conversation::is_exact_js_number(value) || value < 0.0 || value.fract() != 0.0 {
        return Err(JsValue::from_str(&format!(
            "{field} must be a safe non-negative integer"
        )));
    }
    value
        .to_string()
        .parse::<u64>()
        .map_err(|_| JsValue::from_str(&format!("{field} is outside the supported range")))
}
