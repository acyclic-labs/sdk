pub struct Always;

#[cfg(feature = "base")]
pub fn base_only() {}

#[cfg(feature = "default")]
pub fn default_only() {}

#[cfg(not(feature = "extra"))]
pub fn overloaded(value: u64) -> u64 {
    value
}

#[cfg(feature = "extra")]
pub fn overloaded(value: &str) -> usize {
    value.len()
}

#[cfg(feature = "extra")]
pub struct FeatureOnly;
