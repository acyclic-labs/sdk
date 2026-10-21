// Frozen original encoder/decoder: baseline 3d2f72e2a, with separate scopes and storage.
use super::*;
mod admission;
mod arrays;
mod decode;
pub(super) mod serialization;

thread_local! {
    pub(crate) static LEGACY: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}
