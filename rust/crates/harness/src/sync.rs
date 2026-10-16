#[path = "sync/primitives.rs"]
mod primitives;
pub(crate) use primitives::{Arc, Mutex, atomic};
