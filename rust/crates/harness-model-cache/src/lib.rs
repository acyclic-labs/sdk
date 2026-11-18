//! Provider-neutral model cache observations and residency transitions.

mod executor {
    pub mod model_cache;
}

pub use executor::model_cache::*;
