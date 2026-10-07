#![forbid(unsafe_code)]
#![doc = include_str!("../README.md")]

/// Transport-independent contract crate, including the sole generated wire
/// type universe and shared validation implementation.
pub use acyclic_inference_contract as contract;
#[cfg(feature = "host")]
#[allow(missing_docs, unused_qualifications, clippy::all, clippy::pedantic)]
pub mod wire {
    pub use acyclic_inference_contract::wire::*;
    include!(concat!(env!("OUT_DIR"), "/inference.customer.v1.rs"));
}
#[cfg(not(feature = "host"))]
pub use acyclic_inference_contract::wire;
#[cfg(feature = "http-codec")]
pub mod http_codec;
pub use contract::{
    DESCRIPTOR, MAXIMUM_EVALUATION_CANDIDATES, MAXIMUM_EVALUATION_CASES,
    MAXIMUM_EVALUATION_METRICS, MAXIMUM_EVALUATION_RESULTS, MAXIMUM_HTTP_JSON_BYTES,
    MAXIMUM_MESSAGE_BYTES, WatchRunState, validate_customer_wire, watch_run_start_state_wire,
    watch_run_start_wire,
};

#[cfg(feature = "host")]
mod host;
#[cfg(feature = "host")]
pub use host::*;
