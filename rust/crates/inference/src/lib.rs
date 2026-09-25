#![forbid(unsafe_code)]
#![doc = include_str!("../README.md")]

/// Generated customer protobuf contract shared by host and WebAssembly.
#[allow(missing_docs, unused_qualifications, clippy::all, clippy::pedantic)]
pub mod wire {
    include!(concat!(env!("OUT_DIR"), "/inference.customer.v1.rs"));
}

mod contract;
pub use contract::{
    MAXIMUM_EVALUATION_CANDIDATES, MAXIMUM_EVALUATION_CASES, MAXIMUM_EVALUATION_METRICS,
    MAXIMUM_EVALUATION_RESULTS, MAXIMUM_MESSAGE_BYTES, validate_customer_wire,
};

#[cfg(feature = "host")]
mod host;
#[cfg(feature = "host")]
pub use host::*;
