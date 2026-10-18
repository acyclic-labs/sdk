#![no_main]

pub use acyclic_fuzz_support as fuzz_support;

use bytes::BytesMut;
use rmcp::model::ServerJsonRpcMessage;
use std::sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
};

pub(crate) mod sync {
    include!("../../harness/src/sync/arc.rs");
    include!("../../harness/src/sync/atomic.rs");
}
mod mcp {
    pub(crate) use crate::sync;
}

// Compile the production codec, rather than maintaining a fuzz-only parser.
#[path = "../../harness/src/mcp/stdio/native/codec.rs"]
mod codec;

fuzz_support::fuzz_target!(|input: &[u8], fuzz_input| {
    if input.len() > 4096 {
        return;
    }
    for maximum in [1, 16, 128, 4096] {
        let bound = Arc::new(AtomicU32::new(maximum));
        let mut decoder = codec::BoundedCodec::<ServerJsonRpcMessage>::new(Arc::clone(&bound));
        let mut pending = BytesMut::new();
        fuzz_support::stream_round_trip!(fuzz_input, decoder, pending, |index| {
            if index == 1 {
                bound.store(4096, Ordering::Release);
            }
        });
    }
});
