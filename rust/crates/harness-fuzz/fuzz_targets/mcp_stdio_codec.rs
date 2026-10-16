#![no_main]

use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;
use rmcp::model::ServerJsonRpcMessage;
use std::sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
};
use tokio_util::codec::{Decoder as _, Encoder as _};

#[path = "../../harness/src/sync/primitives.rs"]
pub(crate) mod sync;
mod mcp {
    pub(crate) use crate::sync;
}

// Compile the production codec, rather than maintaining a fuzz-only parser.
#[path = "../../harness/src/mcp/stdio/native/codec.rs"]
mod codec;

fuzz_target!(|input: &[u8]| {
    if input.len() > 4096 {
        return;
    }
    for maximum in [1, 16, 128, 4096] {
        let bound = Arc::new(AtomicU32::new(maximum));
        let mut decoder = codec::BoundedCodec::<ServerJsonRpcMessage>::new(Arc::clone(&bound));
        let mut pending = BytesMut::new();
        for (index, chunk) in input.chunks(17).enumerate() {
            if index == 1 {
                bound.store(4096, Ordering::Release);
            }
            pending.extend_from_slice(chunk);
            loop {
                match decoder.decode(&mut pending) {
                    Ok(Some(value)) => {
                        let expected = serde_json::to_value(&value).unwrap();
                        let mut output = BytesMut::new();
                        if decoder.encode(value, &mut output).is_ok() {
                            let restored = decoder.decode(&mut output).unwrap().unwrap();
                            assert_eq!(serde_json::to_value(restored).unwrap(), expected);
                        } else {
                            assert!(output.is_empty());
                        }
                    }
                    Ok(None) => break,
                    Err(_) => {
                        pending.clear();
                        break;
                    }
                }
            }
        }
        let _ = decoder.decode_eof(&mut pending);
    }
});
