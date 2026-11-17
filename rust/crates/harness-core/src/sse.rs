//! Runtime refusal classification over the shared data-event decoder.
pub use acyclic_harness_sse::{Decoder, Frame, decode, frames};

impl From<acyclic_harness_sse::Error> for crate::Error {
    fn from(error: acyclic_harness_sse::Error) -> Self {
        match error {
            acyclic_harness_sse::Error::Input { reason, native } => {
                #[cfg(not(target_arch = "wasm32"))]
                if native {
                    return Self::NativeBoundary {
                        code: crate::wire::ErrorCode::Invalid,
                        reason,
                    };
                }
                let _ = native;
                Self::Invalid(reason.into())
            }
            acyclic_harness_sse::Error::Interrupted { native } => Self::ModelUnavailable {
                failure: crate::ModelTransportFailure::Interrupted,
                excerpt: if native {
                    String::new()
                } else {
                    "truncated SSE event".into()
                },
            },
            #[cfg(not(target_arch = "wasm32"))]
            acyclic_harness_sse::Error::Native(error) => error.into(),
        }
    }
}
