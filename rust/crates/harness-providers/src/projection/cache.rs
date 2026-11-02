//! Hash the original wire serialization, without a second prompt walk or buffer.
use super::{wire::Body, wire::Message};
use acyclic_harness_core::{
    Result,
    executor::{ModelCacheBreakpoint, ModelCacheObservation, ModelCacheTtl},
    model::{ModelProfileBinding, ReasoningOrigin},
};
use acyclic_model_render::{Cache, Family};
use serde::{Serialize, ser::SerializeSeq};
use std::{cell::RefCell, io::Write};

struct Output {
    bytes: Vec<u8>,
    hash: blake3::Hasher,
    breakpoint: Option<ModelCacheBreakpoint>,
}

struct Writer(RefCell<Output>);

impl Write for &Writer {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let mut output = self.0.borrow_mut();
        output.bytes.extend_from_slice(bytes);
        output.hash.update(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

struct Messages<'a, 'body> {
    messages: &'a [Message<'body>],
    writer: &'a Writer,
}

impl Serialize for Messages<'_, '_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let mut sequence = serializer.serialize_seq(Some(self.messages.len()))?;
        for (index, message) in self.messages.iter().enumerate() {
            let parent_digest = (index + 1 == self.messages.len())
                .then(|| *self.writer.0.borrow().hash.finalize().as_bytes());
            sequence.serialize_element(message)?;
            if let Some(parent_digest) = parent_digest {
                let mut output = self.writer.0.borrow_mut();
                output.breakpoint = Some(ModelCacheBreakpoint {
                    digest: *output.hash.finalize().as_bytes(),
                    parent_digest,
                    messages: self.messages.len() as u64,
                });
            }
        }
        sequence.end()
    }
}

struct Capture<'a, 'body> {
    body: &'a Body<'body>,
    writer: &'a Writer,
}

impl Serialize for Capture<'_, '_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        self.body.serialize_with_messages(
            serializer,
            None,
            None::<&str>,
            Messages {
                messages: &self.body.messages,
                writer: self.writer,
            },
        )
    }
}

/// Generation bytes and fixed-size cache provenance produced together.
#[derive(Debug)]
pub(crate) struct Encoded {
    pub bytes: Vec<u8>,
    pub cache: ModelCacheObservation,
}

pub(crate) fn attach(metadata: &mut serde_json::Value, cache: ModelCacheObservation) -> Result<()> {
    let fields = metadata
        .as_object_mut()
        .ok_or_else(|| crate::invalid("terminal model metadata must be an object"))?;
    fields.insert(
        "cache".into(),
        serde_json::to_value(cache)
            .map_err(|_| crate::invalid("cache observation encoding failed"))?,
    );
    Ok(())
}

pub(crate) fn encode(
    body: &Body<'_>,
    adapter: Family,
    binding: Option<&ModelProfileBinding>,
    origin: &ReasoningOrigin,
) -> Result<Encoded> {
    let explicit = adapter == Family::Anthropic && binding.is_some();
    let mut hash = blake3::Hasher::new();
    hash.update(b"harness:projected-cache-prefix:v1\0");
    let writer = Writer(RefCell::new(Output {
        bytes: Vec::with_capacity(128),
        hash,
        breakpoint: None,
    }));
    Capture {
        body,
        writer: &writer,
    }
    .serialize(&mut serde_json::Serializer::new(&writer))
    .map_err(|_| crate::invalid("model request encoding failed"))?;
    let output = writer.0.into_inner();
    // No retention is inferred from a provider default. The admitted Anthropic
    // profile explicitly projects the one top-level ephemeral marker.
    let ttl = if explicit {
        ModelCacheTtl::Explicit {
            seconds: if binding.is_some_and(|binding| binding.cache() == Cache::Hour) {
                3600
            } else {
                300
            },
        }
    } else {
        ModelCacheTtl::ProviderManaged
    };
    Ok(Encoded {
        bytes: output.bytes,
        cache: ModelCacheObservation {
            version: 1,
            credential_scope: origin.credential_scope,
            profile_prefix_digest: binding.map(ModelProfileBinding::prefix_digest),
            ttl,
            breakpoint: output.breakpoint,
        },
    })
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use serde_json::json;

    proptest! {
        #[test]
        fn cache_capture_preserves_wire_bytes_and_sparse_prefix_edges(
            first in ".{0,1024}", last in ".{0,1024}", scope in any::<[u8; 32]>(),
        ) {
            // Generated request ASTs exercise serialization, not provider replies.
            let origin = ReasoningOrigin {
                provider: "generated".into(), model: "generated".into(),
                credential_scope: Some(scope),
            };
            let one = Body::fixture(json!({"model":"generated","input":[first]}), "input");
            let two = Body::fixture(json!({"model":"generated","input":[first,last]}), "input");
            let captured = encode(&two, Family::Responses, None, &origin).unwrap();
            let expected = serde_json::to_vec(&two).unwrap();
            prop_assert_eq!(&captured.bytes, &expected);
            let one = encode(&one, Family::Responses, None, &origin).unwrap();
            let breakpoint = captured.cache.breakpoint.unwrap();
            prop_assert_eq!(breakpoint.parent_digest, one.cache.breakpoint.unwrap().digest);
            prop_assert_eq!(breakpoint.messages, 2);
            let mut hash = blake3::Hasher::new();
            hash.update(b"harness:projected-cache-prefix:v1\0");
            hash.update(&expected[..expected.len() - 2]);
            prop_assert_eq!(breakpoint.digest, *hash.finalize().as_bytes());
            let changed = Body::fixture(json!({"model":"other","input":[first,last]}), "input");
            prop_assert_ne!(breakpoint.digest,
                encode(&changed, Family::Responses, None, &origin).unwrap().cache.breakpoint.unwrap().digest);
            let mut metadata = json!({});
            attach(&mut metadata, captured.cache).unwrap();
            let restored = ModelCacheObservation::reported(&metadata).unwrap();
            prop_assert_eq!(restored, captured.cache);
            let mut foreign = restored;
            foreign.credential_scope = Some(scope.map(|byte| byte ^ 1));
            prop_assert_ne!(foreign.key(), restored.key());
            foreign.credential_scope = None;
            prop_assert_eq!(foreign.key(), None);
            foreign.version = 2;
            prop_assert_eq!(foreign.key(), None);
            prop_assert_eq!(ModelCacheObservation::reported(&json!({"cache":foreign})), None);
            let implicit = encode(&two, Family::Chat, None, &origin).unwrap();
            prop_assert_eq!(&implicit.bytes, &expected);
            prop_assert_eq!(implicit.cache.ttl, ModelCacheTtl::ProviderManaged);
            prop_assert_eq!(implicit.cache.breakpoint, captured.cache.breakpoint);

            let original = acyclic_test_support::measure(|| { std::hint::black_box(serde_json::to_vec(&two).unwrap()); });
            let observed = acyclic_test_support::measure(|| { std::hint::black_box(encode(&two, Family::Responses, None, &origin).unwrap()); });
            prop_assert_eq!((observed.count_total, observed.bytes_total), (original.count_total, original.bytes_total));
            let copied = acyclic_test_support::measure(|| { std::hint::black_box(expected.clone()); });
            prop_assert!(copied.count_total > 0);
        }
    }
}
