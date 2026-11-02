//! Hash the original wire serialization, without a second prompt walk or buffer.
use super::wire::Projection;
use crate::ResolvedModel;
use acyclic_harness_core::{
    Result,
    executor::{ModelCacheBreakpoint, ModelCacheObservation, ModelCacheTtl},
    model::{ModelProfileBinding, RESEARCH_CAPTURE_ENABLED, ReasoningOrigin},
};
use acyclic_model_render::{Cache, Family};
use std::io::Write;

const DOMAIN: &[u8] = b"harness:projected-cache-prefix:v1\0";

// This tuple determines the endpoint, including model-in-URL protocols, without
// building or parsing a second URL. Length framing separates every component.
fn identity(provider: &ResolvedModel) -> [&[u8]; 3] {
    [
        crate::stream::family(provider.config.adapter).as_bytes(),
        provider.config.base_url.as_bytes(),
        provider.model.name.as_bytes(),
    ]
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn hashed_bytes(provider: &ResolvedModel, input_bytes: usize) -> u64 {
    DOMAIN.len() as u64
        + input_bytes as u64
        + identity(provider)
            .into_iter()
            .map(|value| 8 + value.len() as u64)
            .sum::<u64>()
}

struct Output {
    bytes: Vec<u8>,
    hash: blake3::Hasher,
    breakpoint: Option<ModelCacheBreakpoint>,
}

impl Write for Output {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.bytes.extend_from_slice(bytes);
        self.hash.update(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

struct Breakpoint {
    messages: usize,
    parent: Option<[u8; 32]>,
}
impl acyclic_model_render::RenderObserver<Output> for Breakpoint {
    fn before(&mut self, index: usize, output: &mut Output) {
        if index + 1 == self.messages {
            self.parent = Some(*output.hash.finalize().as_bytes());
        }
    }
    fn after(&mut self, index: usize, emitted: bool, output: &mut Output) {
        if emitted && index + 1 == self.messages {
            output.breakpoint = self.parent.map(|parent_digest| ModelCacheBreakpoint {
                digest: *output.hash.finalize().as_bytes(),
                parent_digest,
                messages: self.messages as u64,
            });
        }
    }
}

/// Generation bytes and fixed-size cache provenance produced together.
#[derive(Debug)]
pub(crate) struct Encoded {
    pub bytes: Vec<u8>,
    pub cache: Option<ModelCacheObservation>,
}

pub(crate) fn attach(
    metadata: &mut serde_json::Value,
    cache: Option<ModelCacheObservation>,
) -> Result<()> {
    if !RESEARCH_CAPTURE_ENABLED {
        return Ok(());
    }
    let Some(cache) = cache else { return Ok(()) };
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
    body: &Projection<'_>,
    provider: &ResolvedModel,
    origin: &ReasoningOrigin,
) -> Result<Encoded> {
    if !RESEARCH_CAPTURE_ENABLED {
        return Ok(Encoded {
            bytes: body.encode()?,
            cache: None,
        });
    }
    let binding = provider.profile_binding();
    let explicit = provider.config.adapter == Family::Anthropic && binding.is_some();
    let mut hash = blake3::Hasher::new();
    hash.update(DOMAIN);
    for value in identity(provider) {
        hash.update(&(value.len() as u64).to_be_bytes());
        hash.update(value);
    }
    let mut output = Output {
        bytes: Vec::with_capacity(128),
        hash,
        breakpoint: None,
    };
    let tools = body.tools();
    acyclic_model_render::RequestView {
        target: &body.target,
        fields: &body.fields,
        tools: tools.as_ref(),
        include: None,
        model: None::<&str>,
    }
    .write_observed(
        &body.messages,
        &mut output,
        &mut Breakpoint {
            messages: body.messages.len(),
            parent: None,
        },
    )
    .map_err(|error| crate::invalid(format!("model request encoding: {error}")))?;
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
        cache: Some(ModelCacheObservation {
            version: 1,
            credential_scope: origin.credential_scope,
            profile_prefix_digest: binding.map(ModelProfileBinding::prefix_digest),
            ttl,
            breakpoint: output.breakpoint,
        }),
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
            let (provider, _) = crate::tests::provider(Family::Responses, 200);
            let one = Projection::fixture(json!({"model":"generated","input":[first]}), "input");
            let two = Projection::fixture(json!({"model":"generated","input":[first,last]}), "input");
            let captured = encode(&two, &provider, &origin).unwrap();
            let expected = two.encode().unwrap();
            prop_assert_eq!(&captured.bytes, &expected);

            let original = acyclic_test_support::measure(|| { std::hint::black_box(two.encode().unwrap()); });
            let observed = acyclic_test_support::measure(|| { std::hint::black_box(encode(&two, &provider, &origin).unwrap()); });
            prop_assert_eq!((observed.count_total, observed.bytes_total), (original.count_total, original.bytes_total));
            let copied = acyclic_test_support::measure(|| { std::hint::black_box(expected.clone()); });
            prop_assert!(copied.count_total > 0);
            let mut metadata = json!({"usage":{"input_tokens":17}});
            let attached = acyclic_test_support::measure(|| { attach(&mut metadata, captured.cache).unwrap(); });
            prop_assert_eq!(captured.cache.is_some(), RESEARCH_CAPTURE_ENABLED);
            if !RESEARCH_CAPTURE_ENABLED {
                prop_assert_eq!(metadata, json!({"usage":{"input_tokens":17}}));
                prop_assert_eq!((attached.count_total, attached.bytes_total), (0, 0));
                return Ok(());
            }
            prop_assert!(attached.count_total > 0);

            let cache = captured.cache.unwrap();
            let one = encode(&one, &provider, &origin).unwrap();
            let breakpoint = cache.breakpoint.unwrap();
            prop_assert_eq!(breakpoint.parent_digest, one.cache.unwrap().breakpoint.unwrap().digest);
            prop_assert_eq!(breakpoint.messages, 2);
            let mut hash = blake3::Hasher::new();
            hash.update(DOMAIN);
            for value in identity(&provider) {
                hash.update(&(value.len() as u64).to_be_bytes());
                hash.update(value);
            }
            hash.update(&expected[..expected.len() - 2]);
            prop_assert_eq!(breakpoint.digest, *hash.finalize().as_bytes());
            let changed = Projection::fixture(json!({"model":"other","input":[first,last]}), "input");
            prop_assert_ne!(breakpoint.digest,
                encode(&changed, &provider, &origin).unwrap().cache.unwrap().breakpoint.unwrap().digest);
            let restored = ModelCacheObservation::reported(&metadata).unwrap();
            prop_assert_eq!(restored, cache);
            let mut foreign = restored;
            foreign.credential_scope = Some(scope.map(|byte| byte ^ 1));
            prop_assert_ne!(foreign.key(), restored.key());
            foreign.credential_scope = None;
            prop_assert_eq!(foreign.key(), None);
            foreign.version = 2;
            prop_assert_eq!(foreign.key(), None);
            prop_assert_eq!(ModelCacheObservation::reported(&json!({"cache":foreign})), None);
            let (chat, _) = crate::tests::provider(Family::Chat, 200);
            let implicit = encode(&two, &chat, &origin).unwrap();
            prop_assert_eq!(&implicit.bytes, &expected);
            prop_assert_eq!(implicit.cache.unwrap().ttl, ModelCacheTtl::ProviderManaged);
            prop_assert_ne!(implicit.cache.unwrap().key(), cache.key());
        }

        #[test]
        fn cache_keys_isolate_gemini_models_and_all_resolved_endpoints(name in "[a-z]{1,32}") {
            use acyclic_harness_core::executor::{ModelCacheAccess, ModelCacheError, ModelCacheResidency};

            fn projected(provider: &ResolvedModel) -> Encoded {
                let (request, _) = crate::tests::request(provider).unwrap();
                let names = crate::tool_names::Names::new(request.request());
                let origin = provider.reasoning_origin(provider.key.as_deref(), None);
                let body = super::super::project(provider, request.request(), &names,
                    &origin, &mut crate::files::FileText::default()).unwrap();
                encode(&body, provider, &origin).unwrap()
            }

            for family in [Family::Gemini, Family::Chat, Family::Responses, Family::Anthropic] {
                let (mut provider, _) = crate::tests::provider(family, 200);
                provider.model.name.clone_from(&name);
                let first = projected(&provider);
                let repeated = projected(&provider);
                prop_assert_eq!(first.cache, repeated.cache);
                provider.model.name.push_str("-other");
                let second = projected(&provider);
                if family == Family::Gemini {
                    // The actual Gemini projection omits the model from JSON.
                    prop_assert_eq!(&first.bytes, &second.bytes);
                }
                provider.model.name.clone_from(&name);
                std::sync::Arc::make_mut(&mut provider.config).base_url.push_str("/other");
                let endpoint = projected(&provider);
                prop_assert_eq!(&first.bytes, &endpoint.bytes);
                if RESEARCH_CAPTURE_ENABLED {
                    prop_assert_eq!(first.cache.unwrap().credential_scope, second.cache.unwrap().credential_scope);
                    prop_assert_ne!(first.cache.unwrap().breakpoint, endpoint.cache.unwrap().breakpoint);
                    let key = first.cache.unwrap().key().unwrap();
                    let written = ModelCacheResidency::observe(key, None, 0,
                        ModelCacheAccess::Write(ModelCacheTtl::Explicit { seconds: 300 })).unwrap();
                    for foreign in [second.cache.unwrap().key().unwrap(), endpoint.cache.unwrap().key().unwrap()] {
                        prop_assert_ne!(key, foreign);
                        prop_assert_eq!(ModelCacheResidency::observe(foreign, Some(written), 1,
                            ModelCacheAccess::Miss), Err(ModelCacheError::KeyMismatch));
                    }
                } else {
                    prop_assert_eq!((first.cache, second.cache, endpoint.cache), (None, None, None));
                }
            }
        }
    }
}
