//! Hash the original wire serialization, without a second prompt walk or buffer.
use super::{wire::Message, wire::Request};
use crate::ResolvedModel;
use acyclic_harness_core::{
    Result,
    executor::{ModelCacheBreakpoint, ModelCacheObservation, ModelCacheTtl},
    model::{ModelProfileBinding, RESEARCH_CAPTURE_ENABLED, ReasoningOrigin},
};
use acyclic_model_render::{Cache, Family, Field, Fields, Segment};
use std::{cell::RefCell, io::Write};

const DOMAIN: &[u8] = b"harness:projected-cache-prefix:v1\0";
const KEY_DOMAIN: &[u8] = b"harness:prompt-cache-key:v1\0";

/// The longest `prompt_cache_key` OpenAI accepts; longer keys are refused.
const MAX_KEY: usize = 64;
/// Digest bytes in a key: 192 bits, far beyond any collision concern.
const KEY_BYTES: usize = 24;
/// Key of an admitted profile's frozen prefix.
pub(super) const PROFILE_KEY: &str = "profile-v1:";
/// Key of the rendered header, without a profile.
const PREFIX_KEY: &str = "prefix-v1:";
const _: () = assert!(PROFILE_KEY.len() + 2 * KEY_BYTES <= MAX_KEY);
const _: () = assert!(PREFIX_KEY.len() + 2 * KEY_BYTES <= MAX_KEY);

/// The one `prompt_cache_key` formatter: a versioned tag and a digest prefix,
/// within the provider's length limit, in one allocation.
pub(super) fn key(tag: &str, digest: [u8; 32]) -> String {
    let hex = blake3::Hash::from(digest).to_hex();
    let hex = &hex.as_str()[..2 * KEY_BYTES];
    let mut key = String::with_capacity(tag.len() + hex.len());
    key.push_str(tag);
    key.push_str(hex);
    key
}

/// Requested retention: the admitted profile may ask for an hour, otherwise the
/// provider's five-minute default. Every marker in a request uses this one TTL,
/// so a longer entry never follows a shorter one.
fn hour(provider: &ResolvedModel) -> bool {
    provider
        .profile_binding()
        .is_some_and(|binding| binding.cache() == Cache::Hour)
}

/// The one `cache_control` value of a request.
pub(crate) fn control(provider: &ResolvedModel) -> serde_json::Value {
    let ttl = if hour(provider) { "1h" } else { "5m" };
    serde_json::json!({"type":"ephemeral","ttl":ttl})
}

/// Chat placement follows the original conversation across turns and retries.
/// Other protocols retain the admitted profile's frozen prefix, otherwise the
/// header the provider renders before any message (model, tools, instructions).
/// It is never derived from messages, so history paging, eviction and forks
/// keep it, and a mid-run delta (appended, never declared) cannot change it.
fn prefix_key(provider: &ResolvedModel, fields: &Fields) -> String {
    if provider.config.adapter == Family::Chat {
        return key(
            "conversation-v1:",
            *blake3::hash(provider.conversation.as_bytes()).as_bytes(),
        );
    }
    if let Some(binding) = provider.profile_binding() {
        return super::profile::cache_key(binding);
    }
    let mut hash = blake3::Hasher::new();
    hash.update(KEY_DOMAIN);
    let instructions = fields
        .get(Field::Instructions)
        .and_then(serde_json::Value::as_str);
    for value in [
        provider.model.name.as_bytes(),
        fields
            .tools()
            .map_or(&b""[..], |tools| tools.get().as_bytes()),
        instructions.unwrap_or("").as_bytes(),
    ] {
        hash.update(&(value.len() as u64).to_be_bytes());
        hash.update(value);
    }
    key(PREFIX_KEY, *hash.finalize().as_bytes())
}

/// Asks the provider to cache the stable prefix, in its own native form. Called
/// for every request, with or without an admitted profile.
/// - Anthropic: the system block carries the prefix breakpoint (tools and
///   system render first), and the top-level automatic breakpoint follows the
///   growing conversation. Two of the four slots; no marker ever touches a
///   message, so native thinking blocks stay byte-identical.
/// - Responses and Chat: `prompt_cache_key` routes each stable prefix together.
/// - Routed Chat (OpenRouter): both forms; the gateway applies the routed
///   model's own one.
/// - Gemini: implicit caching over the stable order.
pub(crate) fn project(provider: &ResolvedModel, fields: &mut Fields) {
    match provider.config.adapter {
        Family::Anthropic => fields.insert(Field::CacheControl, control(provider)),
        Family::Responses | Family::Chat => {
            fields.insert(
                Field::PromptCacheKey,
                serde_json::Value::String(prefix_key(provider, fields)),
            );
            if provider.config.adapter == Family::Chat
                && provider.config.prompt_cache == crate::PromptCache::Routed
            {
                fields.insert(Field::CacheControl, control(provider));
            }
        }
        Family::Gemini => {}
    }
}

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

/// Records the cache breakpoint around the final conversation element. The
/// renderer writes separators lazily, so at an element's first byte the hash
/// covers exactly the request through the previous element.
struct Marked<'a, 'body> {
    message: &'a Message<'body>,
    last: Option<u64>,
    writer: &'a Writer,
}

impl Segment for Marked<'_, '_> {
    fn selected(
        &self,
        target: &acyclic_model_render::Provenance,
    ) -> std::result::Result<bool, acyclic_model_render::RenderError> {
        self.message.selected(target)
    }
    fn write(
        &self,
        target: &acyclic_model_render::Provenance,
        instruction: Option<&str>,
        writer: &mut impl Write,
    ) -> std::result::Result<bool, acyclic_model_render::RenderError> {
        let parent_digest = self
            .last
            .map(|_| *self.writer.0.borrow().hash.finalize().as_bytes());
        let turn = self.message.write(target, instruction, writer)?;
        if let (Some(parent_digest), Some(messages)) = (parent_digest, self.last) {
            let mut output = self.writer.0.borrow_mut();
            output.breakpoint = Some(ModelCacheBreakpoint {
                digest: *output.hash.finalize().as_bytes(),
                parent_digest,
                messages,
            });
        }
        Ok(turn)
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
    body: &Request<'_>,
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
    let explicit = provider.config.adapter == Family::Anthropic
        || (provider.config.adapter == Family::Chat
            && provider.config.prompt_cache == crate::PromptCache::Routed);
    let mut hash = blake3::Hasher::new();
    hash.update(DOMAIN);
    for value in identity(provider) {
        hash.update(&(value.len() as u64).to_be_bytes());
        hash.update(value);
    }
    let writer = Writer(RefCell::new(Output {
        bytes: Vec::with_capacity(128),
        hash,
        breakpoint: None,
    }));
    let count = body.messages.len();
    let marked = body
        .messages
        .iter()
        .enumerate()
        .map(|(index, message)| Marked {
            message,
            last: (index + 1 == count).then_some(count as u64),
            writer: &writer,
        });
    body.render_segments(marked, &mut &writer)?;
    let output = writer.0.into_inner();
    // No retention is inferred from a provider default: only a projected
    // `cache_control` marker states one.
    let ttl = if explicit {
        ModelCacheTtl::Explicit {
            seconds: if hour(provider) { 3600 } else { 300 },
        }
    } else {
        ModelCacheTtl::ProviderManaged
    };
    Ok(Encoded {
        bytes: output.bytes,
        cache: Some(ModelCacheObservation {
            version: ModelCacheObservation::VERSION,
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

    #[tokio::test]
    async fn chat_conversations_keep_distinct_affinity_across_turns_and_credential_refresh() {
        use acyclic_harness_core::{
            OperationId,
            model::{ModelEvent, ModelProvider},
        };
        use futures::TryStreamExt as _;

        let (provider, transport) = crate::tests::provider(Family::Chat, 200);
        let (prepared, dispatch) = crate::tests::request(&provider).unwrap();
        let conversation = OperationId::new();
        let first = provider.bind_conversation(conversation).unwrap();
        let reopened = provider.bind_conversation(conversation).unwrap();
        let other = provider.bind_conversation(OperationId::new()).unwrap();
        for selected in [first, reopened, other] {
            let events: Vec<ModelEvent> = selected
                .generate(
                    &prepared,
                    acyclic_harness_core::model::ModelDispatch {
                        operation_id: OperationId::new(),
                        ..dispatch
                    },
                )
                .try_collect()
                .await
                .unwrap();
            assert!(
                events
                    .iter()
                    .any(|event| matches!(event, ModelEvent::Completed { .. }))
            );
        }
        let sent = transport.sent.lock().unwrap();
        let bodies: Vec<serde_json::Value> = sent
            .iter()
            .map(|request| serde_json::from_slice(&request.body).unwrap())
            .collect();
        assert_eq!(bodies.len(), 3);
        assert_eq!(bodies[0]["prompt_cache_key"], bodies[1]["prompt_cache_key"]);
        assert_ne!(bodies[0]["prompt_cache_key"], bodies[2]["prompt_cache_key"]);
        assert_eq!(bodies[0]["messages"], bodies[2]["messages"]);
        eprintln!(
            "affinity_keys={}",
            json!([bodies[0]["prompt_cache_key"], bodies[2]["prompt_cache_key"]])
        );
    }

    #[tokio::test]
    async fn native_chat_affinity_preserves_append_prefix_and_reported_cache_misses() {
        use acyclic_harness_core::model::{
            ModelContent, ModelDispatch, ModelEvent, ModelMessage, ModelRole, PreparedModelRequest,
        };
        use futures::TryStreamExt as _;

        let (provider, _) = crate::tests::provider(Family::Chat, 200);
        let (prepared, dispatch) = crate::tests::request(&provider).unwrap();
        let mut request = prepared.request().clone();
        request.messages[0].content = ModelContent::Text("stable instructions ".repeat(2_600));
        let origin = provider.reasoning_origin(provider.key.as_deref(), None);
        let mut header = None;
        let mut key = None;
        let mut previous_prefix = Vec::new();
        for step in 0..3 {
            if step > 0 {
                request.messages.extend([
                    ModelMessage {
                        role: ModelRole::Assistant,
                        content: ModelContent::Text("answer".into()),
                    },
                    ModelMessage {
                        role: ModelRole::User,
                        content: ModelContent::Text("continue".into()),
                    },
                ]);
            }
            let prepared =
                PreparedModelRequest::prepare(request.clone(), Default::default(), None).unwrap();
            let names = crate::tool_names::Names::new(prepared.request());
            let body = super::super::body_named(
                &provider,
                prepared.request(),
                &names,
                &origin,
                &mut crate::files::FileText::default(),
            )
            .unwrap();
            let encoded = encode(&body, &provider, &origin).unwrap();
            let actual: serde_json::Value = serde_json::from_slice(&encoded.bytes).unwrap();
            let affinity = actual["prompt_cache_key"]
                .as_str()
                .expect("native Chat needs an affinity key");
            assert!(!affinity.is_empty());
            assert_eq!(
                key.get_or_insert_with(|| affinity.to_owned()).as_str(),
                affinity
            );
            let current_header = body.header().unwrap();
            assert_eq!(
                header
                    .get_or_insert_with(|| current_header.bytes().to_vec())
                    .as_slice(),
                current_header.bytes()
            );
            assert!(
                encoded.bytes.starts_with(&previous_prefix),
                "earlier wire bytes changed at step {step}"
            );
            previous_prefix =
                encoded.bytes[..encoded.bytes.len() - body.tail().unwrap().bytes().len()].to_vec();
            assert!(actual.get("cache_control").is_none());

            // Routing is a request, never evidence of a hit. Preserve a real
            // endpoint's zero cached-token observation without inventing reuse.
            let reply = crate::fixtures::CHAT.replace(
                "\"prompt_tokens\":5",
                "\"prompt_tokens\":5,\"prompt_tokens_details\":{\"cached_tokens\":0}",
            );
            let events: Vec<_> = crate::stream::events(
                Family::Chat,
                crate::tests::chunks(reply, 7),
                4096,
                ModelDispatch {
                    step,
                    request_digest: prepared.request_digest(),
                    ..dispatch
                },
            )
            .try_collect()
            .await
            .unwrap();
            let metadata = events
                .iter()
                .find_map(|event| match event {
                    ModelEvent::Completed { metadata } => Some(metadata),
                    _ => None,
                })
                .unwrap();
            assert_eq!(
                metadata["usage"]["prompt_tokens_details"]["cached_tokens"],
                0
            );
        }
    }

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
            let one = Request::fixture(json!({"model":"generated","input":[first]}), "input");
            let two = Request::fixture(json!({"model":"generated","input":[first,last]}), "input");
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
            let changed = Request::fixture(json!({"model":"other","input":[first,last]}), "input");
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
                let body = super::super::body_named(provider, request.request(), &names,
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
                    let written = ModelCacheResidency::observe(key, None, [1; 16], 0,
                        ModelCacheAccess::Write(ModelCacheTtl::Explicit { seconds: 300 })).unwrap();
                    for foreign in [second.cache.unwrap().key().unwrap(), endpoint.cache.unwrap().key().unwrap()] {
                        prop_assert_ne!(key, foreign);
                        prop_assert_eq!(ModelCacheResidency::observe(foreign, Some(written), [1; 16], 1,
                            ModelCacheAccess::Miss), Err(ModelCacheError::KeyMismatch));
                    }
                } else {
                    prop_assert_eq!((first.cache, second.cache, endpoint.cache), (None, None, None));
                }
            }
        }
    }
}
