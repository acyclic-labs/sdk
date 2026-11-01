//! Projection accepts only the sealed result of the profile file resolver.
use crate::{Adapter, invalid};
use acyclic_harness_core::{Result, model::ModelProfileBinding};
use acyclic_model_render::{Cache, Effort, Reasoning};
use serde_json::{Map, Value, json};

fn prefix_cache_key(prefix: [u8; 32]) -> String {
    let digest = blake3::Hash::from(prefix).to_hex();
    let mut key = String::with_capacity("profile-v1:".len() + digest.len());
    key.push_str("profile-v1:");
    key.push_str(digest.as_str());
    key
}

pub(crate) fn validate(adapter: Adapter, binding: &ModelProfileBinding) -> Result<()> {
    if binding.fixed_effort().is_some() && adapter != Adapter::Responses {
        return Err(invalid(
            "fixed role effort requires an epoch on this protocol",
        ));
    }
    if binding.fixed_effort().is_some() && binding.reasoning() == Reasoning::Disabled {
        return Err(invalid("a role cannot enable disabled reasoning"));
    }
    if !matches!(
        (adapter, binding.cache()),
        (_, Cache::Automatic) | (Adapter::Anthropic, Cache::Ephemeral | Cache::Hour)
    ) {
        return Err(invalid(
            "cache strategy is not admitted by the selected protocol",
        ));
    }
    let admitted = match (adapter, binding.reasoning()) {
        (_, Reasoning::Disabled) => true,
        (Adapter::Responses | Adapter::Chat, Reasoning::Effort(effort))
        | (Adapter::Anthropic, Reasoning::Adaptive(effort))
        | (Adapter::Gemini, Reasoning::Level(effort)) => effort != Effort::Adaptive,
        _ => false,
    };
    if !admitted {
        return Err(invalid(
            "reasoning baseline is not admitted by the selected protocol",
        ));
    }
    Ok(())
}

pub(super) fn project(
    adapter: Adapter,
    binding: &ModelProfileBinding,
    fields: &mut Map<String, Value>,
) {
    match adapter {
        Adapter::Responses => {
            // Roles share this key because their directives and restrictions
            // append to the conversation instead of replacing its prefix.
            fields.insert(
                "prompt_cache_key".into(),
                Value::String(prefix_cache_key(binding.prefix_digest())),
            );
        }
        Adapter::Anthropic => {
            // The one top-level automatic breakpoint consumes one slot. No
            // marker is injected into native thinking or empty content blocks.
            let ttl = if binding.cache() == Cache::Hour {
                "1h"
            } else {
                "5m"
            };
            fields.insert(
                "cache_control".into(),
                json!({"type":"ephemeral","ttl":ttl}),
            );
        }
        Adapter::Chat | Adapter::Gemini => {}
    }
    match (adapter, binding.reasoning()) {
        (Adapter::Responses, Reasoning::Effort(effort)) => {
            fields.insert(
                "reasoning".into(),
                json!({"effort":effort,"summary":"auto"}),
            );
        }
        (Adapter::Chat, Reasoning::Effort(effort)) => {
            fields.insert("reasoning_effort".into(), json!(effort));
        }
        (Adapter::Anthropic, Reasoning::Adaptive(effort)) => {
            fields.insert(
                "thinking".into(),
                json!({"type":"adaptive","display":"summarized"}),
            );
            fields.insert("output_config".into(), json!({"effort":effort}));
        }
        (Adapter::Gemini, Reasoning::Level(effort)) => {
            // This object was constructed by this same projector, never supplied
            // by a caller. The output reservation remains on its original field.
            if let Some(Value::Object(config)) = fields.get_mut("generationConfig") {
                config.insert(
                    "thinkingConfig".into(),
                    json!({"thinkingLevel":effort,"includeThoughts":true}),
                );
            }
        }
        _ => {}
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use acyclic_harness_core::{
        approvals::ApprovalPolicy,
        model::{ModelProvider, PreparedModelRequest},
        profile::{AgentOverride, ModelProfile, ProjectLayer, RoleFile, resolve},
    };

    // Exercise the same protocol projection and encoding as dispatch, without a
    // transport or a second Value projection inside the measured region.
    fn measured_projection(
        provider: &crate::ResolvedModel,
        prepared: &PreparedModelRequest,
        phase: &str,
    ) -> Result<(Value, Vec<u8>)> {
        let mut encoded = None;
        let started = std::time::Instant::now();
        let allocation = acyclic_test_support::measure(|| {
            encoded = Some((|| {
                let request = prepared.request();
                let names = crate::tool_names::Names::new(request);
                let origin = provider.reasoning_origin(provider.key.as_deref(), None);
                let mut files = crate::files::FileText::default();
                let projected =
                    crate::projection::body_named(provider, request, &names, &origin, &mut files)?;
                serde_json::to_vec(&projected)
                    .map_err(|_| invalid("cannot encode measured profile projection"))
            })());
        });
        let elapsed = started.elapsed().as_nanos();
        let encoded = encoded.unwrap()?;
        let binding = provider.profile_binding().unwrap();
        println!(
            "{}",
            json!({
                "version": 1,
                "task": "admitted-profile-projection",
                "input_kind": "generated_profile_and_request",
                "phase": phase,
                "adapter": provider.config.adapter,
                "model": provider.model(),
                "prefix_digest": binding.prefix_digest(),
                "prepared_request": std::str::from_utf8(prepared.bytes()).unwrap(),
                "request": std::str::from_utf8(&encoded).unwrap(),
                "request_bytes": encoded.len(),
                "projection_elapsed_ns": elapsed,
                "allocation_count": allocation.count_total,
                "allocated_bytes": allocation.bytes_total,
                "peak_bytes": allocation.bytes_max,
                "model_calls": 0,
                "tokens": Value::Null,
                "cache_hits": Value::Null,
                "tool_execution": Value::Null,
                "terminal_quality": Value::Null,
            })
        );
        let decoded = serde_json::from_slice(&encoded)
            .map_err(|_| invalid("cannot decode measured projection"))?;
        Ok((decoded, encoded))
    }

    #[test]
    fn gemini_levels_follow_the_exact_retained_catalog_member() -> Result<()> {
        use crate::{Credentials, Registry};
        use std::collections::BTreeMap;

        let (_, transport) = crate::tests::provider(Adapter::Gemini, 200);
        let mut registry = Registry::with_transport(
            Credentials::explicit(BTreeMap::from([("gemini".into(), "fixture".into())])),
            transport.clone(),
        );
        let mut catalog = registry.catalog()["gemini"].as_ref().clone();
        // Alias support is a catalog fact, rather than a model-name heuristic.
        let advertised = catalog.models["gemini-4-argon"];
        catalog.models.insert("opaque-alias".into(), advertised);
        let mut unknown = serde_json::to_value(advertised).unwrap();
        unknown.as_object_mut().unwrap().remove("thinking_levels");
        catalog.models.insert(
            "gemini-future".into(),
            serde_json::from_value(unknown).unwrap(),
        );
        registry.register("gemini".into(), catalog.clone())?;
        let project = ProjectLayer::parse("---\nversion: 1\n---\n")?;
        let approvals = ApprovalPolicy::default().bind()?;
        let agent = AgentOverride::parse("---\nversion: 1\n---\n", &approvals)?;
        for (name, levels) in [
            ("gemini-2.5-pro", false),
            ("gemini-2.5-flash", false),
            ("gemini-4-argon", true),
            ("opaque-alias", true),
            ("gemini-future", false),
        ] {
            let key = format!("gemini/{name}");
            let role = RoleFile::parse(&format!(
                "---\nversion: 1\nname: worker\nmodel: {key}\ncontext: fork\nmay_spawn: []\nmax_output_tokens: 64\n---\n"
            ))?;
            if let Ok((family, source)) = acyclic_harness_core::profile::models::sources(&key) {
                let selected = registry.resolve(&key)?;
                let model = ModelProfile::parse(family, source, selected.capacity, "renderer-v1")?;
                let profile = resolve(&model, &role, &project, &agent)?;
                let provider = selected.with_profile(&profile)?;
                let (request, _) = crate::tests::request(&provider)?;
                assert!(
                    crate::projection::body(&provider, request.request())?
                        .pointer("/generationConfig/thinkingConfig")
                        .is_none()
                );
            }
            for effort in [None, Some("low"), Some("medium"), Some("high")] {
                let reasoning = effort.map_or_else(
                    || "{mode: disabled}".into(),
                    |effort| format!("{{mode: level, effort: {effort}}}"),
                );
                let model = ModelProfile::parse(
                    &format!(
                        "---\nversion: 1\ntools: []\nreasoning: {reasoning}\ncache: automatic\nbudget_tokens: 4096\n---\n"
                    ),
                    &format!("---\nversion: 1\nkey: {key}\n---\n"),
                    advertised.capacity,
                    "renderer-v1",
                )?;
                let profile = resolve(&model, &role, &project, &agent)?;
                let selected = registry.resolve(&key)?;
                let original = selected.model().clone();
                let provider = selected.with_profile(&profile);
                if !levels && effort.is_some() {
                    assert!(matches!(
                        provider,
                        Err(acyclic_harness_core::Error::Invalid(_))
                    ));
                    continue;
                }
                let provider = provider?;
                let (request, _) = crate::tests::request(&provider)?;
                let body = crate::projection::body(&provider, request.request())?;
                assert_eq!(
                    body.pointer("/generationConfig/thinkingConfig"),
                    effort
                        .map(|effort| json!({"thinkingLevel":effort,"includeThoughts":true}))
                        .as_ref()
                );
                if name == "opaque-alias" && effort == Some("high") {
                    // Changing future admission cannot rewrite a retained member.
                    catalog.models.get_mut(name).unwrap().thinking_levels = false;
                    registry.register("gemini".into(), catalog.clone())?;
                    let next = registry.resolve(&key)?;
                    assert_ne!(next.model(), &original);
                    assert!(next.with_profile(&profile).is_err());
                    assert_eq!(crate::projection::body(&provider, request.request())?, body);
                }
            }
        }
        assert!(transport.sent.lock().unwrap().is_empty());
        Ok(())
    }

    #[test]
    fn parsed_profiles_bind_original_selection_capacity_and_protocol_controls() -> Result<()> {
        for fragments in [
            vec![],
            vec![String::new()],
            vec!["λ\n".into()],
            vec![" x ".into(), "\r\n".into(), "tail".into()],
        ] {
            for separator in ["", "\n", "\n\n"] {
                let expected = fragments.join(separator);
                let mut owned = Some(fragments.clone());
                let original = owned.as_ref().unwrap().first().map(|text| text.as_ptr());
                let mut joined = None;
                let cost = acyclic_test_support::measure(|| {
                    joined = Some(crate::projection::text::join(
                        owned.take().unwrap(),
                        separator,
                    ));
                });
                let joined = joined.unwrap();
                assert_eq!(joined, expected);
                if fragments.len() <= 1 {
                    assert_eq!(cost.count_total, 0);
                }
                if fragments.len() == 1 {
                    assert_eq!(Some(joined.as_ptr()), original);
                    if !joined.is_empty() {
                        let mut copied = None;
                        let copied_cost = acyclic_test_support::measure(|| {
                            copied = Some(fragments.join(separator));
                        });
                        assert_eq!(copied.unwrap(), joined);
                        assert!(copied_cost.bytes_total > cost.bytes_total);
                    }
                }
            }
        }
        let empty = "---\nversion: 1\n---\n";
        let project = ProjectLayer::parse(empty)?;
        let approvals = ApprovalPolicy::default().bind()?;
        let agent = AgentOverride::parse(empty, &approvals)?;
        let role = RoleFile::parse(
            "---\nversion: 1\nname: worker\nmodel: fixture/model\ncontext: fork\nmay_spawn: []\nmax_output_tokens: 64\n---\n",
        )?;
        for (adapter, reasoning, pointer) in [
            (Adapter::Responses, "effort", "/reasoning/effort"),
            (Adapter::Chat, "effort", "/reasoning_effort"),
            (Adapter::Anthropic, "adaptive", "/output_config/effort"),
            (
                Adapter::Gemini,
                "level",
                "/generationConfig/thinkingConfig/thinkingLevel",
            ),
        ] {
            let (provider, _) = crate::tests::provider(adapter, 200);
            let original = provider.model().clone();
            let family = format!(
                "---\nversion: 1\ntools: []\nreasoning: {{mode: {reasoning}, effort: high}}\ncache: automatic\nbudget_tokens: 4096\n---\n"
            );
            let model = ModelProfile::parse(
                &family,
                "---\nversion: 1\nkey: fixture/model\n---\n",
                provider.capacity,
                "renderer-v1",
            )?;
            let profile = resolve(&model, &role, &project, &agent)?;
            let mut foreign = original.clone();
            foreign.name.push_str("-foreign");
            assert!(profile.bind_model(&foreign, provider.capacity).is_err());
            let mut foreign_capacity = provider.capacity;
            foreign_capacity.context_tokens += 1;
            assert!(profile.bind_model(&original, foreign_capacity).is_err());
            let wrong_protocol = {
                let wrong_family = family.replace(&format!("mode: {reasoning}"), "mode: level");
                let wrong_model = ModelProfile::parse(
                    &wrong_family,
                    "---\nversion: 1\nkey: fixture/model\n---\n",
                    provider.capacity,
                    "renderer-v1",
                )?;
                resolve(&wrong_model, &role, &project, &agent)?
                    .bind_model(&original, provider.capacity)?
            };
            assert_eq!(
                validate(adapter, &wrong_protocol).is_ok(),
                adapter == Adapter::Gemini
            );
            let provider = provider.with_profile(&profile)?;
            let (mut rotated, _) = crate::tests::provider(adapter, 200);
            rotated.model.revision.push_str("-rotated");
            let rotated = rotated.with_profile(&profile)?;
            assert_ne!(provider.model(), rotated.model());
            assert!(rotated.context_capacity(provider.model()).is_err());
            assert!(provider.context_capacity(&original).is_err());
            assert_eq!(
                provider.context_capacity(provider.model())?.context_tokens,
                4096
            );
            let (prepared, _) = crate::tests::request(&provider)?;
            let (body, first_bytes) = measured_projection(&provider, &prepared, "first")?;
            let (_, repeated) = measured_projection(&provider, &prepared, "repeat")?;
            assert_eq!(repeated, first_bytes);
            let retained: acyclic_harness_core::model::ModelRequest =
                serde_json::from_slice(prepared.bytes()).unwrap();
            assert_eq!(&retained, prepared.request());
            // Restoring a deleted options map must refuse at the shared request
            // boundary, rather than silently retaining the same cache identity
            // while discarding configuration the caller expected to apply.
            let mut restored = serde_json::to_value(&retained).unwrap();
            restored["options"] = json!({
                "max_output_tokens": 65,
                "tools": [],
                "reasoning_effort": "low",
            });
            let restored = serde_json::to_vec(&restored).unwrap();
            let mut admitted = None;
            let cost = acyclic_test_support::measure(|| {
                admitted = Some(serde_json::from_slice::<
                    acyclic_harness_core::model::ModelRequest,
                >(&restored));
            });
            let error = admitted.unwrap().unwrap_err();
            assert!(error.is_data());
            println!(
                "{}",
                json!({
                    "version": 1,
                    "task": "removed-model-configuration",
                    "input_kind": "generated_profile_and_request",
                    "phase": "restored_options",
                    "adapter": adapter,
                    "model": provider.model(),
                    "prefix_digest": profile.prefix_digest(),
                    "prepared_request": std::str::from_utf8(prepared.bytes()).unwrap(),
                    "restored_request": std::str::from_utf8(&restored).unwrap(),
                    "refusal": error.to_string(),
                    "allocation_count": cost.count_total,
                    "allocated_bytes": cost.bytes_total,
                    "peak_bytes": cost.bytes_max,
                    "model_calls": 0,
                    "tokens": Value::Null,
                    "cache_hits": Value::Null,
                    "tool_execution": Value::Null,
                    "terminal_quality": Value::Null,
                })
            );
            assert_eq!(body.pointer(pointer), Some(&json!("high")));
            if matches!(adapter, Adapter::Anthropic | Adapter::Gemini) {
                let mut smaller = None;
                for repetitions in [8192, 16384] {
                    let header = "λ\n".repeat(repetitions);
                    let mut request = prepared.request().clone();
                    request.messages[0].content =
                        acyclic_harness_core::model::ModelContent::Text(header.clone());
                    let request = PreparedModelRequest::prepare(request, Default::default(), None)?;
                    let names = crate::tool_names::Names::new(request.request());
                    let origin = provider.reasoning_origin(provider.key.as_deref(), None);
                    let mut projected = None;
                    let allocation = acyclic_test_support::measure(|| {
                        projected = Some(crate::projection::body_named(
                            &provider,
                            request.request(),
                            &names,
                            &origin,
                            &mut crate::files::FileText::default(),
                        ));
                    });
                    let projected = projected.unwrap()?;
                    let mut expected = body.clone();
                    if adapter == Adapter::Anthropic {
                        expected["system"] = json!(header);
                    } else {
                        expected["systemInstruction"]["parts"][0]["text"] = json!(header);
                    }
                    assert_eq!(serde_json::to_value(&projected).unwrap(), expected);
                    let mut detached = None;
                    let copied = acyclic_test_support::measure(|| {
                        detached = Some(projected.fields.clone());
                    });
                    assert_eq!(detached.unwrap(), projected.fields);
                    if let Some((bytes, allocated, copied_bytes)) = smaller {
                        let growth = (header.len() - bytes) as u64;
                        // One owned copy is required by the projected JSON.
                        // A restored join/JSON clone regresses this byte oracle.
                        assert_eq!(allocation.bytes_total - allocated, growth);
                        assert_eq!(copied.bytes_total - copied_bytes, growth);
                        assert_ne!(
                            allocation.bytes_total - allocated + copied.bytes_total - copied_bytes,
                            growth
                        );
                    }
                    smaller = Some((header.len(), allocation.bytes_total, copied.bytes_total));
                    println!(
                        "{}",
                        json!({
                            "version": 1,
                            "task": "system-text-projection",
                            "input_kind": "generated_profile_and_request",
                            "adapter": adapter,
                            "model": provider.model(),
                            "prefix_digest": profile.prefix_digest(),
                            "prepared_request": std::str::from_utf8(request.bytes()).unwrap(),
                            "request": serde_json::to_string(&projected).unwrap(),
                            "system_bytes": header.len(),
                            "allocation_count": allocation.count_total,
                            "allocated_bytes": allocation.bytes_total,
                            "peak_bytes": allocation.bytes_max,
                            "detached_copy_bytes": copied.bytes_total,
                            "model_calls": 0,
                        })
                    );
                }
            }
            let adaptive_role = RoleFile::parse(
                "---\nversion: 1\nname: worker\nmodel: fixture/model\ncontext: fork\nmay_spawn: []\neffort: adaptive\nmax_output_tokens: 64\n---\n",
            )?;
            let adaptive = resolve(&model, &adaptive_role, &project, &agent)?;
            let (fresh, _) = crate::tests::provider(adapter, 200);
            let adaptive = fresh.with_profile(&adaptive)?;
            assert!(
                adaptive
                    .profile_binding()
                    .unwrap()
                    .configuration_update(None)
                    .is_none()
            );
            let (adaptive_request, _) = crate::tests::request(&adaptive)?;
            let adaptive_body = crate::projection::body(&adaptive, adaptive_request.request())?;
            assert_eq!(adaptive_body.pointer(pointer), Some(&json!("high")));
            if adapter == Adapter::Responses {
                use acyclic_harness_core::model::{
                    ModelContent, ModelContentPart, ModelMessage, ModelRole, PreparedModelRequest,
                };
                let fixed_role = RoleFile::parse(
                    "---\nversion: 1\nname: worker\nmodel: fixture/model\ncontext: fork\nmay_spawn: []\neffort: low\nmax_output_tokens: 64\n---\n",
                )?;
                let fixed = resolve(&model, &fixed_role, &project, &agent)?;
                let (fresh, _) = crate::tests::provider(adapter, 200);
                let fixed = fresh.with_profile(&fixed)?;
                let (request, _) = crate::tests::request(&fixed)?;
                let mut request = request.request().clone();
                // Actual omission control: admitting the profile alone must not
                // silently rewrite the root field or manufacture an input item.
                assert!(crate::projection::body(&fixed, &request).is_err());
                let update = fixed
                    .profile_binding()
                    .unwrap()
                    .configuration_update(None)
                    .unwrap();
                request.messages.insert(0, update.clone());
                let prepared = PreparedModelRequest::prepare(request, Default::default(), None)?;
                let mut request = prepared.request().clone();
                let projected = crate::projection::body(&fixed, &request)?;
                assert!(crate::validate_input(adapter, &projected).is_empty());
                let mut adjacent = projected.clone();
                adjacent["input"]
                    .as_array_mut()
                    .unwrap()
                    .insert(1, projected["input"][0].clone());
                assert!(crate::validate_input(adapter, &adjacent).contains(&"OAR-CFG-02"));
                let mut malformed = projected.clone();
                malformed["input"][0]["reasoning"]["effort"] = json!("adaptive");
                assert!(crate::validate_input(adapter, &malformed).contains(&"OAR-CFG-01"));
                let mut interrupted = projected.clone();
                interrupted["input"].as_array_mut().unwrap().insert(
                    0,
                    json!({
                        "type":"function_call", "call_id":"pending", "name":"read", "arguments":"{}"
                    }),
                );
                assert!(crate::validate_input(adapter, &interrupted).contains(&"OAR-TOOL-04"));
                assert_eq!(projected["reasoning"], body["reasoning"]);
                assert_eq!(projected["tools"], body["tools"]);
                assert_eq!(projected["prompt_cache_key"], body["prompt_cache_key"]);
                assert_eq!(
                    projected["input"][0],
                    json!({"type":"configuration_update","reasoning":{"effort":"low"}})
                );
                assert_eq!(
                    &projected["input"].as_array().unwrap()[1..],
                    body["input"].as_array().unwrap()
                );
                let retained: acyclic_harness_core::model::ModelRequest =
                    serde_json::from_slice(prepared.bytes()).unwrap();
                assert_eq!(crate::projection::body(&fixed, &retained)?, projected);
                request.messages.insert(1, update.clone());
                assert!(crate::projection::body(&fixed, &request).is_err());
                request.messages.remove(1);
                let mut foreign = update.clone();
                let ModelContent::Part(ModelContentPart::Configuration { prefix, .. }) =
                    &mut foreign.content
                else {
                    unreachable!()
                };
                prefix[0] ^= 1;
                request.messages.insert(1, foreign.clone());
                request.messages.insert(2, update.clone());
                assert!(crate::projection::body(&fixed, &request).is_err());
                request.messages.remove(2);
                assert_eq!(crate::projection::body(&fixed, &request)?, projected);
                request.messages.remove(1);
                // Returning to the adaptive role restores its frozen baseline,
                // rather than retaining the preceding role's fixed override.
                request.messages.push(ModelMessage {
                    role: ModelRole::Assistant,
                    content: ModelContent::Text("settled".into()),
                });
                request.messages.push(
                    adaptive
                        .profile_binding()
                        .unwrap()
                        .configuration_update(Some(Effort::Low))
                        .unwrap(),
                );
                request.model = adaptive.model().clone();
                let restored = crate::projection::body(&adaptive, &request)?;
                assert_eq!(restored["reasoning"], body["reasoning"]);
                assert_eq!(
                    restored["input"].as_array().unwrap().last().unwrap(),
                    &json!({"type":"configuration_update","reasoning":{"effort":"high"}})
                );
                let mut wrong = update.clone();
                wrong.role = ModelRole::Assistant;
                let mut invalid = prepared.request().clone();
                invalid.messages[0] = wrong;
                assert!(PreparedModelRequest::prepare(invalid, Default::default(), None).is_err());
                assert!(update.content.validate_user_input(&mut None).is_err());
                let mut invalid = prepared.request().clone();
                invalid.messages[0].content = ModelContent::Parts(vec![match update.content {
                    ModelContent::Part(part) => part,
                    _ => unreachable!(),
                }]);
                assert!(PreparedModelRequest::prepare(invalid, Default::default(), None).is_err());
            }
            let mut wrong_request = prepared.request().clone();
            wrong_request.max_output_tokens = Some(65);
            assert!(crate::projection::body(&provider, &wrong_request).is_err());
            let (mut unknown, _) = crate::tests::provider(adapter, 200);
            unknown.capacity.output_tokens = None;
            let (request, _) = crate::tests::request(&unknown)?;
            let mut request = request.request().clone();
            // An explicit admitted reservation works without an advertised
            // output limit; omitting it cannot invent a provider capacity.
            assert!(crate::projection::body(&unknown, &request).is_ok());
            request.max_output_tokens = None;
            assert!(crate::projection::body(&unknown, &request).is_err());
            let ephemeral = family.replace("cache: automatic", "cache: ephemeral");
            let ephemeral = ModelProfile::parse(
                &ephemeral,
                "---\nversion: 1\nkey: fixture/model\n---\n",
                provider.capacity,
                "renderer-v1",
            )?;
            let ephemeral = resolve(&ephemeral, &role, &project, &agent)?;
            let (fresh, _) = crate::tests::provider(adapter, 200);
            let cached = fresh.with_profile(&ephemeral);
            assert_eq!(cached.is_ok(), adapter == Adapter::Anthropic);
            if let Ok(cached) = cached {
                let (request, _) = crate::tests::request(&cached)?;
                let body = crate::projection::body(&cached, request.request())?;
                assert_eq!(
                    body["cache_control"],
                    json!({"type":"ephemeral","ttl":"5m"})
                );
            }
            if adapter == Adapter::Responses {
                let key = body["prompt_cache_key"].clone();
                let prefix = profile.prefix_digest();
                let mut exact = None;
                let exact_cost = acyclic_test_support::measure(|| {
                    exact = Some(prefix_cache_key(prefix));
                });
                // Restoring the original formatter preserves every byte but
                // must regress the same one-allocation oracle.
                let mut formatted = None;
                let formatted_cost = acyclic_test_support::measure(|| {
                    formatted = Some(format!(
                        "profile-v1:{}",
                        blake3::Hash::from(prefix).to_hex()
                    ));
                });
                assert_eq!(exact, formatted);
                assert_eq!(exact_cost.count_total, 1);
                assert!(formatted_cost.count_total > exact_cost.count_total);
                assert!(formatted_cost.bytes_total > exact_cost.bytes_total);
                assert_eq!(key, json!(exact));
                println!(
                    "{}",
                    json!({
                        "version": 1,
                        "task": "prefix-cache-key-allocation",
                        "prefix_digest": prefix,
                        "key": key,
                        "exact_allocations": exact_cost.count_total,
                        "formatted_allocations": formatted_cost.count_total,
                        "exact_bytes": exact_cost.bytes_total,
                        "formatted_bytes": formatted_cost.bytes_total,
                    })
                );
                let reviewer = RoleFile::parse(
                    "---\nversion: 1\nname: reviewer\nmodel: fixture/model\ncontext: fresh\nmay_spawn: []\nmax_output_tokens: 64\n---\n",
                )?;
                let reviewer = resolve(&model, &reviewer, &project, &agent)?;
                assert_ne!(profile.digest(), reviewer.digest());
                assert_eq!(profile.prefix_digest(), reviewer.prefix_digest());
                let (fresh, _) = crate::tests::provider(adapter, 200);
                let fresh = fresh.with_profile(&reviewer)?;
                let (request, _) = crate::tests::request(&fresh)?;
                let (reviewed, _) = measured_projection(&fresh, &request, "role_change")?;
                assert_eq!(reviewed["prompt_cache_key"], key);

                let changed_family = format!("{family}Changed frozen instructions.\n");
                let changed = ModelProfile::parse(
                    &changed_family,
                    "---\nversion: 1\nkey: fixture/model\n---\n",
                    provider.capacity,
                    "renderer-v1",
                )?;
                let changed = resolve(&changed, &role, &project, &agent)?;
                let (fresh, _) = crate::tests::provider(adapter, 200);
                let fresh = fresh.with_profile(&changed)?;
                let (request, _) = crate::tests::request(&fresh)?;
                let (changed, _) = measured_projection(&fresh, &request, "prefix_change")?;
                assert_ne!(changed["prompt_cache_key"], key);
            }
        }
        Ok(())
    }
}
