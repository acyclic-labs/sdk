//! Projection accepts only the sealed result of the profile file resolver.
use crate::{Adapter, invalid};
use acyclic_harness_core::{Result, model::ModelProfileBinding};
use acyclic_model_render::{Cache, Effort, Reasoning};
use serde_json::{Map, Value, json};

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
                json!(format!(
                    "profile-v1:{}",
                    blake3::Hash::from(binding.prefix_digest()).to_hex()
                )),
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
    if adapter == Adapter::Gemini {
        if let Some(Value::Object(config)) = fields.get_mut("generationConfig") {
            config.insert("temperature".into(), json!(1.0));
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use acyclic_harness_core::{
        approvals::ApprovalPolicy,
        model::ModelProvider,
        profile::{AgentOverride, ModelProfile, ProjectLayer, RoleFile, resolve},
    };

    #[test]
    fn parsed_profiles_bind_original_selection_capacity_and_protocol_controls() -> Result<()> {
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
                "---\nversion: 1\ntools: []\nreasoning: {{mode: {reasoning}, effort: high}}\ncache: automatic\nbudget_tokens: 4096\nwatermark_tokens: 2000\n---\n"
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
            let body = crate::projection::body(&provider, prepared.request())?;
            assert_eq!(body.pointer(pointer), Some(&json!("high")));
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
                let reviewer = RoleFile::parse(
                    "---\nversion: 1\nname: reviewer\nmodel: fixture/model\ncontext: fresh\nmay_spawn: []\nmax_output_tokens: 64\n---\n",
                )?;
                let reviewer = resolve(&model, &reviewer, &project, &agent)?;
                assert_ne!(profile.digest(), reviewer.digest());
                assert_eq!(profile.prefix_digest(), reviewer.prefix_digest());
                let (fresh, _) = crate::tests::provider(adapter, 200);
                let fresh = fresh.with_profile(&reviewer)?;
                let (request, _) = crate::tests::request(&fresh)?;
                assert_eq!(
                    crate::projection::body(&fresh, request.request())?["prompt_cache_key"],
                    key
                );

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
                assert_ne!(
                    crate::projection::body(&fresh, request.request())?["prompt_cache_key"],
                    key
                );
            }
        }
        Ok(())
    }
}
