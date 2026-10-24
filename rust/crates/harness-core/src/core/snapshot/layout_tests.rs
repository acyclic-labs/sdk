use super::*;
use crate::HeapLayout;
use crate::conversation::ConversationStateWire;
use crate::executor::json::{JsonBounds, JsonValueSchema, admit_json_layout};
use serde_json::json;
use std::collections::BTreeSet;

/// The per-task allocation ceiling the Pi hosts run under.
const TASK_BYTES: u64 = 8 << 20;
const BOUNDS: JsonBounds = JsonBounds {
    maximum_bytes: 1 << 24,
    maximum_items: 1 << 24,
    maximum_depth: 256,
};

macro_rules! fixture {
    ($name:literal) => {
        serde_json::from_str::<Value>(include_str!(concat!(
            "../../../../harness/fixtures/v2/",
            $name
        )))
        .expect("fixture")
    };
}

fn digest() -> Value {
    json!(vec![7_u8; 32])
}
fn id(byte: u8) -> Value {
    json!(uuid::Uuid::from_bytes([byte; 16]))
}
fn resource(kind: &str, provider: &Value) -> Value {
    json!({"kind": kind, "provider": provider, "key": [1, 2], "version": "1"})
}

struct Parts {
    file: Value,
    manifest: Value,
    volume: Value,
    provider: Value,
    generation: Value,
    authority: Value,
    reference: Value,
    dependency: Value,
    record: Value,
    configuration: Value,
    status: Value,
    ticket: Value,
    resolution: Value,
}
impl Parts {
    fn new() -> Self {
        let file = fixture!("file-ref.json");
        let mut manifest = file.clone();
        manifest["descriptor"]["media_type"] =
            json!("application/vnd.acyclic.harness.attachments+json");
        let volume = file["volume"].clone();
        let provider = volume["provider"].clone();
        let authority = json!({"kind": "conversation", "id": "parent"});
        Self {
            generation: resource("generation", &provider),
            reference: json!({"authority": authority, "revision": 1}),
            dependency: fixture!("extension-dependency.json"),
            record: fixture!("extension-record.json"),
            configuration: fixture!("extension-configuration.json"),
            status: json!({"state": "succeeded", "result": file}),
            ticket: json!({
                "id": id(5), "kind": "approval", "request": file, "deadline_unix_ms": 1,
                "approval": {"operation_id": id(6), "action_digest": digest()},
            }),
            resolution: json!({
                "id": id(5), "expected_version": 1,
                "outcome": {"kind": "answered", "answer": file}, "detail": file,
            }),
            authority,
            file,
            manifest,
            volume,
            provider,
        }
    }
    fn message(&self, sequence: u64, attachments: Value) -> Value {
        let mut message = fixture!("conversation-message.json");
        message["id"] = id(u8::try_from(sequence % 256).expect("byte"));
        message["sequence"] = json!(sequence);
        message["attachments"] = attachments;
        message["reply_to"] = id(9);
        message["tool_call_id"] = json!("call");
        message["extensions"] = json!({"example": self.file});
        message
    }
    fn inline(&self) -> Value {
        json!({"kind": "inline", "items": [{"file": self.file, "label": "attached"}]})
    }
    fn revisions(&self) -> Vec<Value> {
        let (provider, volume, generation) = (&self.provider, &self.volume, &self.generation);
        vec![
            json!({"kind": "history", "reference": resource("stream", provider)}),
            json!({"kind": "project", "reference": {"volume": volume, "generation": generation}}),
            json!({"kind": "private_volume", "reference": {
                "volume": volume, "generation": generation, "paths": ["notes"],
            }}),
            json!({"kind": "context", "reference": resource("context", provider)}),
            json!({"kind": "process", "reference": resource("checkpoint", provider)}),
            json!({"kind": "artifact", "reference": resource("artifact", provider)}),
            json!({"kind": "shared_volume", "reference": volume}),
            json!({"kind": "extension", "reference": {
                "name": "example.state", "version": 1, "implementation_digest": digest(),
                "reference": resource("artifact", provider),
            }}),
        ]
    }
    fn scope(&self) -> Value {
        json!({
            "id": "scope", "capabilities": ["read"], "agent": id(2), "issuer": "issuer",
            "parent_proof": digest(), "proof": digest(),
        })
    }
    fn seed(&self) -> Value {
        let (file, volume, generation) = (&self.file, &self.volume, &self.generation);
        let revisions = self.revisions();
        let mut seed = fixture!("fork-seed.json");
        seed["attached_agents"] = json!([id(3)]);
        seed["resources"] = revisions
            .iter()
            .map(|revision| json!({"source": revision, "revision": revision}))
            .collect();
        seed["omissions"] = json!([
            {"selection": {"required": true, "revision": revisions[0]},
             "outcome": {"kind": "captured", "value": {"source": revisions[1], "revision": revisions[1]}}},
            {"selection": {"required": false, "revision": revisions[2]},
             "outcome": {"kind": "unsupported", "value": "unsupported"}},
        ]);
        seed["authorized_source"] = json!({
            "owner": self.authority, "caller_task": id(4), "caller_admission_digest": digest(),
            "caller_fence": {"reservation_id": "reservation", "placement": "placement"},
            "operation_id": id(6), "source_task": id(4), "project": volume,
            "generation": generation, "native_snapshot": {"tree": [1, {"leaf": "a"}]},
            "graph_root": id(4), "maximum_work": 1, "maximum_bytes": 1,
            "policy_scope": self.scope(),
        });
        seed["child_private_generation"] = generation.clone();
        seed["child_result_volume"] = volume.clone();
        seed["child_result_generation"] = generation.clone();
        seed["inherited_context"] = json!([file]);
        seed["summary"] = json!({
            "selection": {
                "checkpoint": file,
                "limits": {
                    "file_bytes": 1, "path_bytes": 1, "attachments": 1, "render_bytes": 1,
                    "model_steps": 1, "model_events_per_step": 1, "tool_calls_per_step": 1,
                    "context_messages": 1,
                },
                "history_limits": {"maximum_events": 1, "maximum_bytes": 1},
            },
            "context": file, "payloads": [file], "references": [file],
        });
        seed["shared_grants"] =
            json!([{"volume": volume, "child_agent": id(3), "operations": ["read", "write"]}]);
        seed["reference_grants"] =
            json!([{"file": file, "reader": id(3), "attachment_manifest": self.manifest}]);
        seed["history_witness"] = digest();
        seed["attachment_manifests"] = json!([self.manifest]);
        seed["boundary"] = json!({"provider": self.provider, "evidence": [1, 2]});
        seed
    }
    fn receipt(&self) -> Value {
        let mut receipt = fixture!("project-merge-receipt.json");
        for generation in [
            "source_generation",
            "expected_target_generation",
            "result_generation",
        ] {
            receipt[generation] = self.generation.clone();
        }
        receipt["provider_proof"]["statement"] = json!({"commit": [1, {"tree": "a"}]});
        receipt["notice"] = self.message(
            2,
            json!({"kind": "manifest", "manifest": self.manifest, "item_count": 1}),
        );
        receipt
    }
    /// One payload of every `EventPayload` variant.
    fn payloads(&self) -> Vec<Value> {
        let (file, dependency, configuration) = (&self.file, &self.dependency, &self.configuration);
        let attestation = json!({
            "provider": "provider", "effect_id": id(7), "attempt_id": id(8),
            "request_digest": digest(), "guarantee": "exactly_once",
            "result_schema_digest": digest(), "status": self.status, "proof": digest(),
        });
        vec![
            json!({"kind": "lifecycle_transitioned", "from": "pending", "to": "active", "reason": "start"}),
            json!({"kind": "custom", "record": self.record}),
            json!({"kind": "extension_state_migrated", "migration": fixture!("extension-state-migration.json")}),
            json!({"kind": "extensions_selected", "previous": [dependency], "selected": [dependency], "configurations": [configuration]}),
            json!({"kind": "extension_configured", "previous": configuration, "record": configuration}),
            json!({
                "kind": "effect_planned", "effect_id": id(7), "provider": "provider",
                "guarantee": "exactly_once", "effect_kind": "effect", "request": file,
                "request_digest": digest(), "result_schema": file, "result_schema_digest": digest(),
            }),
            json!({"kind": "effect_dispatched", "effect_id": id(7), "attempt_id": id(8)}),
            json!({"kind": "effect_resolved", "observation": attestation}),
            json!({"kind": "fork_published", "seed": self.seed()}),
            json!({"kind": "project_merge_published", "receipt": self.receipt()}),
            json!({"kind": "conversation_bound", "agent": id(2)}),
            json!({"kind": "conversation_message_appended", "message": self.message(1, self.inline())}),
            json!({"kind": "model_context_selected", "selection": {
                "conversation_revision": 1, "message_ids": [id(1)], "checkpoint": file,
            }}),
            json!({"kind": "interaction_opened", "ticket": self.ticket}),
            json!({"kind": "interaction_resolved", "resolution": self.resolution}),
        ]
    }
    fn event(&self, revision: usize, payload: &Value) -> Value {
        json!({
            "revision": revision, "operation_id": id(6), "intent_digest": digest(),
            "scope": {"id": "scope", "capabilities": ["read"], "issuer": "issuer", "agent": id(2)},
            "attestation": digest(), "causal_parent": self.reference, "payload": payload,
        })
    }
    /// A checkpoint whose every list, map, box and option is populated,
    /// with `copies` elements in each list of events and messages.
    fn snapshot(&self, copies: usize) -> Value {
        let payloads = self.payloads();
        let events: Vec<Value> = (0..copies)
            .flat_map(|_| payloads.iter().cloned())
            .enumerate()
            .map(|(index, payload)| self.event(index + 1, &payload))
            .collect();
        let messages: Vec<Value> = (1..=copies)
            .map(|sequence| self.message(sequence as u64 + 1, self.inline()))
            .collect();
        let (file, dependency, configuration) = (&self.file, &self.dependency, &self.configuration);
        json!({
            "format_version": 1,
            "authority": self.authority,
            "revision": events.len(),
            "events": events,
            "projection": {
                "lifecycle": "active",
                "active_extensions": [dependency],
                "active_extension_revision": 1,
                "extension_states": {"example.state": [self.reference, self.record]},
                "configured_extensions": [[dependency, configuration]],
                "active_configurations": [configuration],
                "effects": {id(7).as_str().expect("id"): {
                    "provider": "provider", "guarantee": "exactly_once", "effect_kind": "effect",
                    "request": file, "request_digest": digest(), "result_schema": file,
                    "result_schema_digest": digest(), "status": self.status, "attempts": [id(8)],
                }},
                "indexed_terminal_effects": [id(7)],
                "effects_archived": true,
                "forks": [[{"kind": "conversation", "id": "child"}, self.seed()]],
                "published_merges": [["merge", [1, 2]]],
                "conversation": {
                    "agent": id(2),
                    "messages": messages,
                    "archived_prefix": {"through_sequence": 1, "history_digest": digest(), "checkpoint": file},
                },
                "latest_context_checkpoint": file,
                "interactions": {id(5).as_str().expect("id"): [self.ticket, self.resolution]},
                "bindings": [[dependency, digest()]],
            },
            "state_digest": digest(),
            "attestation": digest(),
        })
    }
}

/// Every array, object and option in `value` holds at least one value.
fn assert_populated(value: &Value, path: &str) {
    match value {
        Value::Null => panic!("{path} is empty"),
        Value::Array(items) => {
            assert!(!items.is_empty(), "{path} is empty");
            for (index, item) in items.iter().enumerate() {
                assert_populated(item, &format!("{path}[{index}]"));
            }
        }
        Value::Object(fields) => {
            assert!(!fields.is_empty(), "{path} is empty");
            for (key, item) in fields {
                assert_populated(item, &format!("{path}.{key}"));
            }
        }
        _ => {}
    }
}

/// The checkpoint's own derived layout.
const LAYOUT: &crate::executor::json::JsonLayout =
    <Snapshot<ConversationStateWire> as HeapLayout>::LAYOUT;

fn admit(bytes: &[u8]) -> Result<u64> {
    admit_json_layout(
        bytes,
        BOUNDS,
        Some(JsonValueSchema::Snapshot),
        LAYOUT,
        None,
        &mut |_| Ok(()),
    )
    .map(|(_, cost)| cost.bytes())
}

/// Reserved bytes and the measured peak of the decode.
fn reserve_and_decode(snapshot: &Value) -> (u64, u64) {
    let bytes = serde_json::to_vec(snapshot).expect("encode");
    let reserved = admit(&bytes).expect("a populated checkpoint is admitted");
    let mut decoded = None;
    let measured = acyclic_test_support::measure(|| {
        decoded = Some(serde_json::from_slice::<Snapshot<ConversationStateWire>>(
            &bytes,
        ));
    });
    decoded
        .expect("decoded")
        .expect("a populated checkpoint decodes");
    (reserved, measured.bytes_max)
}

#[test]
fn every_container_of_a_fully_populated_checkpoint_is_reserved() {
    let parts = Parts::new();
    let snapshot = parts.snapshot(1);
    assert_populated(&snapshot, "snapshot");
    let kinds: BTreeSet<_> = parts
        .payloads()
        .iter()
        .map(|payload| payload["kind"].as_str().expect("kind").to_owned())
        .collect();
    assert_eq!(kinds.len(), 15, "one payload of every event variant");
    let (reserved, measured) = reserve_and_decode(&snapshot);
    assert!(
        measured <= reserved,
        "decode peak {measured} exceeds reservation {reserved}"
    );
}

#[test]
fn the_reservation_covers_the_decode_as_lists_grow() {
    let parts = Parts::new();
    for copies in [2, 7, 33, 64] {
        let (reserved, measured) = reserve_and_decode(&parts.snapshot(copies));
        assert!(
            measured <= reserved,
            "{copies} copies: decode peak {measured} exceeds reservation {reserved}"
        );
        // Linear in the checkpoint, with bounded slack over the real peak.
        eprintln!("{copies} copies: reserved {reserved}, decode peak {measured}");
        assert!(
            reserved <= measured * 4,
            "{copies} copies: {reserved} vs {measured}"
        );
    }
}

#[test]
fn a_container_its_layout_cannot_hold_is_refused_before_decoding() {
    let snapshot = Parts::new().snapshot(1);
    let described = admit(&serde_json::to_vec(&snapshot).expect("encode")).expect("admitted");
    // A list where the type holds a unit enum: the decode rejects it, so
    // admission refuses it instead of charging a guessed slot.
    let mut wrong = snapshot.clone();
    wrong["projection"]["lifecycle"] = json!([[1], [2], [3]]);
    let bytes = serde_json::to_vec(&wrong).expect("encode");
    let refused = admit(&bytes).expect_err("an ill-kinded container is refused");
    assert!(
        refused.to_string().contains("outside its typed layout"),
        "{refused}"
    );
    assert!(serde_json::from_slice::<Snapshot<ConversationStateWire>>(&bytes).is_err());
    // An unknown member holds no typed storage: serde skips or rejects it,
    // so it charges nothing beyond its lexical admission.
    let mut unknown = snapshot;
    unknown["projection"]["unknown"] = json!([[1], [2], [3]]);
    let bytes = serde_json::to_vec(&unknown).expect("encode");
    assert_eq!(admit(&bytes).expect("admitted"), described);
}

#[test]
fn a_conversation_checkpoint_reserves_a_fraction_of_the_task_budget() {
    // A Pi conversation checkpoint of the size the context tests write: a
    // lifecycle and a conversation, every other collection empty.
    let parts = Parts::new();
    let checkpoint = |messages: u64| {
        let messages: Vec<Value> = (1..=messages)
            .map(|sequence| parts.message(sequence, json!({"kind": "inline", "items": []})))
            .collect();
        json!({
            "format_version": 1, "authority": parts.authority, "revision": messages.len(),
            "events": [],
            "projection": {
                "lifecycle": "active", "active_extensions": [], "active_extension_revision": null,
                "extension_states": {}, "configured_extensions": [], "active_configurations": [],
                "effects": {}, "forks": [], "published_merges": [],
                "conversation": {"agent": id(2), "messages": messages, "archived_prefix": null},
                "latest_context_checkpoint": null, "interactions": {}, "bindings": [],
            },
            "state_digest": digest(), "attestation": digest(),
        })
    };
    let snapshot = (1..)
        .map(checkpoint)
        .find(|snapshot| serde_json::to_vec(snapshot).expect("encode").len() >= 11_600)
        .expect("checkpoint");
    let bytes = serde_json::to_vec(&snapshot).expect("encode");
    let (reserved, measured) = reserve_and_decode(&snapshot);
    eprintln!(
        "{} byte checkpoint: reserved {reserved}, decode peak {measured}",
        bytes.len()
    );
    assert!(reserved < TASK_BYTES / 8, "reserved {reserved}");
    assert!(
        measured <= reserved,
        "decode peak {measured} exceeds reservation {reserved}"
    );
}
