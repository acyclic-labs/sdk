//! Public wire contracts for workspace context and merge publication records.

#[path = "wire_contracts/hosted_contract.rs"]
mod hosted_contract;

mod compat_wire {
    mod tests {
        use acyclic_fs::compat_wire::*;
        use acyclic_fs::{
            Digest, GenerationId, MergePlan, MultiRootFence, MultiRootMergeCandidate,
            MultiRootMergePlan, MultiRootMergeRoot, MultiRootPublication,
            MultiRootPublicationPhase, OperationId, Publication, WorkspaceContextId, WorkspaceId,
            WorkspaceRootId,
        };
        use std::collections::{BTreeMap, BTreeSet};

        fn generation(byte: u8) -> GenerationId {
            GenerationId::new(Digest::from_bytes([byte; 32]))
        }

        fn root_plan() -> MultiRootMergePlan {
            MultiRootMergePlan {
                operation_id: OperationId::from_bytes([1; 16]),
                parent_context_id: WorkspaceContextId::from_bytes([2; 16]),
                child_context_id: WorkspaceContextId::from_bytes([3; 16]),
                roots: [(
                    WorkspaceRootId::from_bytes([4; 16]),
                    MultiRootMergeRoot {
                        source_workspace_id: WorkspaceId::from_bytes([5; 16]),
                        merge_workspace_id: None,
                        source_generation: generation(6),
                        target_workspace_id: WorkspaceId::from_bytes([7; 16]),
                        target_generation: generation(8),
                        base_generation: generation(9),
                        child_wins_bindings: BTreeSet::new(),
                    },
                )]
                .into_iter()
                .collect(),
            }
        }

        #[test]
        fn merge_plan_round_trips_and_envelope_is_forward_compatible() {
            let plan = MergePlan {
                base: generation(1),
                ours: generation(2),
                theirs: generation(3),
                conflicts: Vec::new(),
                truncated: false,
            };
            let encoded = encode_merge_plan(&plan).expect("encode");
            assert!(encoded.contains("\"payload\":"));
            assert!(!encoded.contains("\"value\":"));
            assert_eq!(decode_merge_plan(&encoded).expect("decode"), plan);
            let unsupported = encoded.replacen("\"version\":1", "\"version\":2", 1);
            assert!(matches!(
                decode_merge_plan(&unsupported),
                Err(CompatibilityWireError::UnsupportedEnvelope)
            ));
            let additive = encoded.replacen("{", "{\"futureField\":true,", 1);
            assert_eq!(decode_merge_plan(&additive).expect("additive fields"), plan);
        }

        #[test]
        fn multi_root_plan_uses_entry_arrays_and_round_trips() {
            let plan = root_plan();
            let encoded = encode_multi_root_plan(&plan).expect("encode");
            assert!(encoded.contains("\"roots\":[{"));
            assert_eq!(decode_multi_root_plan(&encoded).expect("decode"), plan);
        }

        #[test]
        fn publication_round_trip_preserves_exact_published_generations() {
            let plan = root_plan();
            let root_id = *plan.roots.keys().next().expect("root");
            let generation = generation(42);
            let mut record = MultiRootPublication {
                version: 1,
                revision: 2,
                candidate: MultiRootMergeCandidate {
                    resolutions: plan
                        .roots
                        .keys()
                        .map(|root| (*root, BTreeMap::new()))
                        .collect(),
                    plan,
                },
                phase: MultiRootPublicationPhase::Applied,
                published_roots: BTreeSet::from([root_id]),
                published_generations: BTreeMap::from([(root_id, generation)]),
                fences: BTreeMap::from([(root_id, MultiRootFence { token: vec![7] })]),
                conflicts: BTreeMap::new(),
                projected_roots: BTreeMap::new(),
                declared_conflicts: BTreeMap::new(),
                paused_root: None,
            };
            let publication = Publication::Applied(record.clone());
            let encoded = encode_publication(&publication).expect("encode");
            assert!(encoded.contains("publishedGenerations"));
            assert_eq!(decode_publication(&encoded).expect("decode"), publication);
            let payload = decode_publication_payload(&encoded).expect("payload");
            let envelope = encode_publication_payload(&payload).expect("envelope");
            assert_eq!(
                decode_publication(&envelope).expect("restored publication"),
                publication
            );
            let wire: serde_json::Value = serde_json::from_str(&encoded).expect("encoded JSON");
            for field in ["publishedRoots", "publishedGenerations", "fences"] {
                let mut repeated = wire.clone();
                let entries = repeated
                    .get_mut("payload")
                    .and_then(|payload| payload.get_mut("value"))
                    .and_then(|value| value.get_mut(field))
                    .and_then(serde_json::Value::as_array_mut)
                    .expect("entry array");
                entries.push(entries.first().cloned().expect("one entry"));
                assert!(matches!(
                    decode_publication(&serde_json::to_string(&repeated).expect("duplicate JSON")),
                    Err(CompatibilityWireError::InvalidValue)
                ));
            }

            // An Applied record without the fences of its roots cannot come from
            // the coordinator, whose recovery rejects it; neither may the decoder.
            record.fences.clear();
            let encoded = encode_publication(&Publication::Applied(record)).expect("encode");
            assert!(matches!(
                decode_publication(&encoded),
                Err(CompatibilityWireError::InvalidValue)
            ));
        }

        #[test]
        fn multi_root_wire_rejects_empty_and_mismatched_candidates() {
            let mut plan = root_plan();
            plan.roots.clear();
            assert!(matches!(
                encode_multi_root_plan(&plan),
                Err(CompatibilityWireError::InvalidValue)
            ));
            let candidate = MultiRootMergeCandidate {
                plan: root_plan(),
                resolutions: BTreeMap::new(),
            };
            assert!(matches!(
                encode_multi_root_candidate(&candidate),
                Err(CompatibilityWireError::InvalidValue)
            ));
        }
    }
}

mod workspace_context_wire {
    mod tests {
        use acyclic_fs::workspace_context_wire::*;
        use acyclic_fs::{
            WorkspaceContext, WorkspaceContextId, WorkspaceContextRoot, WorkspaceContextState,
            WorkspaceId, WorkspaceRootId,
        };
        use acyclic_fs_wire::wire::filesystem::v2 as wire;
        use prost::Message;
        use std::collections::BTreeMap;

        fn root(byte: u8) -> Result<WorkspaceContextRoot, Box<dyn std::error::Error>> {
            Ok(WorkspaceContextRoot {
                root_id: WorkspaceRootId::from_bytes([byte; 16]),
                source_path: std::env::current_dir()?,
                workspace_id: WorkspaceId::from_bytes([byte + 1; 16]),
                workspace_name: "project".to_owned(),
                parent_workspace_id: None,
                mount_path: None,
            })
        }

        #[test]
        fn context_and_root_wire_round_trip_without_json_or_float_revisions()
        -> Result<(), Box<dyn std::error::Error>> {
            let root = root(3)?;
            let context = WorkspaceContext {
                version: 1,
                revision: u64::MAX,
                context_id: WorkspaceContextId::from_bytes([5; 16]),
                parent_context_id: None,
                roots: BTreeMap::from([(root.root_id, root.clone())]),
                state: WorkspaceContextState::Frozen,
            };
            assert_eq!(decode_context(&encode_context(&context)?)?, context);
            assert_eq!(
                decode_roots(&encode_roots(std::slice::from_ref(&root))?)?,
                vec![root.clone()]
            );
            assert_eq!(decode_root(&encode_root(&root)?)?, root);
            Ok(())
        }

        #[test]
        fn duplicate_roots_and_unknown_state_are_rejected() -> Result<(), Box<dyn std::error::Error>>
        {
            assert!(matches!(
                encode_roots(&[]),
                Err(WorkspaceContextWireError::InvalidRecord)
            ));
            assert!(matches!(
                decode_roots(&[]),
                Err(WorkspaceContextWireError::InvalidRecord)
            ));
            assert!(matches!(
                encode_discard(&[]),
                Err(WorkspaceContextWireError::InvalidRecord)
            ));
            assert!(matches!(
                decode_discard(&[]),
                Err(WorkspaceContextWireError::InvalidRecord)
            ));
            let root = root(3)?;
            let encoded = wire::WorkspaceContextRoot::decode(encode_root(&root)?.as_slice())?;
            let duplicate = wire::WorkspaceContextRoots {
                roots: vec![encoded.clone(), encoded],
            };
            assert!(matches!(
                decode_roots(&duplicate.encode_to_vec()),
                Err(WorkspaceContextWireError::InvalidRecord)
            ));
            let invalid = wire::WorkspaceContextSnapshot {
                version: 1,
                revision: 0,
                context_id: vec![5; 16],
                parent_context_id: None,
                roots: vec![],
                state: wire::WorkspaceContextState::Unspecified as i32,
            };
            assert!(matches!(
                decode_context(&invalid.encode_to_vec()),
                Err(WorkspaceContextWireError::InvalidRecord)
            ));
            Ok(())
        }
    }
}
