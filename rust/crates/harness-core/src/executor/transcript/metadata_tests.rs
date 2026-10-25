use super::*;

mod malformed;

#[derive(Serialize, Deserialize, crate::HeapLayout)]
#[serde(transparent)]
struct ScratchRetainingControl<T>(T);

fn check_manifest_diagnostics(manifest: &RequestManifest) -> Result<()> {
    let value =
        serde_json::to_value(manifest).map_err(|error| Error::Invalid(error.to_string()))?;
    // These fields reach each transitive decoder: fixed arrays, primitives,
    // FileRef, ProviderRefWire, the adjacent-tagged owner and its buffered id.
    for (pointer, is_string) in [
        ("/request_digest/0", false),
        ("/messages/count", false),
        ("/proof/0/31", false),
        ("/header/descriptor/sha256/0", false),
        ("/header/volume/provider/family", true),
        ("/header/volume/owner/kind", false),
        ("/header/volume/owner/id", true),
        ("/header/path", true),
        ("/admitted_limits/context_messages", false),
    ] {
        for replacement in [
            serde_json::json!({"unexpected": ["escaped\\\"".repeat(256)]}),
            if is_string {
                Value::Null
            } else {
                Value::String("invalid\\\"".repeat(256))
            },
        ] {
            let mut malformed = value.clone();
            *malformed
                .pointer_mut(pointer)
                .ok_or_else(|| invalid(None))? = replacement;
            let bytes = serde_json::to_vec(&malformed)
                .map_err(|error| Error::Invalid(error.to_string()))?;
            for native in [false, true] {
                let cancellation = acyclic_fs::CancellationToken::new();
                let budget =
                    acyclic_fs::performance::work_budget(16 * 1024 * 1024, 16 * 1024 * 1024);
                let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
                    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
                let baseline = work.live_allocation_bytes();
                let io = if native {
                    PreparationIo::from_native_work(&work)?
                } else {
                    PreparationIo::from_budget(budget, &cancellation)?
                };
                let mut result = None;
                let physical = acyclic_test_support::measure(|| {
                    result = Some(io.decode(
                        std::marker::PhantomData::<RequestManifest>,
                        &bytes,
                        crate::wire::ErrorCode::Storage,
                    ));
                });
                assert!(
                    result.ok_or_else(|| invalid(None))?.is_err(),
                    "accepted malformed {pointer}"
                );
                assert!(
                    physical.bytes_max <= io.receipt().peak_allocation_bytes,
                    "unfunded diagnostic at {pointer}: {physical:?}"
                );
                assert_eq!(io.owned_bytes()?, 0);
                drop(io);
                assert_eq!(work.live_allocation_bytes(), baseline);
            }
        }
    }
    Ok(())
}

// Same layout and wire representation, but deliberately retains decode scratch.
impl<T: PreparationRecord> PreparationRecord for ScratchRetainingControl<T> {
    fn diagnostic_label_bytes() -> u64 {
        T::diagnostic_label_bytes()
    }
    fn json_value_schema() -> &'static crate::executor::json::JsonValueSchema {
        T::json_value_schema()
    }
}

// A missing resident substitution must remain observable even when the shared
// layout's whole decode is already much smaller than the retired coarse bound.
fn check_scratch_control<T: PreparationRecord + Serialize>(value: &T) -> Result<()> {
    let bytes = crate::contract::canonical_json_bytes(value)?;
    for native in [false, true] {
        let cancellation = acyclic_fs::CancellationToken::new();
        let budget = acyclic_fs::performance::work_budget(16 * 1024 * 1024, 16 * 1024 * 1024);
        let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let baseline = work.live_allocation_bytes();
        let io = if native {
            PreparationIo::from_native_work(&work)?
        } else {
            PreparationIo::from_budget(budget, &cancellation)?
        };
        let (decoded, hold) = io.decode(
            std::marker::PhantomData::<ScratchRetainingControl<T>>,
            &bytes,
            crate::wire::ErrorCode::Storage,
        )?;
        let resident = decoded
            .0
            .decoded_owned_bytes()?
            .ok_or_else(|| invalid(None))?;
        assert!(
            hold.bytes() > resident,
            "negative control did not retain transient decode storage: {} <= {resident}",
            hold.bytes()
        );
        drop(decoded);
        drop(hold);
        assert_eq!(io.owned_bytes()?, 0);
        drop(io);
        assert_eq!(work.live_allocation_bytes(), baseline);
    }
    Ok(())
}

fn check_decode<T: PreparationRecord + Serialize>(
    value: &T,
    maximum_peak: u64,
    bounded: bool,
) -> Result<()> {
    check_decode_with_field(value, maximum_peak, bounded, None)
}

fn check_decode_with_field<T: PreparationRecord + Serialize>(
    value: &T,
    maximum_peak: u64,
    bounded: bool,
    invalid_field: Option<&str>,
) -> Result<()> {
    let value = serde_json::to_value(value).map_err(|error| Error::Invalid(error.to_string()))?;
    for native in [false, true] {
        for malformed in [false, true] {
            let mut wire = value.clone();
            if malformed {
                wire.as_object_mut().ok_or_else(|| invalid(None))?.insert(
                    invalid_field.map_or_else(|| "unknown\\\"".repeat(256), str::to_owned),
                    Value::Null,
                );
            }
            let bytes =
                serde_json::to_vec(&wire).map_err(|error| Error::Invalid(error.to_string()))?;
            let cancellation = acyclic_fs::CancellationToken::new();
            let budget = acyclic_fs::performance::work_budget(16 * 1024 * 1024, 16 * 1024 * 1024);
            let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
                .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
            let baseline = work.live_allocation_bytes();
            let io = if native {
                PreparationIo::from_native_work(&work)?
            } else {
                PreparationIo::from_budget(budget, &cancellation)?
            };
            let mut result = None;
            let physical = acyclic_test_support::measure(|| {
                result = Some(io.decode(
                    std::marker::PhantomData::<T>,
                    &bytes,
                    crate::wire::ErrorCode::Storage,
                ));
            });
            let result = result.ok_or_else(|| invalid(None))?;
            assert_eq!(result.is_ok(), !malformed);
            let peak = io.receipt().peak_allocation_bytes;
            assert!(physical.count_total > 0);
            assert!(
                physical.bytes_max <= peak,
                "physical={physical:?}; admitted={peak}"
            );
            println!(
                "metadata native={native} malformed={malformed} bounded={bounded} peak={peak} physical_peak={}",
                physical.bytes_max
            );
            assert_eq!(
                peak <= maximum_peak,
                bounded,
                "ref-only metadata reserved unrelated workflow slots: {peak}; bound={maximum_peak}"
            );
            if let Ok((decoded, hold)) = &result {
                assert_eq!(
                    Some(hold.bytes()),
                    decoded.decoded_owned_bytes()?,
                    "decoded record retained transient scratch"
                );
                assert_eq!(
                    serde_json::to_value(decoded)
                        .map_err(|error| Error::Invalid(error.to_string()))?,
                    value
                );
                let funded = i64::try_from(hold.bytes())
                    .map_err(|error| Error::Invalid(error.to_string()))?;
                assert!(
                    physical.bytes_current <= funded,
                    "physical={physical:?}; custody={}",
                    hold.bytes()
                );
                if bounded {
                    assert!(
                        hold.bytes() < 256 * 1024,
                        "ref-only metadata retained decode scratch: {}",
                        hold.bytes()
                    );
                }
            }
            drop(result);
            assert_eq!(io.owned_bytes()?, 0);
            if native && !malformed {
                // Resident Values have no lexical container census. They must
                // retain the conservative fallback and still fund actual use.
                let mut result = None;
                let physical = acyclic_test_support::measure(|| {
                    result = Some(
                        io.value_decode_preparation::<T>(&value, crate::wire::ErrorCode::Storage),
                    );
                });
                let (decoded, hold) = result.ok_or_else(|| invalid(None))??;
                assert!(physical.bytes_max <= work.receipt().peak_allocation_bytes - baseline);
                let funded = i64::try_from(hold.bytes())
                    .map_err(|error| Error::Invalid(error.to_string()))?;
                assert!(physical.bytes_current <= funded);
                drop(decoded);
                drop(hold);
                assert_eq!(io.owned_bytes()?, 0);
            }
            drop(io);
            assert_eq!(work.live_allocation_bytes(), baseline);
        }
    }
    Ok(())
}

#[tokio::test]
async fn ref_only_metadata_admission_covers_physical_storage_without_workflow_slots() -> Result<()>
{
    let storage =
        crate::filesystem::MemoryHarnessStorage::new(crate::AgentId::new(), 65_536).await?;
    let reference = storage
        .stage(
            OperationId::new(),
            "metadata/input.json",
            b"{}",
            "application/json",
            "input.json",
        )
        .await?;
    let manifest = RequestManifest {
        header: reference.clone(),
        header_digest: [255; 32],
        messages: MessagePrefix {
            reference: reference.clone(),
            count: 1,
            content_digest: [255; 32],
            chain_digest: [255; 32],
        },
        request_digest: [255; 32],
        admitted_limits: Limits::default(),
        proof: Some(([255; 32], [255; 32])),
    };
    check_decode(&manifest, 2 * 1024 * 1024, true)?;
    check_manifest_diagnostics(&manifest)?;
    check_scratch_control(&manifest)?;
    let compaction = crate::context::CompactionReference {
        source_digest: [255; 32],
        maximum_messages: 10,
        retention: crate::context::CompactionRetention::default(),
        summary: Some(crate::context::ContextSummary {
            operation_id: OperationId::new(),
            step: 0,
            source_messages: 10,
            source_digest: [255; 32],
            output: reference.clone(),
        }),
    };
    check_decode_with_field(&compaction, 1024 * 1024, true, Some("retention"))?;
    check_scratch_control(&compaction)?;
    for count in [1, 8, 64] {
        let checkpoint = crate::context::CanonicalContextCheckpoint {
            operation_id: OperationId::new(),
            selection: crate::conversation::ModelContextSelection {
                conversation_revision: count,
                message_ids: (1..=count)
                    .map(|index| uuid::Uuid::from_u128(index.into()))
                    .collect(),
                checkpoint: Some(reference.clone()),
            },
            source: reference.clone(),
            retained: reference.clone(),
            compaction: reference.clone(),
        };
        check_decode(&checkpoint, 16 * 1024 * 1024, true)?;
    }
    Ok(())
}

#[tokio::test]
async fn contexts_retain_values_and_sparse_nodes_without_decoder_scratch() -> Result<()> {
    let storage =
        crate::filesystem::MemoryHarnessStorage::new(crate::AgentId::new(), 65_536).await?;
    let reference = storage
        .stage(
            OperationId::new(),
            "metadata/context.json",
            b"{}",
            "application/json",
            "context.json",
        )
        .await?;
    for count in [1, 8] {
        let sparse = serde_json::json!({"outer": [{"inner": "value"}]});
        let context = crate::context::Context {
            messages: vec![
                ModelMessage {
                    role: ModelRole::User,
                    content: ModelContent::Text("history".repeat(128)),
                },
                ModelMessage {
                    role: ModelRole::Assistant,
                    content: ModelContent::Part(ModelContentPart::ToolCall(
                        crate::model::ToolCall::new("call".into(), "tool".into(), sparse.clone())
                            .with_metadata(sparse.clone()),
                    )),
                },
                ModelMessage {
                    role: ModelRole::Tool,
                    content: ModelContent::Part(ModelContentPart::ToolResult {
                        call_id: "call".into(),
                        name: "tool".into(),
                        content: crate::model::ToolResultContent::Json { value: sparse },
                    }),
                },
            ],
            metadata: (0..count)
                .map(|index| (format!("file:{index}"), reference.clone()))
                .collect(),
            current_input_index: Some(0),
        };
        check_decode(&context, 16 * 1024 * 1024, true)?;
        check_scratch_control(&context)?;
    }
    Ok(())
}

#[tokio::test]
async fn inline_projection_custody_releases_decoder_scratch_before_checkpoint_reads() -> Result<()>
{
    let storage =
        crate::filesystem::MemoryHarnessStorage::new(crate::AgentId::new(), 65_536).await?;
    let reference = storage
        .stage(
            OperationId::new(),
            "metadata/projection.json",
            b"{}",
            "application/json",
            "projection.json",
        )
        .await?;
    for count in [1, 8] {
        let projection = ResponseProjection {
            context: crate::context::Context {
                messages: vec![ModelMessage {
                    role: ModelRole::User,
                    content: ModelContent::Text("history".repeat(128)),
                }],
                metadata: (0..count)
                    .map(|index| (format!("file:{index}"), reference.clone()))
                    .collect(),
                current_input_index: Some(0),
            },
            canonical: Some(reference.clone()),
            checkpoint: Some(reference.clone()),
            working: Some(reference.clone()),
        };
        // Includes physical peak/residence, malformed diagnostics, sparse maps,
        // original native custody, and resident-Value decoding through one codec.
        check_decode(&projection, 16 * 1024 * 1024, true)?;
        check_scratch_control(&projection)?;
    }
    Ok(())
}
