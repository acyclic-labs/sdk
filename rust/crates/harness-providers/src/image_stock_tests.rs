use super::*;
use acyclic_harness::{
    Admission, AgentId, Capabilities, OperationId,
    context::{CompactionPolicy, ContextManifest},
    conversation::{
        BoundedContentFuture, ContentIo, ContentResidencyVerifier, FileRef, VolumeClass,
        VolumeOperation, VolumeOwner, VolumeRef,
    },
    core::{AggregateKind, Authority, AuthorityIssuer},
    distributed::{SchedulerPayloadStore, WorkPull, Worker},
    durable_tool::ResumableToolRegistry,
    filesystem::{
        FilesystemContentPublisher, FilesystemHost, FilesystemTaskRuntime, MODEL_TASK_COMMAND_KIND,
        ModelTaskCommand, PreparationIo, TaskWorkerAttempt, TaskWorkerOutcome,
    },
    resources::ProviderRef,
    runtime::{RuntimeScope, TaskDefinition, TaskRegistry},
    scheduler::{ResourceRequest, SessionLimits},
    store::{HistoryIo, HistoryWork},
    tool::ToolRegistry,
    workflow::{MachineIdentity, MachineRegistry, WorkflowCommand},
};
use acyclic_stream::MemoryStream;

#[derive(serde::Deserialize)]
struct ImageCase {
    name: String,
    supports_images: bool,
    replace_after_yield: bool,
    expected_copies: usize,
    expected_image_reads: usize,
    expected_fallbacks: usize,
}

fn image_cases() -> Result<Vec<ImageCase>> {
    serde_json::from_str(include_str!("fixtures/native-images.json"))
        .map_err(|error| invalid(error.to_string()))
}

struct ImageReads {
    reader: Arc<dyn ContentResidencyVerifier>,
    calls: AtomicUsize,
}

impl ContentResidencyVerifier for ImageReads {
    fn read<'a>(
        &'a self,
        file: &'a FileRef,
        maximum: u64,
        io: Option<&'a ContentIo>,
    ) -> Result<BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        if file.descriptor().media_type().starts_with("image/") {
            self.calls.fetch_add(1, Ordering::SeqCst);
        }
        self.reader.read(file, maximum, io)
    }
}

#[tokio::test]
async fn image_attach_and_paste_run_on_the_issued_native_task() -> Result<()> {
    let cases = image_cases()?;
    image_stock_turn(Some(
        cases
            .first()
            .ok_or_else(|| invalid("missing image baseline"))?,
    ))
    .await
}

#[tokio::test]
async fn image_versions_and_fallback_survive_native_task_recovery() -> Result<()> {
    for case in image_cases()?.iter().skip(1) {
        image_stock_turn(Some(case)).await?;
    }
    Ok(())
}

async fn image_stock_turn(image_case: Option<&ImageCase>) -> Result<()> {
    let with_images = image_case.is_some();
    for (adapter, response) in [
        (
            Adapter::Chat,
            "data: {\"choices\":[{\"delta\":{\"content\":\"done\"},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":1}}\n\ndata: [DONE]\n\n",
        ),
        (
            Adapter::Anthropic,
            "data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":5}}}\n\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"done\"}}\n\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":1}}\n\ndata: {\"type\":\"message_stop\"}\n\n",
        ),
        (
            Adapter::Responses,
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"done\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":5,\"output_tokens\":1}}}\n\n",
        ),
        (
            Adapter::Gemini,
            "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"done\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":5,\"candidatesTokenCount\":1}}\n\n",
        ),
    ] {
        let transport = Arc::new(Fixture::new(response));
        let mut provider = if adapter == Adapter::Responses {
            let mut registry = Registry::with_transport(
                Credentials::explicit(BTreeMap::from([(
                    "openai".into(),
                    "fixture-api-key".into(),
                )])),
                transport.clone(),
            );
            let catalog = tempfile::NamedTempFile::new().map_err(|_| invalid("fixture catalog"))?;
            std::fs::write(catalog.path(), include_str!("fixtures/luna-catalog.json"))
                .map_err(|_| invalid("fixture catalog"))?;
            registry.load_catalog(catalog.path())?;
            let selected = registry.resolve("openai/gpt-6-luna")?;
            assert_eq!(
                selected.context_capacity(selected.model())?.context_tokens,
                8192
            );
            selected
        } else {
            let (mut provider, _) = provider(adapter, 200);
            provider.transport = transport.clone();
            provider
        };
        let model = provider.model().clone();
        let storage_provider =
            ProviderRef::new("fixture".into(), "filesystem".into(), "2".into(), &mut None)?;
        let host = FilesystemHost::new(acyclic_fs::Fs::memory(), &storage_provider, None)?;
        let agent = AgentId::from_bytes([2; 16]);
        let volume = VolumeRef::new(
            storage_provider,
            "private".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(agent),
            &mut None,
        )?;
        host.create_volume(&volume).await?;
        let issuer = AuthorityIssuer::new(
            "fixture",
            [47; 32],
            Authority {
                kind: AggregateKind::Task,
                id: "owner".into(),
            },
        );
        let signed = issuer.issue(
            "owner",
            &Capabilities::new([
                "operation:declare".into(),
                "operation:observe".into(),
                "operation:cancel".into(),
                "task:spawn:model@1".into(),
                "model:generate".into(),
                volume.capability(VolumeOperation::Read, &mut None)?,
                volume.capability(VolumeOperation::Write, &mut None)?,
            ]),
            Some(agent),
            None,
            &mut None,
        )?;
        let scope = RuntimeScope::new(signed.capabilities().clone(), Limits::default())?;
        let work = work(&CancellationToken::new())?;
        let io = PreparationIo::from_native_work(&work)?;
        let operation = OperationId::from_bytes([1; 16]);
        let payloads = FilesystemContentPublisher::new(
            host.clone(),
            &volume,
            &issuer.verifier(&mut None)?,
            &signed,
            65_536,
            None,
        )?;
        let mut images = None;
        let input = if let Some(case) = image_case {
            use acyclic_harness::{
                client::{attach_image, paste_image},
                conversation::ContentPublisher,
                filesystem::FilesystemContentVerifier,
                model::{
                    FileProjectionPolicy, ImageDetail, ModelContentPart, NativeMediaIntent,
                    NativeMediaPolicy,
                },
                tui::{ClipboardContent, prepare_image},
            };
            use base64::Engine as _;
            let clipboard = prepare_image(2, 2, vec![255; 16]).expect("clipboard image");
            let ClipboardContent::Image { jpeg, .. } = &clipboard else {
                unreachable!()
            };
            let selected_clipboard =
                prepare_image(2, 2, [255, 0, 0, 255].repeat(4)).expect("selected image");
            let ClipboardContent::Image {
                jpeg: selected_jpeg,
                ..
            } = &selected_clipboard
            else {
                unreachable!()
            };
            let (selected, hold) = ContentPublisher::stage_once(
                payloads.as_ref(),
                OperationId::new(),
                "attachments/selected.jpg",
                selected_jpeg,
                "image/jpeg",
                "selected.jpg",
                None,
            )?
            .await?;
            drop(hold);
            let reader = FilesystemContentVerifier::new(
                host.clone(),
                &issuer.verifier(&mut None)?,
                &signed,
                65_536,
                Some(&io),
            )?;
            let attached = attach_image(selected, reader.as_ref(), 4096).await?;
            let paste_operation = OperationId::new();
            let pasted = paste_image(payloads.as_ref(), paste_operation, &clipboard, 4096).await?;
            let pinned = [attached.file.clone(), pasted.file.clone()];
            let reads = Arc::new(ImageReads {
                reader,
                calls: AtomicUsize::new(0),
            });
            provider = provider.with_content_reader(reads.clone());
            if case.supports_images {
                Arc::make_mut(&mut provider.config).images.insert(
                    model.name.clone(),
                    crate::ImageInput {
                        maximum_bytes: 4096,
                        maximum_total_bytes: 8192,
                        maximum_images: 4,
                        maximum_dimension: 1024,
                        maximum_tokens: 1024,
                    },
                );
            } else {
                Arc::make_mut(&mut provider.config)
                    .images
                    .remove(&model.name);
            }
            transport
                .expected_images
                .lock()
                .expect("fixture image expectations")
                .extend([selected_jpeg, jpeg].into_iter().map(|bytes| {
                    (
                        base64::engine::general_purpose::STANDARD
                            .encode(bytes)
                            .into_bytes(),
                        case.expected_copies,
                    )
                }));
            transport
                .expected_images
                .lock()
                .expect("fallback expectations")
                .push((b"omitted".to_vec(), case.expected_fallbacks));
            let mut parts = vec![ModelContentPart::Text {
                text: "question".into(),
            }];
            for attachment in [attached, pasted] {
                parts.push(ModelContentPart::File {
                    file: attachment.file,
                    policy: FileProjectionPolicy::Native(Box::new(NativeMediaPolicy {
                        intent: NativeMediaIntent::Image {
                            detail: ImageDetail::Auto,
                        },
                        maximum_bytes: 4096,
                        maximum_work: 1024,
                        configuration: None,
                    })),
                });
            }
            images = Some((reads, pinned, paste_operation, clipboard));
            ModelContent::Parts(parts)
        } else {
            ModelContent::Text("question".into())
        };
        let provider = Arc::new(provider);
        let command = ModelTaskCommand {
            summary_source: None,
            context_manifest: ContextManifest::default()
                .publish(
                    payloads.as_ref(),
                    OperationId::from_bytes([10; 16]),
                    "manifest.json",
                    Limits::default(),
                )
                .await?,
            input,
            selected_context: None,
            max_steps: 1,
            max_output_tokens: None,
            selected_output_volume: None,
            mailbox_operation: None,
        };
        let payload = payloads
            .stage(
                operation,
                "command",
                &serde_json::to_vec(&command).expect("command fixture"),
                None,
            )?
            .await?;
        let machine = Arc::new(OneCommand {
            identity: MachineIdentity {
                name: "model".into(),
                version: "1".into(),
                digest: [48; 32],
            },
            schema: json!({"type":"boolean"}),
            command: WorkflowCommand {
                operation_id: OperationId::from_bytes([3; 16]),
                kind: MODEL_TASK_COMMAND_KIND.into(),
                payload,
            },
        });
        let mut tasks = TaskRegistry::default();
        tasks.register(
            TaskDefinition::<serde_json::Value, serde_json::Value>::resumable(
                machine.clone(),
                json!({"type":"boolean"}),
                json!({"type":"null"}),
            )?
            .publish(&work)?,
        )?;
        let mut machines = MachineRegistry::default();
        machines.register(machine, &mut None)?;
        let runtime = FilesystemTaskRuntime::open(
            Arc::new(MemoryStream::default()),
            host.clone(),
            &volume,
            &issuer.verifier(&mut None)?,
            &signed,
            &scope,
            &tasks,
            &machines,
            &ToolRegistry::default(),
            SessionLimits {
                active_tasks: 1,
                total_tasks: 1,
                depth: 1,
                model_steps: 1,
            },
            1,
            65_536,
            Arc::new(Clock),
            None,
            &[],
            &ResumableToolRegistry::default(),
            Some(&io),
        )
        .await?;
        let definition =
            tasks.get_version::<serde_json::Value, serde_json::Value>("model", "1", None)?;
        assert!(matches!(
            runtime
                .harness()
                .admit(operation, &definition, serde_json::Value::Bool(false), None)
                .await?,
            Admission::Accepted(_)
        ));
        let mut history = HistoryWork::new(PreparationIo::from_native_work(&work)?);
        history.retain_owned(
            std::mem::size_of::<WorkPull>().max(std::mem::size_of::<TaskWorkerAttempt>()) as u64,
        )?;
        let pulled = runtime
            .task_host()
            .pull_work(
                &Worker {
                    id: "fixture".into(),
                    available: ResourceRequest::default(),
                    labels: BTreeMap::new(),
                },
                Some(operation),
                &mut Some(&mut history),
            )
            .await?;
        let WorkPull::Claimed(lease) = pulled else {
            panic!("native lease")
        };
        let recovery = history.into_custody()?;
        let policy = CompactionPolicy::Disabled;
        let commands =
            runtime
                .commands()
                .with_model(&model, provider.clone(), &policy, Some(&io))?;
        let (attempt, custody) = runtime
            .resume_task_with_native_work(
                lease,
                recovery,
                &commands,
                if with_images { 1 } else { 2 },
                &work,
            )
            .await?;
        assert!(custody.work().shares_allocation_context(&work));
        let attempt = if with_images {
            let TaskWorkerAttempt::Progress(TaskWorkerOutcome::Yielded { lease }) = attempt else {
                if let TaskWorkerAttempt::Unresolved { error, .. } = attempt {
                    return Err(error);
                }
                panic!("image task must yield at its transition boundary");
            };
            assert_eq!(transport.sent.load(Ordering::SeqCst), 0);
            let (reads, _, _, _) = images.as_ref().expect("image input fixture");
            assert_eq!(
                reads.calls.load(Ordering::SeqCst),
                0,
                "yielding before dispatch must not materialize images"
            );
            if let Some(case) = image_case.filter(|case| case.replace_after_yield) {
                use acyclic_harness::{
                    client::paste_image,
                    conversation::ContentPublisher,
                    tui::{ClipboardContent, prepare_image},
                };
                use base64::Engine as _;
                let (_, pinned, paste_operation, clipboard) =
                    images.as_ref().expect("pinned images");
                let replacement =
                    prepare_image(2, 2, [0, 0, 0, 255].repeat(4)).expect("replacement image");
                let ClipboardContent::Image { jpeg, .. } = &replacement else {
                    unreachable!()
                };
                for file in pinned {
                    let (updated, hold) = payloads
                        .stage_once(
                            OperationId::new(),
                            file.path(),
                            jpeg,
                            "image/jpeg",
                            "replacement.jpg",
                            None,
                        )?
                        .await?;
                    assert_ne!(
                        updated, *file,
                        "{} replaces the draft, not the submitted version",
                        case.name
                    );
                    drop(hold);
                }
                assert_eq!(
                    paste_image(payloads.as_ref(), *paste_operation, clipboard, 4096)
                        .await?
                        .file,
                    pinned[1]
                );
                transport
                    .expected_images
                    .lock()
                    .expect("replacement expectations")
                    .push((
                        base64::engine::general_purpose::STANDARD
                            .encode(jpeg)
                            .into_bytes(),
                        0,
                    ));
            }
            // A second caller invocation recovers the same durable reservation;
            // it does not replenish the first invocation's original ledger.
            let next = super::work(&CancellationToken::new())?;
            let next_io = PreparationIo::from_native_work(&next)?;
            let mut history = HistoryWork::new(PreparationIo::from_native_work(&next)?);
            history.retain_owned(
                std::mem::size_of::<WorkPull>().max(std::mem::size_of::<TaskWorkerAttempt>())
                    as u64,
            )?;
            let recovered = runtime
                .task_host()
                .recover_work(
                    acyclic_harness::TaskId::from_bytes(operation.into_bytes()),
                    &mut Some(&mut history),
                )
                .await?;
            let WorkPull::Claimed(recovered) = recovered else {
                panic!("retained image task lease")
            };
            assert_eq!(recovered.reservation, lease.reservation);
            assert_eq!(
                recovered.operation.operation_id,
                lease.operation.operation_id
            );
            drop(lease);
            drop(custody);
            let commands =
                runtime
                    .commands()
                    .with_model(&model, provider, &policy, Some(&next_io))?;
            let (attempt, custody) = runtime
                .resume_task_with_native_work(
                    recovered,
                    history.into_custody()?,
                    &commands,
                    1,
                    &next,
                )
                .await?;
            assert!(custody.work().shares_allocation_context(&next));
            drop(custody);
            attempt
        } else {
            drop(custody);
            attempt
        };
        if let TaskWorkerAttempt::Unresolved { error, .. } = attempt {
            return Err(error);
        }
        assert!(
            matches!(
                attempt,
                TaskWorkerAttempt::Progress(TaskWorkerOutcome::Completed { .. })
            ),
            "{adapter:?}"
        );
        assert_eq!(transport.sent.load(Ordering::SeqCst), 1, "{adapter:?}");
        assert_eq!(
            transport.counted.load(Ordering::SeqCst),
            0,
            "disabled compaction does not issue a count request"
        );
        if let (Some(case), Some((reads, _, _, _))) = (image_case, images) {
            assert_eq!(
                reads.calls.load(Ordering::SeqCst),
                case.expected_image_reads,
                "{adapter:?} {}",
                case.name
            );
        }
    }
    Ok(())
}
