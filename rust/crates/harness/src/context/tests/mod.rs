use super::*;
use crate::{
    AgentId,
    conversation::{FileDescriptor, VolumeClass, VolumeOwner, VolumeRef},
    model::{FileProjectionPolicy, ModelContentPart, ModelRole},
    resources::ProviderRef,
};
#[cfg(feature = "filesystem")]
fn owned_context_budget() -> acyclic_fs::WorkBudget {
    acyclic_fs::WorkBudget {
        bytes_copied: 1_048_576,
        bytes_encoded: 1_048_576,
        bytes_hashed: 1_048_576,
        items_examined: 4096,
        allocation_operations: 4096,
        peak_allocation_bytes: 1_048_576,
        ..acyclic_fs::WorkBudget::default()
    }
}
use acyclic_stream::MemoryStream;
fn native_retention_policy() -> FileProjectionPolicy {
    FileProjectionPolicy::Native(Box::new(crate::model::NativeMediaPolicy {
        intent: crate::model::NativeMediaIntent::Document { maximum_pages: 1 },
        maximum_bytes: 1_048_576,
        maximum_work: 1,
        configuration: None,
    }))
}
fn native_call_context(file: &FileRef, results: usize, native_in_result: bool) -> Result<Context> {
    let mut calls: Vec<_> = (0..results)
        .map(|index| ModelContentPart::ToolCall {
            call_id: format!("native-{index}"),
            name: "native.tool".into(),
            arguments: Value::Null,
        })
        .collect();
    if !native_in_result {
        calls.push(ModelContentPart::File {
            file: file.clone(),
            policy: native_retention_policy(),
        });
    }
    let mut messages = vec![
        ModelMessage {
            role: ModelRole::User,
            content: ModelContent::Text("old".into()),
        },
        ModelMessage {
            role: ModelRole::Assistant,
            content: ModelContent::Parts(calls),
        },
        ModelMessage {
            role: ModelRole::Assistant,
            content: ModelContent::Text("gap".into()),
        },
    ];
    for index in 0..results {
        messages.push(ModelMessage {
            role: ModelRole::Tool,
            content: ModelContent::Part(ModelContentPart::ToolResult {
                call_id: format!("native-{index}"),
                name: "native.tool".into(),
                content: if native_in_result && index == 0 {
                    crate::model::ToolResultContent::Parts {
                        parts: vec![crate::model::ModelDataPart::File {
                            file: file.clone(),
                            policy: native_retention_policy(),
                        }],
                    }
                } else {
                    crate::model::ToolResultContent::Json { value: Value::Null }
                },
            }),
        });
    }
    messages.push(ModelMessage {
        role: ModelRole::User,
        content: ModelContent::Text("current".into()),
    });
    Ok(Context {
        current_input_index: Some(
            u32::try_from(messages.len() - 1)
                .map_err(|_| crate::Error::Invalid("fixture index exceeds u32".into()))?,
        ),
        messages,
        metadata: BTreeMap::new(),
    })
}
struct RefVerifier;
struct TestPayloadStore {
    volume: VolumeRef,
    files: std::sync::Mutex<BTreeMap<String, (FileRef, Vec<u8>)>>,
    reads: std::sync::atomic::AtomicUsize,
}
fn payload_limits() -> Limits {
    Limits {
        file_bytes: 1_048_576,
        render_bytes: 1_048_576,
        context_messages: 4_096,
        attachments: 1_024,
        path_bytes: 4_096,
        ..Limits::default()
    }
}
struct LimitSpy(std::sync::Mutex<Vec<Limits>>);
impl ContextSource for LimitSpy {
    fn load<'a>(
        &'a self,
        _: &'a ContextInput,
        limits: Limits,
        io: Option<&'a crate::executor::PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Vec<ModelMessage>>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            self.0.lock().unwrap().push(limits);
            Ok(vec![ModelMessage {
                role: ModelRole::System,
                content: ModelContent::Text("source".into()),
            }])
        })
    }
}
impl ContextStage for LimitSpy {
    fn name(&self) -> &str {
        "limit-spy"
    }
    fn contract(&self, _: Option<&crate::executor::PreparationIo>) -> Result<Value> {
        Ok(serde_json::json!({"revision":"1"}))
    }
    fn apply<'a>(
        &'a self,
        _: &'a ContextInput,
        context: Context,
        limits: Limits,
        io: Option<&'a crate::executor::PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Context>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            self.0.lock().unwrap().push(limits);
            Ok(context)
        })
    }
}
impl ContextRenderer for LimitSpy {
    fn contract(&self, _: Option<&crate::executor::PreparationIo>) -> Result<Value> {
        Ok(serde_json::json!({"revision":"1"}))
    }
    fn render<'a>(
        &'a self,
        _: &'a ContextSelection,
        _: ContextRenderMode,
        _: &'a ContextInput,
        limits: Limits,
        io: Option<&'a crate::executor::PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Vec<ModelMessage>>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            self.0.lock().unwrap().push(limits);
            Ok(vec![ModelMessage {
                role: ModelRole::System,
                content: ModelContent::Text("rendered".into()),
            }])
        })
    }
}
impl TestPayloadStore {
    fn new() -> Result<Self> {
        Ok(Self {
            volume: VolumeRef::new(
                ProviderRef::new("test".into(), "filesystem".into(), "2".into(), &mut None)?,
                "context".into(),
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(AgentId::from_bytes([1; 16])),
                &mut None,
            )?,
            files: std::sync::Mutex::new(BTreeMap::new()),
            reads: std::sync::atomic::AtomicUsize::new(0),
        })
    }
}
impl ContentPublisher for TestPayloadStore {
    fn volume(&self) -> &VolumeRef {
        &self.volume
    }

    fn stage<'a>(
        &'a self,
        operation: crate::OperationId,
        path: &'a str,
        bytes: &'a [u8],
        media_type: &'a str,
        display_name: &'a str,
        io: Option<&'a crate::executor::PreparationIo>,
    ) -> Result<
        crate::conversation::BoundedContentFuture<
            'a,
            (FileRef, Option<crate::conversation::ContentHold<'a>>),
        >,
    > {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            let reference = FileRef::new(
                self.volume.clone(),
                path.into(),
                operation.to_string(),
                FileDescriptor::from_bytes(bytes, media_type, &mut None)?,
                display_name.into(),
                &mut None,
            )?;
            let mut files = self.files.lock().unwrap();
            if let Some((retained, body)) = files.get(path) {
                if retained != &reference || body != bytes {
                    return Err(crate::Error::Conflict("payload identity changed".into()));
                }
                return Ok((retained.clone(), None));
            }
            files.insert(path.to_owned(), (reference.clone(), bytes.to_vec()));
            Ok((reference, None))
        })
    }
}
impl ContentResidencyVerifier for TestPayloadStore {
    fn verify<'a>(
        &'a self,
        file: &'a FileRef,
        _maximum_bytes: u64,
        io: Option<&'a crate::executor::PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        crate::conversation::BoundedContentFuture::new(
            io,
            || async move { file.validate(&mut None) },
        )
    }

    fn read<'a>(
        &'a self,
        file: &'a FileRef,
        _maximum_bytes: u64,
        io: Option<&'a crate::executor::PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let files = self.files.lock().unwrap();
            let (reference, bytes) = files
                .get(file.path())
                .ok_or_else(|| crate::Error::Storage("missing context payload".into()))?;
            if file != reference {
                return Err(crate::Error::Storage(
                    "context payload reference changed".into(),
                ));
            }
            Ok(bytes.clone().into())
        })
    }
}
struct RemoveInputMarker;
impl ContextStage for RemoveInputMarker {
    fn name(&self) -> &str {
        "remove-input-marker"
    }

    fn contract(&self, _: Option<&crate::executor::PreparationIo>) -> Result<Value> {
        Ok(serde_json::json!({"name": self.name(), "revision": "1"}))
    }

    fn apply<'a>(
        &'a self,
        _: &'a ContextInput,
        mut context: Context,
        _: Limits,
        io: Option<&'a crate::executor::PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Context>> {
        context.current_input_index = None;
        crate::conversation::BoundedContentFuture::new(io, || async move { Ok(context) })
    }
}
struct InputDependentProjection {
    revision: u32,
    calls: Arc<std::sync::atomic::AtomicUsize>,
}
impl ContextStage for InputDependentProjection {
    fn name(&self) -> &str {
        "input-dependent-projection"
    }

    fn contract(&self, _: Option<&crate::executor::PreparationIo>) -> Result<Value> {
        Ok(serde_json::json!({ "name": self.name(), "revision": self.revision }))
    }

    fn apply<'a>(
        &'a self,
        input: &'a ContextInput,
        mut context: Context,
        _: Limits,
        io: Option<&'a crate::executor::PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Context>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            context.messages.push(ModelMessage {
                role: ModelRole::System,
                content: ModelContent::Text(format!(
                    "stage:{}:{}",
                    self.revision,
                    serde_json::to_string(&input.input)
                        .map_err(|error| crate::Error::Invalid(error.to_string()))?
                )),
            });
            context.messages.reverse();
            let length = u32::try_from(context.messages.len())
                .map_err(|error| crate::Error::Invalid(error.to_string()))?;
            context.current_input_index =
                context.current_input_index.map(|index| length - 1 - index);
            Ok(context)
        })
    }
}
fn retained_input_base(input: ModelContent) -> Context {
    Context {
        messages: vec![
            ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text("older question".into()),
            },
            ModelMessage {
                role: ModelRole::Assistant,
                content: ModelContent::Text("older answer".into()),
            },
            ModelMessage {
                role: ModelRole::User,
                content: input,
            },
        ],
        current_input_index: Some(2),
        metadata: BTreeMap::new(),
    }
}
struct CountingVerifier {
    count: std::sync::atomic::AtomicUsize,
    payloads: Arc<TestPayloadStore>,
}
impl ContentResidencyVerifier for CountingVerifier {
    fn verify<'a>(
        &'a self,
        file: &'a FileRef,
        _maximum_bytes: u64,
        io: Option<&'a crate::executor::PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            self.count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            file.validate(&mut None)
        })
    }

    fn read<'a>(
        &'a self,
        file: &'a FileRef,
        _maximum_bytes: u64,
        io: Option<&'a crate::executor::PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        self.payloads.read(file, _maximum_bytes, io)
    }
}
struct AttributeRenderer;
impl ContextRenderer for AttributeRenderer {
    fn contract(&self, _: Option<&crate::executor::PreparationIo>) -> Result<Value> {
        Ok(serde_json::json!({"revision": "1"}))
    }

    fn render<'a>(
        &'a self,
        selection: &'a ContextSelection,
        mode: ContextRenderMode,
        _: &'a ContextInput,
        _: crate::conversation::Limits,
        io: Option<&'a crate::executor::PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Vec<ModelMessage>>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            let ContextSourceValue::Attribute { attribute } = &selection.source else {
                return Err(crate::Error::Unsupported(
                    "test renderer handles attributes".into(),
                ));
            };
            Ok(vec![ModelMessage {
                role: ModelRole::System,
                content: ModelContent::Text(format!(
                    "{mode:?}:{:?}:{}:{}",
                    selection.representation, attribute.state_revision, attribute.value
                )),
            }])
        })
    }
}
fn attribute_pipeline(
    value: &str,
    mode: ContextRenderMode,
    representation: ContextRepresentation,
) -> Result<ContextPipeline> {
    let attribute = ContextAttribute::typed(
        "example.role".into(),
        "1".into(),
        serde_json::json!({"type":"string"}),
        value.into(),
        &value,
    )?;
    Ok(ContextPipeline::new([Arc::new(SelectionStage::new(
        "role".into(),
        ContextSelection {
            source: ContextSourceValue::Attribute { attribute },
            extent: ContextExtent::Whole,
            representation,
        },
        Arc::new(AttributeRenderer),
        Arc::new(RefVerifier),
        mode,
        ContextPlacement::Prepend,
        crate::conversation::Limits::default(),
    )?) as Arc<dyn ContextStage>]))
}
impl ContentResidencyVerifier for RefVerifier {
    fn verify<'a>(
        &'a self,
        reference: &'a FileRef,
        _maximum_bytes: u64,
        io: Option<&'a crate::executor::PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            reference.validate(&mut None)
        })
    }
}
fn message(role: ModelRole, text: &str) -> Result<ModelMessage> {
    let volume = VolumeRef::new(
        ProviderRef::new("test".into(), "filesystem".into(), "2".into(), &mut None)?,
        "context".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(AgentId::from_bytes([1; 16])),
        &mut None,
    )?;
    let file = FileRef::new(
        volume,
        format!("context/{text}.txt"),
        "pinned".into(),
        FileDescriptor::from_bytes(text.as_bytes(), "text/plain", &mut None)?,
        format!("{text}.txt"),
        &mut None,
    )?;
    Ok(ModelMessage {
        role,
        content: ModelContent::Part(ModelContentPart::File {
            file,
            policy: FileProjectionPolicy::BoundedFull,
        }),
    })
}
mod compaction;
mod durable;
mod pipeline;
