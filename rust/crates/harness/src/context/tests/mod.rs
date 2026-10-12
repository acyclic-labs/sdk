use super::*;
use crate::{
    AgentId,
    conversation::{FileDescriptor, VolumeClass, VolumeOwner, VolumeRef},
    model::{FileProjectionPolicy, ModelContentPart, ModelRole},
    resources::ProviderRef,
};
use acyclic_stream::MemoryStream;
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
        text(ModelRole::User, "old"),
        ModelMessage {
            role: ModelRole::Assistant,
            content: ModelContent::Parts(calls),
        },
        text(ModelRole::Assistant, "gap"),
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
    messages.push(text(ModelRole::User, "current"));
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
fn input() -> ContextInput {
    ContextInput {
        input: ModelContent::Text("current".into()),
        selected_context: None,
        step: 0,
        prior_messages: Vec::new(),
    }
}
fn text(role: ModelRole, value: &str) -> ModelMessage {
    ModelMessage {
        role,
        content: ModelContent::Text(value.into()),
    }
}
fn context(messages: Vec<ModelMessage>, current: Option<u32>) -> Context {
    Context {
        messages,
        current_input_index: current,
        metadata: BTreeMap::new(),
    }
}
fn summary(source: &Context, through: usize) -> Result<ContextSummary> {
    let mut prefix = source.clone();
    prefix.messages.truncate(through);
    prefix.current_input_index = prefix
        .current_input_index
        .filter(|index| (*index as usize) < through);
    Ok(ContextSummary {
        operation_id: crate::OperationId::new(),
        step: 0,
        source_messages: through as u32,
        source_digest: crate::contract::canonical_json_digest(&prefix, u64::MAX, &mut None)?,
        output: message(ModelRole::System, "summary")?
            .content
            .file_refs()
            .next()
            .unwrap()
            .clone(),
    })
}
fn compact(
    source: &Context,
    maximum: usize,
    through: usize,
) -> Result<(Context, CompactionReference)> {
    DurableContextProvider::compact(
        source,
        maximum,
        Some(summary(source, through)?),
        CompactionRetention::default(),
        None,
    )
}
fn durable(
    stream: Arc<MemoryStream>,
    payloads: Arc<TestPayloadStore>,
    maximum: u32,
    limits: Limits,
) -> Result<DurableContextProvider> {
    DurableContextProvider::new(
        stream,
        StreamPath::new("context/test").unwrap(),
        "memory",
        "1",
        maximum,
        payloads,
        limits,
    )
}
mod compaction;
mod durable;
mod pipeline;
