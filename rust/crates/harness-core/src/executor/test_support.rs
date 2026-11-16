use super::*;
use crate::{
    AgentId,
    conversation::{FileDescriptor, VolumeOwner, VolumeRef},
    resources::ProviderRef,
};
use serde_json::json;
use std::collections::HashMap;
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Default)]
pub struct Journal(
    Mutex<Vec<ExecutionRecord>>,
    Mutex<HashMap<String, (FileRef, acyclic_stream::OwnedBytes)>>,
    Mutex<HashMap<InteractionId, (Interaction, Option<InteractionOutcome>)>>,
    AtomicUsize,
    AtomicUsize,
    AtomicUsize,
    ExecutionClaims,
    AtomicUsize,
    Option<crate::core::AuthorityVerifier>,
    ExecutionJournalCacheId,
    tokio_util::sync::CancellationToken,
);

impl Journal {
    /// Reopens the journal before the named tool completion.
    pub fn reopen_before_tool_completion(&self, call: &str) -> Self {
        Self(
            Mutex::new(self.0.lock().unwrap().iter().take_while(|record|
                !matches!(&record.event, ExecutionEvent::ToolCompleted { call_id, .. } if call_id == call)
            ).cloned().collect()),
            Mutex::new(self.1.lock().unwrap().clone()),
            Mutex::new(self.2.lock().unwrap().clone()),
            AtomicUsize::new(0), AtomicUsize::new(0), AtomicUsize::new(0),
            ExecutionClaims::default(), AtomicUsize::new(0), None,
            ExecutionJournalCacheId::default(), Default::default(),
        )
    }
    fn stage_source<'a>(
        &'a self,
        operation_id: OperationId,
        idempotency_key: String,
        bytes: acyclic_stream::OwnedBytes,
        media_type: &'static str,
    ) -> BoxFuture<'a, Result<FileRef>> {
        Box::pin(async move {
            let key = format!("{operation_id}:{idempotency_key}");
            let mut stored = self.1.lock().unwrap();
            if let Some((reference, existing)) = stored.get(&key) {
                return if existing.as_ref() == bytes.as_ref() {
                    Ok(reference.clone())
                } else {
                    Err(Error::Conflict("journal staging identity reused".into()))
                };
            }
            let volume = VolumeRef::new(
                ProviderRef::new("test".into(), "filesystem".into(), "2".into(), &mut None)?,
                "journal".into(),
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(AgentId::from_bytes([0; 16])),
                &mut None,
            )?;
            let reference = FileRef::new(
                volume,
                format!("journal/{}.json", blake3::hash(key.as_bytes()).to_hex()),
                blake3::hash(&bytes).to_hex().to_string(),
                FileDescriptor::from_bytes(&bytes, media_type, &mut None)?,
                "event.json".into(),
                &mut None,
            )?;
            stored.insert(key, (reference.clone(), bytes));
            Ok(reference)
        })
    }
}

struct ChangingContextSource(AtomicUsize);

impl crate::context::ContextSource for ChangingContextSource {
    fn load<'a>(
        &'a self,
        _: &'a ContextInput,
        _: Limits,
        io: Option<&'a crate::executor::PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Vec<ModelMessage>>> {
        let read = self.0.fetch_add(1, Ordering::SeqCst);
        crate::conversation::BoundedContentFuture::new(io, || async move {
            Ok(vec![ModelMessage {
                role: ModelRole::System,
                content: ModelContent::Text(format!("source revision {read}")),
            }])
        })
    }
}

impl ExecutionJournal for Journal {
    fn model_cancellation(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async {
            self.10.cancelled().await;
            Ok(())
        })
    }

    fn has_file_bound_model_verification(&self) -> bool {
        true
    }

    fn working_checkpoint_authority(&self) -> Option<&crate::core::AuthorityVerifier> {
        self.8.as_ref()
    }

    fn cache_identity(&self) -> Option<ExecutionJournalCacheId> {
        Some(self.9)
    }

    fn stage_prepared_model_request<'a>(
        &'a self,
        operation: OperationId,
        key: &'a str,
        prepared: &'a crate::model::PreparedModelRequest,
        previous: Option<&'a FileRef>,
        io: Option<&'a PreparationIo>,
    ) -> Result<Option<crate::conversation::BoundedContentFuture<'a, FileRef>>> {
        self.8
            .as_ref()
            .map(|verifier| {
                crate::conversation::BoundedContentFuture::new(io, || {
                    crate::executor::transcript::stage_attested_request(
                        self, operation, key, prepared, previous, verifier, io,
                    )
                })
            })
            .transpose()
    }

    fn started_calls(
        &self,
        io: Option<&PreparationIo>,
    ) -> Result<Option<Arc<crate::executor::StartedCalls>>> {
        self.6.started_calls(io).map(Some)
    }

    fn acquire_execution<'a>(
        &'a self,
        operation: OperationId,
        io: Option<&PreparationIo>,
    ) -> Result<crate::executor::ExecutionAttempt<'a>> {
        self.6.acquire([0; 32], operation, io)
    }

    fn verify_model_content<'a>(
        &'a self,
        content: &'a ModelContent,
        io: Option<&'a PreparationIo>,
        _context: Option<&'a crate::runtime::TaskContext>,
        _summary_files: &'a [&'a FileRef],
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        if io.is_some() {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Unsupported,
                "finite test journal verification is unavailable",
            ));
        }
        crate::conversation::BoundedContentFuture::new(io, || async move {
            for file in content.file_refs() {
                file.descriptor()
                    .verify(&self.load(file, io)?.await?, &mut None)?;
            }
            Ok(())
        })
    }

    fn verify_input_file<'a>(
        &'a self,
        reference: &'a FileRef,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        if io.is_some() {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Unsupported,
                "finite test journal verification is unavailable",
            ));
        }
        crate::conversation::BoundedContentFuture::new(io, || async move {
            let bytes = self.load(reference, io)?.await?;
            reference.descriptor().verify(&bytes, &mut None)
        })
    }

    fn replay<'a>(
        &'a self,
        operation_id: OperationId,
        after: u64,
        maximum: u32,
        io: Option<&'a crate::conversation::ContentIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Vec<ExecutionRecord>>> {
        if io.is_some() {
            return Err(crate::Error::Unsupported(
                "finite test journal IO is unavailable".into(),
            ));
        }
        crate::conversation::BoundedContentFuture::new(io, || async move {
            self.3.fetch_add(1, Ordering::SeqCst);
            validate_execution_page(maximum)?;
            let records: Vec<_> = self
                .0
                .lock()
                .unwrap()
                .iter()
                .filter(|record| record.operation_id == operation_id && record.sequence > after)
                .take(maximum as usize)
                .cloned()
                .collect();
            self.7.fetch_add(records.len(), Ordering::SeqCst);
            Ok(records)
        })
    }

    fn append<'a>(
        &'a self,
        operation_id: OperationId,
        idempotency_key: &'a str,
        event: &'a ExecutionEvent,
        io: Option<&'a crate::conversation::ContentIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        if io.is_some() {
            return Err(crate::Error::Unsupported(
                "finite test journal append is unavailable".into(),
            ));
        }
        crate::conversation::BoundedContentFuture::new(io, || async move {
            let event = event.clone();
            let idempotency_key = idempotency_key.to_owned();
            let mut records = self.0.lock().unwrap();
            if let Some(existing) = records.iter().find(|record| {
                record.operation_id == operation_id && record.idempotency_key == idempotency_key
            }) {
                return if existing.event == event {
                    Ok(())
                } else {
                    Err(Error::Conflict("journal retry identity reused".into()))
                };
            }
            let sequence = records
                .iter()
                .filter(|record| record.operation_id == operation_id)
                .count() as u64
                + 1;
            records.push(ExecutionRecord {
                committed_at_micros: None,
                operation_id,
                sequence,
                idempotency_key,
                event,
            });
            Ok(())
        })
    }

    fn append_if_tail<'a>(
        &'a self,
        operation_id: OperationId,
        expected_tail: u64,
        idempotency_key: &'a str,
        event: &'a ExecutionEvent,
        io: Option<&'a crate::conversation::ContentIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, bool>> {
        #[cfg(not(target_arch = "wasm32"))]
        acyclic_test_support::differential::record(("journal append", expected_tail));
        if io.is_some() {
            return Err(crate::Error::Unsupported(
                "finite test journal append is unavailable".into(),
            ));
        }
        crate::conversation::BoundedContentFuture::new(io, || async move {
            let event = event.clone();
            let idempotency_key = idempotency_key.to_owned();
            let mut records = self.0.lock().unwrap();
            let tail = records
                .iter()
                .filter(|record| record.operation_id == operation_id)
                .count() as u64;
            if tail != expected_tail {
                return Ok(false);
            }
            records.push(ExecutionRecord {
                committed_at_micros: None,
                operation_id,
                sequence: tail + 1,
                idempotency_key,
                event,
            });
            Ok(true)
        })
    }

    fn stage<'a>(
        &'a self,
        operation_id: OperationId,
        idempotency_key: &'a str,
        bytes: &'a [u8],
        media_type: &'static str,
        io: Option<&'a crate::conversation::ContentIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, FileRef>> {
        acyclic_test_support::differential::record(("journal stage", bytes.len() as u64));
        if io.is_some() {
            return Err(crate::Error::Unsupported(
                "finite test journal stage is unavailable".into(),
            ));
        }
        crate::conversation::BoundedContentFuture::new(io, || async move {
            self.stage_source(
                operation_id,
                idempotency_key.to_owned(),
                bytes.to_vec().into(),
                media_type,
            )
            .await
        })
    }

    fn staged_ref<'a>(
        &'a self,
        operation: OperationId,
        key: &'a str,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Option<FileRef>>> {
        if io.is_some() {
            return Err(Error::Unsupported(
                "finite test journal lookup is unavailable".into(),
            ));
        }
        crate::conversation::BoundedContentFuture::new(io, || async move {
            Ok(self
                .1
                .lock()
                .unwrap()
                .get(&format!("{operation}:{key}"))
                .map(|(file, _)| file.clone()))
        })
    }
    fn load<'a>(
        &'a self,
        reference: &'a FileRef,
        io: Option<&'a crate::conversation::ContentIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, crate::conversation::Custody>> {
        #[cfg(not(target_arch = "wasm32"))]
        acyclic_test_support::differential::record((
            "journal load",
            reference.descriptor().byte_length(),
        ));
        if io.is_some() {
            return Err(crate::Error::Unsupported(
                "finite test journal IO is unavailable".into(),
            ));
        }
        crate::conversation::BoundedContentFuture::new(io, || async move {
            self.4.fetch_add(1, Ordering::SeqCst);
            if reference.descriptor().media_type() == "text/plain" {
                self.5.fetch_add(1, Ordering::SeqCst);
            }
            self.1
                .lock()
                .unwrap()
                .values()
                .find(|(stored, _)| stored == reference)
                .map(|(_, bytes)| crate::conversation::Custody::Unmetered(bytes.clone()))
                .ok_or_else(|| Error::NotFound("journal content is missing".into()))
        })
    }

    fn load_range<'a>(
        &'a self,
        reference: &'a FileRef,
        range: acyclic_fs::ByteRange,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, crate::conversation::Custody>> {
        if io.is_some() {
            return Err(Error::Unsupported(
                "finite test journal range IO is unavailable".into(),
            ));
        }
        crate::conversation::BoundedContentFuture::new(io, || async move {
            let start = usize::try_from(range.offset)
                .map_err(|_| Error::Invalid("journal range offset overflows".into()))?;
            let end = range
                .offset
                .checked_add(range.length)
                .and_then(|end| usize::try_from(end).ok())
                .ok_or_else(|| Error::Invalid("journal range end overflows".into()))?;
            let stored = self.1.lock().unwrap();
            let (_, bytes) = stored
                .values()
                .find(|(stored, _)| stored == reference)
                .ok_or_else(|| Error::NotFound("journal content is missing".into()))?;
            if end > bytes.len() {
                return Err(Error::Invalid(
                    "journal range exceeds original payload".into(),
                ));
            }
            // This adapter owns immutable resident bytes already. Return only
            // their selected view; no whole-payload load or copy is performed.
            Ok(crate::conversation::Custody::Unmetered(
                bytes.slice(start..end),
            ))
        })
    }

    fn open_interaction<'a>(
        &'a self,
        id: InteractionId,
        request: &'a Interaction,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        if io.is_some() {
            return Err(Error::Unsupported(
                "test journal has no native interactions".into(),
            ));
        }
        crate::conversation::BoundedContentFuture::new(io, || async move {
            request.validate(&mut None)?;
            let mut interactions = self.2.lock().unwrap();
            if let Some((original, _)) = interactions.get(&id) {
                if original != request {
                    return Err(Error::Conflict("approval request changed".into()));
                }
            } else {
                interactions.insert(id, (request.clone(), None));
            }
            Ok(())
        })
    }

    fn interaction_outcome<'a>(
        &'a self,
        id: InteractionId,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Option<InteractionOutcome>>> {
        if io.is_some() {
            return Err(Error::Unsupported(
                "test journal has no native interactions".into(),
            ));
        }
        crate::conversation::BoundedContentFuture::new(io, || async move {
            Ok(self
                .2
                .lock()
                .unwrap()
                .get(&id)
                .and_then(|(_, outcome)| outcome.clone()))
        })
    }
}

impl crate::conversation::ContentResidencyVerifier for Journal {
    fn read<'a>(
        &'a self,
        file: &'a FileRef,
        _maximum_bytes: u64,
        _ceiling: Option<&'a crate::Capabilities>,
        io: Option<&'a crate::executor::PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, crate::conversation::Custody>> {
        crate::conversation::BoundedContentFuture::new(
            io,
            || async move { self.load(file, io)?.await },
        )
    }
}

struct OnceStage(AtomicUsize);

impl crate::context::ContextStage for OnceStage {
    fn name(&self) -> &str {
        "once"
    }
    fn contract(&self, _: Option<&crate::executor::PreparationIo>) -> Result<Value> {
        Ok(json!({"revision": "1"}))
    }
    fn apply<'a>(
        &'a self,
        _: &'a ContextInput,
        context: crate::context::Context,
        _: Limits,
        io: Option<&'a crate::executor::PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, crate::context::Context>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            if self.0.fetch_add(1, Ordering::SeqCst) != 0 {
                return Err(Error::NotFound("source removed after admission".into()));
            }
            Ok(context)
        })
    }
}
