//! Timer.
use super::*;

impl<P: StreamProvider> CoordinatorTaskHost<P> {
    #[cfg(feature = "filesystem")]
    /// Durably retains one timer identity under its original owner and work.
    pub async fn poll_timer(
        self: &Arc<Self>,
        task: TaskId,
        fence: &crate::scheduler::LeaseFence,
        operation: OperationId,
        deadline_unix_ms: u64,
        native: crate::NativeWork<'_>,
    ) -> Result<bool> {
        #[cfg(not(target_arch = "wasm32"))]
        let owner = if let Some(native) = native {
            self.journal_owner_with_native_work(task, fence, native)
                .await?
        } else {
            self.journal_owner(task, fence.clone()).await?
        };
        #[cfg(target_arch = "wasm32")]
        let owner = self.journal_owner(task, fence.clone()).await?;
        #[cfg(not(target_arch = "wasm32"))]
        let mut io = native
            .map(|work| {
                crate::filesystem::PreparationIo::from_native_work(work)
                    .map(crate::store::HistoryWork::new)
            })
            .transpose()?;
        #[cfg(not(target_arch = "wasm32"))]
        let mut work = io.as_mut().map(|io| io as &mut dyn crate::store::HistoryIo);
        #[cfg(target_arch = "wasm32")]
        let mut work: Option<&mut dyn crate::store::HistoryIo> = None;
        let _ = native;
        let mut custody = crate::wire_codec::CodecQuota::new(&mut work, 0)?;
        if let Some(io) = custody.work().as_deref_mut() {
            io.check_cancellation()?;
        }
        if deadline_unix_ms == 0 {
            return Err(admission_boundary(
                custody.work(),
                crate::wire::ErrorCode::Invalid,
                "timer deadline is invalid",
            ));
        }
        let (task, fence) = owner.task_binding();
        if !owner.input_grants.contains("timer:wait") {
            return Err(admission_boundary(
                custody.work(),
                crate::wire::ErrorCode::Unauthorized,
                "task scope lacks timer:wait",
            ));
        }
        let (path, path_retained) = Self::timer_path_with_work(task, custody.work())?;
        custody.adopt(path_retained)?;
        let task_text = Self::timer_path_text(task);
        let mut operation_text = [0; 36];
        uuid::Uuid::from_bytes(operation.into_bytes())
            .hyphenated()
            .encode_lower(&mut operation_text);
        if let Some(io) = custody.work().as_deref_mut() {
            io.hashed_bytes((task_text.len() + 1 + operation_text.len()) as u64)?;
        }
        let mut identity = blake3::Hasher::new();
        identity.update(&task_text);
        identity.update(b"/");
        identity.update(&operation_text);
        let digest = identity.finalize();
        let (key, key_retained) = {
            let mut key_custody = crate::wire_codec::CodecQuota::new(custody.work(), 0)?;
            let bytes = crate::core::copy_copy_vector(digest.as_bytes(), &mut key_custody)?;
            key_custody.transfer();
            let (bytes, retained) =
                crate::store::operations::owned_bytes_from_retained_vec(bytes, key_custody.work())?;
            (StreamKey::new(bytes)?, retained)
        };
        custody.adopt(key_retained)?;
        let encoded = crate::store::history::canonical_bytes_with_work(
            &TimerEvent {
                task_id: task,
                operation_id: operation,
                deadline_unix_ms,
            },
            acyclic_stream::MAX_RECORD_BYTES as u64,
            custody.work(),
        )?;
        let (bytes, byte_retained) =
            crate::store::operations::owned_bytes_from_retained_vec(encoded, custody.work())?;
        custody.adopt(byte_retained)?;
        loop {
            self.verify_owner(task, fence, false, custody.work())
                .await?;
            let (tail, found) = self
                .timer_state_with_work(task, operation, deadline_unix_ms, custody.work())
                .await?;
            if found
                || owner
                    .append(
                        path.clone(),
                        tail,
                        &key,
                        bytes.clone(),
                        crate::distributed::JournalWrite::Fresh,
                        custody.work(),
                    )
                    .await?
            {
                self.verify_owner(task, fence, false, custody.work())
                    .await?;
                if let Some(io) = custody.work().as_deref_mut() {
                    io.check_cancellation()?;
                }
                return Ok(self.clock.now_unix_millis() >= deadline_unix_ms);
            }
        }
    }

    #[cfg(feature = "filesystem")]
    pub(crate) async fn timer_ready(
        &self,
        task: TaskId,
        operation: OperationId,
        deadline: u64,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<bool> {
        let admission = self
            .admission_with_work(OperationId::from_bytes(task.into_bytes()), work)
            .await?;
        drop(admission);
        let (_, retained) = self
            .timer_state_with_work(task, operation, deadline, work)
            .await?;
        if let Some(io) = work.as_deref_mut() {
            io.check_cancellation()?;
        }
        Ok(retained && self.clock.now_unix_millis() >= deadline)
    }

    pub(super) fn timer_path_text(task: TaskId) -> [u8; 54] {
        let mut text = [0_u8; 54];
        let prefix = b"harness/v2/timers/";
        text[..prefix.len()].copy_from_slice(prefix);
        uuid::Uuid::from_bytes(task.into_bytes())
            .hyphenated()
            .encode_lower(&mut text[prefix.len()..]);
        text
    }

    pub(super) fn timer_path_with_work(
        task: TaskId,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(acyclic_stream::StreamPath, u64)> {
        let text = Self::timer_path_text(task);
        let text = std::str::from_utf8(&text).expect("canonical ASCII timer path");
        if let Some(io) = work.as_deref_mut() {
            io.admit_copy(text.len() as u64)?;
            io.admit_allocation(text.len() as u64)?;
            io.retain_owned(text.len() as u64)?;
            io.copy_bytes(text.len() as u64)?;
        }
        let path = acyclic_stream::StreamPath::new(text)?;
        Ok((path, if work.is_some() { text.len() as u64 } else { 0 }))
    }

    pub(super) fn timer_stream(
        &self,
        task: TaskId,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(
        acyclic_stream::Stream<P>,
        Option<crate::runtime::NativeCustody>,
    )> {
        let text = Self::timer_path_text(task);
        crate::store::operations::stream(
            &self.stream,
            std::str::from_utf8(&text).expect("canonical ASCII timer path"),
            work,
        )
    }

    pub(super) async fn timer_state_with_work(
        &self,
        task: TaskId,
        operation: OperationId,
        deadline_unix_ms: u64,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(u64, bool)> {
        let stream = self.timer_stream(task, work)?;
        let tail =
            match crate::store::operations::bounds_with_work(&self.stream, stream.0.path(), work)
                .await?
            {
                Ok(bounds) => bounds.tail,
                Err(error) if error == StreamError::NotFound => 0,
                Err(error) => return Err(crate::store::operations::stream_error_with_work(error, work)),
            };
        let mut after = 0;
        let mut found = false;
        while after < tail {
            let mut page = crate::store::operations::read_records(
                &self.stream,
                stream.0.path(),
                after,
                (tail - after).min(64) as u32,
                work,
            )
            .await?
            .map_err(|error| crate::store::operations::stream_error_with_work(error, work))?;
            let mut count = 0;
            while let Some(record) = page.next(work).await? {
                count += 1;
                if count > 64 {
                    return Err(admission_boundary(
                        work,
                        crate::wire::ErrorCode::Storage,
                        "invalid timer replay page",
                    ));
                }
                if record.sequence != after || after >= tail {
                    return Err(admission_boundary(
                        work,
                        crate::wire::ErrorCode::Storage,
                        "timer replay sequence differs",
                    ));
                }
                let event: TimerEvent = match work.as_deref_mut() {
                    Some(io) => {
                        let (value, owned) = io.decode_json_value(&record.value)?;
                        let decoded = TimerEvent::deserialize(&value).map_err(|_| {
                            admission_boundary(
                                &mut Some(&mut *io),
                                crate::wire::ErrorCode::Storage,
                                "timer replay record is invalid",
                            )
                        });
                        drop(value);
                        let release = io.release_owned(owned);
                        let event = decoded?;
                        release?;
                        event
                    }
                    None => crate::contract::json_from_slice(&record.value)
                        .map_err(|error| Error::Storage(error.to_string()))?,
                };
                drop(record);
                if event.task_id != task || event.deadline_unix_ms == 0 {
                    return Err(admission_boundary(
                        work,
                        crate::wire::ErrorCode::Conflict,
                        "timer history belongs to another task",
                    ));
                }
                if event.operation_id == operation {
                    if found || event.deadline_unix_ms != deadline_unix_ms {
                        return Err(admission_boundary(
                            work,
                            crate::wire::ErrorCode::Conflict,
                            "timer identity reused",
                        ));
                    }
                    found = true;
                }
                after += 1;
            }
            if count == 0 {
                return Err(admission_boundary(
                    work,
                    crate::wire::ErrorCode::Storage,
                    "invalid timer replay page",
                ));
            }
        }
        Ok((tail, found))
    }
}
