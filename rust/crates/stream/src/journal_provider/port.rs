use super::*;
use crate::replicated::{AppendIf, CommandJournal, JournalEntry, JournalFrame, JournalPage};

pub(super) struct HostPort {
    pub(super) journal: HostJournal,
    pub(super) limits: JournalStreamLimits,
    pub(super) configured: bool,
    pub(super) bytes: u64,
}

#[async_trait(?Send)]
impl CommandJournal for HostPort {
    async fn read(&mut self, from: u64, page: usize) -> Result<JournalPage, StreamError> {
        if !self.configured {
            let maximum = u32::try_from(MAX_COMMAND_BYTES + 17).map_err(unavailable)?;
            JsFuture::from(
                self.journal
                    .configure(&self.limits.configuration(), maximum)
                    .map_err(unavailable)?,
            )
            .await
            .map_err(unavailable)?;
            self.configured = true;
        }
        JsFuture::from(self.journal.flush().map_err(unavailable)?)
            .await
            .map_err(unavailable)?;
        let from = u32::try_from(from).map_err(unavailable)?;
        let page = u32::try_from(page).map_err(unavailable)?;
        let value = self.journal.read(from, page).map_err(unavailable)?;
        let values = value.dyn_into::<js_sys::Array>().map_err(unavailable)?;
        if values.length() == 0 || values.length() > page.saturating_add(1) {
            return Err(StreamError::InvalidResponse);
        }
        let metadata = values
            .get(0)
            .dyn_into::<Uint8Array>()
            .map_err(unavailable)?;
        if metadata.length() != 16 {
            return Err(StreamError::InvalidResponse);
        }
        let metadata = metadata.to_vec();
        let head = word(&metadata, 0)?;
        let bytes = word(&metadata, 8)?;
        if head > u64::from(self.limits.commands) || bytes > self.limits.journal_bytes {
            return Err(StreamError::Capacity);
        }
        self.bytes = bytes;
        let mut entries = Vec::with_capacity(values.length() as usize - 1);
        for index in 1..values.length() {
            let value = values
                .get(index)
                .dyn_into::<Uint8Array>()
                .map_err(unavailable)?;
            let length = value.length() as usize;
            if !(18..=MAX_COMMAND_BYTES + 17).contains(&length) {
                return Err(StreamError::InvalidResponse);
            }
            let frame = value.to_vec();
            let at_millis = word(&frame, 0)?;
            let deadline = word(&frame, 9)?;
            match frame.get(8) {
                Some(0) if deadline == 0 => {}
                Some(1) if at_millis < deadline => {}
                _ => return Err(StreamError::InvalidResponse),
            }
            entries.push(JournalEntry {
                sequence: u64::from(from) + u64::from(index),
                at_millis,
                command: OwnedBytes::from(frame).slice(17..),
            });
        }
        Ok(JournalPage {
            entries,
            head,
            at_millis: SystemUnixMillisClock.now_unix_millis(),
        })
    }

    async fn append_if(
        &mut self,
        expected: u64,
        frames: &[JournalFrame<'_>],
    ) -> Result<AppendIf, StreamError> {
        if frames.is_empty() {
            return Err(StreamError::InvalidArgument);
        }
        let expected = u32::try_from(expected).map_err(unavailable)?;
        let count = u32::try_from(frames.len()).map_err(unavailable)?;
        let next = expected
            .checked_add(count)
            .filter(|value| *value <= self.limits.commands)
            .ok_or(StreamError::Capacity)?;
        let bytes = frames
            .iter()
            .try_fold(self.bytes, |total, frame| {
                total.checked_add(frame.command.encoded_len() as u64 + 17)
            })
            .filter(|bytes| *bytes <= self.limits.journal_bytes)
            .ok_or(StreamError::Capacity)?;
        let changes = js_sys::Array::new();
        for frame in frames {
            for (path, subtree) in &frame.changes {
                let change = js_sys::Array::new();
                change.push(&JsValue::from_str(path.as_str()));
                change.push(&JsValue::from_bool(*subtree));
                changes.push(&change);
            }
        }
        let accepted = Rc::new(Cell::new(None));
        let sampled = accepted.clone();
        let at_millis = frames.last().ok_or(StreamError::InvalidArgument)?.at_millis;
        let deadline = frames.iter().filter_map(|frame| frame.deadline).min();
        // Only owned encoded bytes escape into the host's deferred factory.
        let encoded: Vec<_> = frames
            .iter()
            .map(|frame| {
                let mut encoded = Vec::with_capacity(17 + frame.command.encoded_len());
                encoded.extend_from_slice(&frame.at_millis.to_le_bytes());
                encoded.push(u8::from(frame.deadline.is_some()));
                encoded.extend_from_slice(&frame.deadline.unwrap_or(0).to_le_bytes());
                frame.command.encode(&mut encoded);
                encoded
            })
            .collect();
        let factory = Closure::<dyn FnMut() -> Result<js_sys::Array, JsValue>>::new(move || {
            if sampled.get().is_some() {
                return Err(JsValue::from_str("STEP journal-accept failed: repeated frame admission; recovery: reopen and replay the original Stream request"));
            }
            let at = SystemUnixMillisClock.now_unix_millis();
            sampled.set(Some(at));
            if deadline.is_some_and(|deadline| at >= deadline) {
                return Err(JsValue::from_str("STEP journal-accept refused: deadline elapsed; recovery: inspect the original request identity before submitting another operation"));
            }
            let output = js_sys::Array::new();
            for encoded in &encoded {
                output.push(&Uint8Array::from(encoded.as_slice()));
            }
            Ok(output)
        }).into_js_value();
        // JavaScript retains the factory across a cancelled Rust waiter. A
        // later refresh replays any accepted transaction before serving state.
        let appended = JsFuture::from(
            self.journal
                .append(expected, factory.unchecked_ref(), &changes)
                .map_err(unavailable)?,
        )
        .await
        .map_err(unavailable);
        if appended.is_err()
            && accepted
                .get()
                .is_some_and(|at| deadline.is_some_and(|deadline| at >= deadline))
        {
            return Err(StreamError::DeadlineElapsed);
        }
        let head = appended?
            .as_f64()
            .filter(|head| head.fract() == 0.0 && *head >= 0.0 && *head <= f64::from(u32::MAX))
            .ok_or(StreamError::InvalidResponse)? as u32;
        let Some(_) = accepted.get() else {
            return Ok(AppendIf::Stale {
                head: u64::from(head),
            });
        };
        if head != next {
            return Err(StreamError::InvalidResponse);
        }
        self.bytes = bytes;
        Ok(AppendIf::Accepted {
            sequence: u64::from(head),
            at_millis,
        })
    }

    fn changed(&mut self, after: u64) -> BoxProviderFuture<'static, Result<(), StreamError>> {
        let (send, received) = futures::channel::oneshot::channel();
        let mut send = Some(send);
        let callback = Closure::<dyn FnMut()>::new(move || {
            if let Some(send) = send.take() {
                let _ = send.send(());
            }
        })
        .into_js_value();
        let registration = u32::try_from(after).map_err(unavailable).and_then(|after| {
            self.journal
                .changed(after, callback.unchecked_ref())
                .map_err(unavailable)
        });
        Box::pin(async move {
            let registration = Registration {
                remove: registration?,
                _callback: callback,
            };
            let result = received.await.map_err(unavailable);
            drop(registration);
            result
        })
    }
}

fn word(bytes: &[u8], start: usize) -> Result<u64, StreamError> {
    Ok(u64::from_le_bytes(
        bytes
            .get(start..start + 8)
            .ok_or(StreamError::InvalidResponse)?
            .try_into()
            .map_err(unavailable)?,
    ))
}

struct Registration {
    remove: js_sys::Function,
    _callback: JsValue,
}
impl Drop for Registration {
    fn drop(&mut self) {
        let _ = self.remove.call0(&JsValue::NULL);
    }
}
