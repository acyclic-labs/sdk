//! Proc-macro UniFFI comparison over the same canonical `MemoryStream` provider.
//!
//! This module deliberately exposes a polling object. UniFFI has no first-class Rust Stream
//! mapping, so this keeps the runtime and cancellation semantics explicit for generated Python.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU8, Ordering},
};
use std::time::Duration;

use acyclic_stream::{AppendOutcome, StreamClient, StreamError};
use bytes::Bytes;
use futures::StreamExt as _;
use tokio::task::JoinHandle;

use crate::{AcyclicEngine, EngineRuntime};

#[derive(Debug, thiserror::Error, uniffi::Error)]
pub enum UniFfiStreamError {
    #[error("provider error: {0}")]
    Provider(String),
    #[error("synchronous binding cannot run from a Tokio runtime")]
    RuntimeReentry,
}

impl From<StreamError> for UniFfiStreamError {
    fn from(error: StreamError) -> Self {
        Self::Provider(error.to_string())
    }
}

#[derive(Clone, Debug, uniffi::Record)]
pub struct UniFfiAppendReceipt {
    pub start: u64,
    pub end: u64,
    pub tail: u64,
}

#[derive(Clone, Debug, uniffi::Enum)]
pub enum UniFfiPollResult {
    Record { sequence: u64, value: Vec<u8> },
    End,
    Pending,
    Cancelled,
}

enum Message {
    Record(acyclic_stream::Record),
    End,
}

const TERMINAL_NONE: u8 = 0;
const TERMINAL_END: u8 = 1;
const TERMINAL_CANCELLED: u8 = 2;

#[derive(uniffi::Object)]
pub struct UniFfiStreamProbe {
    engine: Arc<AcyclicEngine>,
}

#[derive(uniffi::Object)]
pub struct UniFfiPollingReader {
    engine: Arc<AcyclicEngine>,
    receiver: Mutex<std::sync::mpsc::Receiver<Message>>,
    cancelled: Arc<AtomicBool>,
    terminal: Arc<AtomicU8>,
    task: Mutex<Option<JoinHandle<()>>>,
}

fn runtime_reentry() -> bool {
    tokio::runtime::Handle::try_current().is_ok()
}

#[uniffi::export]
impl UniFfiStreamProbe {
    #[uniffi::constructor]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            engine: Arc::new(AcyclicEngine {
                runtime: EngineRuntime::new().expect("Tokio runtime"),
                provider: Arc::new(acyclic_stream::MemoryStream::new(
                    acyclic_stream::MemoryLimits::default(),
                )),
            }),
        })
    }

    pub fn append(
        &self,
        path: String,
        value: Vec<u8>,
    ) -> Result<UniFfiAppendReceipt, UniFfiStreamError> {
        if runtime_reentry() {
            return Err(UniFfiStreamError::RuntimeReentry);
        }
        match self.engine.runtime.block_on(async {
            StreamClient::new(Arc::clone(&self.engine.provider))
                .stream(path)?
                .append(Bytes::from(value))
                .await
        })? {
            AppendOutcome::Committed(receipt) => Ok(UniFfiAppendReceipt {
                start: receipt.start,
                end: receipt.end,
                tail: receipt.tail,
            }),
            AppendOutcome::TailConflict { actual_tail } => Err(UniFfiStreamError::Provider(
                format!("tail conflict at {actual_tail}"),
            )),
        }
    }

    pub fn open_reader(
        &self,
        path: String,
        from: u64,
        follow: bool,
    ) -> Result<Arc<UniFfiPollingReader>, UniFfiStreamError> {
        if runtime_reentry() {
            return Err(UniFfiStreamError::RuntimeReentry);
        }
        let stream = self.engine.runtime.block_on(async {
            let stream = StreamClient::new(Arc::clone(&self.engine.provider)).stream(path)?;
            if follow {
                Ok::<_, UniFfiStreamError>(stream.follow(from).await?)
            } else {
                Ok::<_, UniFfiStreamError>(stream.read(from, 64).await?)
            }
        })?;
        let (sender, receiver) = std::sync::mpsc::sync_channel(66);
        let cancelled = Arc::new(AtomicBool::new(false));
        let terminal = Arc::new(AtomicU8::new(TERMINAL_NONE));
        let task_cancelled = Arc::clone(&cancelled);
        let task = self.engine.runtime.spawn(async move {
            let mut stream = stream;
            loop {
                if task_cancelled.load(Ordering::Acquire) {
                    return;
                }
                match stream.next().await {
                    Some(Ok(record)) => {
                        if sender.send(Message::Record(record)).is_err() {
                            return;
                        }
                    }
                    Some(Err(_)) | None => {
                        let _ = sender.send(Message::End);
                        return;
                    }
                }
            }
        });
        Ok(Arc::new(UniFfiPollingReader {
            engine: Arc::clone(&self.engine),
            receiver: Mutex::new(receiver),
            cancelled,
            terminal,
            task: Mutex::new(Some(task)),
        }))
    }
}

#[uniffi::export]
impl UniFfiPollingReader {
    pub fn next(&self) -> Result<UniFfiPollResult, UniFfiStreamError> {
        if runtime_reentry() {
            return Err(UniFfiStreamError::RuntimeReentry);
        }
        match self.terminal.load(Ordering::Acquire) {
            TERMINAL_END => return Ok(UniFfiPollResult::End),
            TERMINAL_CANCELLED => return Ok(UniFfiPollResult::Cancelled),
            _ => {}
        }
        self.engine.runtime.block_on(async {
            tokio::task::yield_now().await;
            tokio::time::sleep(Duration::from_millis(2)).await
        });
        let result = {
            let receiver = self
                .receiver
                .lock()
                .map_err(|_| UniFfiStreamError::Provider("reader mutex poisoned".into()))?;
            receiver.recv_timeout(Duration::from_millis(25))
        };
        match self.terminal.load(Ordering::Acquire) {
            TERMINAL_END => return Ok(UniFfiPollResult::End),
            TERMINAL_CANCELLED => return Ok(UniFfiPollResult::Cancelled),
            _ => {}
        }
        match result {
            Ok(Message::Record(record)) => Ok(UniFfiPollResult::Record {
                sequence: record.sequence,
                value: record.value.to_vec(),
            }),
            Ok(Message::End) => {
                self.terminal
                    .compare_exchange(
                        TERMINAL_NONE,
                        TERMINAL_END,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .ok();
                Ok(UniFfiPollResult::End)
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => Ok(UniFfiPollResult::Pending),
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                self.terminal
                    .compare_exchange(
                        TERMINAL_NONE,
                        TERMINAL_CANCELLED,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .ok();
                Ok(UniFfiPollResult::Cancelled)
            }
        }
    }

    pub fn cancel(&self) {
        if self.cancelled.swap(true, Ordering::AcqRel) {
            return;
        }
        self.terminal
            .compare_exchange(
                TERMINAL_NONE,
                TERMINAL_CANCELLED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .ok();
        if let Ok(mut task) = self.task.lock() {
            if let Some(task) = task.take() {
                task.abort();
            }
        }
    }
}

impl Drop for UniFfiPollingReader {
    fn drop(&mut self) {
        self.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn generated_style_object_calls_canonical_stream() {
        let probe = UniFfiStreamProbe::new();
        probe
            .append("uniffi/probe".into(), b"hello".to_vec())
            .unwrap();
        let reader = probe.open_reader("uniffi/probe".into(), 0, true).unwrap();
        assert!(matches!(
            reader.next().unwrap(),
            UniFfiPollResult::Record { sequence: 0, .. }
        ));
        reader.cancel();
        assert!(matches!(
            reader.next().unwrap(),
            UniFfiPollResult::Cancelled
        ));
    }
}
