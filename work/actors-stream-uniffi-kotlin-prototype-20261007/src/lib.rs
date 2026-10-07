//! Research-only UniFFI bridge over the real Rust StreamProvider streams.
//!
//! This prototype keeps recovery, ordering, bounds, and transport in
//! acyclic-stream. The foreign boundary owns only an opaque cursor with
//! next/close operations. Dropping or closing a cursor drops the original
//! BoxStream; it does not replay, buffer, or translate recovery behavior.

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

use acyclic_stream::{AppendOutcome, ChildStream, Record, RecordStream, StreamClient, StreamError};
#[cfg(feature = "qualification")]
use acyclic_stream::MemoryStream;
use acyclic_stream::grpc::Client as GrpcStreamClient;
use futures::StreamExt;
use tokio::sync::Notify;

uniffi::setup_scaffolding!();

#[derive(Debug, thiserror::Error, uniffi::Error)]
pub enum StreamBridgeError {
    #[error("{code}: {detail}")]
    Stream { code: String, detail: String },
    #[error("stream cursor is closed")]
    Closed,
}

fn stream_error(error: StreamError) -> StreamBridgeError {
    StreamBridgeError::Stream {
        code: error.code().to_owned(),
        detail: error.to_string(),
    }
}

/// One ordered Rust Stream record with full-width numeric fields preserved.
#[derive(Clone, Debug, uniffi::Record)]
pub struct StreamRecord {
    pub sequence: u64,
    pub value: Vec<u8>,
    pub commit_id: Vec<u8>,
    pub committed_at_micros: u64,
}

/// One direct child returned by the Rust Stream provider.
#[derive(Clone, Debug, uniffi::Record)]
pub struct StreamChild {
    pub path: String,
}

struct RecordCursorState {
    stream: std::sync::Mutex<Option<RecordStream>>,
    closed: AtomicBool,
    active_counted: AtomicBool,
    cancel: Notify,
    active: Arc<AtomicU64>,
}

impl Drop for RecordCursorState {
    fn drop(&mut self) {
        self.closed.store(true, Ordering::Release);
        if self.active_counted.swap(false, Ordering::AcqRel) {
            self.active.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

/// Opaque cursor over the original Rust RecordStream BoxStream.
#[derive(uniffi::Object)]
pub struct RecordCursor {
    state: Arc<RecordCursorState>,
}

impl RecordCursor {
    fn new(stream: RecordStream, active: Arc<AtomicU64>) -> Arc<Self> {
        active.fetch_add(1, Ordering::AcqRel);
        Arc::new(Self {
            state: Arc::new(RecordCursorState {
                stream: std::sync::Mutex::new(Some(stream)),
                closed: AtomicBool::new(false),
                active_counted: AtomicBool::new(true),
                cancel: Notify::new(),
                active,
            }),
        })
    }

    fn take(&self) -> Option<RecordStream> {
        self.state.stream.lock().expect("record cursor mutex").take()
    }

    fn restore(&self, stream: RecordStream) {
        if self.state.closed.load(Ordering::Acquire) {
            drop(stream);
            return;
        }
        let mut guard = self.state.stream.lock().expect("record cursor mutex");
        if self.state.closed.load(Ordering::Acquire) {
            drop(stream);
        } else {
            *guard = Some(stream);
        }
    }

    fn close_now(&self) {
        if !self.state.closed.swap(true, Ordering::AcqRel) {
            self.state.cancel.notify_waiters();
            drop(self.take());
            if self.state.active_counted.swap(false, Ordering::AcqRel) {
                self.state.active.fetch_sub(1, Ordering::AcqRel);
            }
        }
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl RecordCursor {
    /// Pulls one item in provider order. Cancellation drops the pending BoxStream.
    pub async fn next(&self) -> Result<Option<StreamRecord>, StreamBridgeError> {
        let mut stream = self.take().ok_or(StreamBridgeError::Closed)?;
        let item = tokio::select! {
            _ = self.state.cancel.notified() => {
                drop(stream);
                return Err(StreamBridgeError::Closed);
            }
            item = stream.next() => item,
        };
        match item {
            Some(Ok(record)) => {
                self.restore(stream);
                Ok(Some(record.into()))
            }
            Some(Err(error)) => {
                self.close_now();
                Err(stream_error(error))
            }
            None => {
                self.close_now();
                Ok(None)
            }
        }
    }

    /// Closes the cursor and drops the underlying Rust BoxStream.
    pub async fn close_cursor(&self) {
        self.close_now();
    }
}

impl From<Record> for StreamRecord {
    fn from(record: Record) -> Self {
        Self {
            sequence: record.sequence,
            value: record.value.to_vec(),
            commit_id: record.commit_id.as_bytes().to_vec(),
            committed_at_micros: record.committed_at_micros,
        }
    }
}

struct ChildCursorState {
    stream: std::sync::Mutex<Option<ChildStream>>,
    closed: AtomicBool,
    cancel: Notify,
}

#[derive(uniffi::Object)]
pub struct ChildCursor {
    state: Arc<ChildCursorState>,
}

impl ChildCursor {
    fn new(stream: ChildStream) -> Arc<Self> {
        Arc::new(Self {
            state: Arc::new(ChildCursorState {
                stream: std::sync::Mutex::new(Some(stream)),
                closed: AtomicBool::new(false),
                cancel: Notify::new(),
            }),
        })
    }

    fn take(&self) -> Option<ChildStream> {
        self.state.stream.lock().expect("child cursor mutex").take()
    }

    fn restore(&self, stream: ChildStream) {
        if self.state.closed.load(Ordering::Acquire) {
            drop(stream);
            return;
        }
        let mut guard = self.state.stream.lock().expect("child cursor mutex");
        if self.state.closed.load(Ordering::Acquire) {
            drop(stream);
        } else {
            *guard = Some(stream);
        }
    }

    fn close_now(&self) {
        if !self.state.closed.swap(true, Ordering::AcqRel) {
            self.state.cancel.notify_waiters();
            drop(self.take());
        }
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl ChildCursor {
    pub async fn next(&self) -> Result<Option<StreamChild>, StreamBridgeError> {
        let mut stream = self.take().ok_or(StreamBridgeError::Closed)?;
        let item = tokio::select! {
            _ = self.state.cancel.notified() => {
                drop(stream);
                return Err(StreamBridgeError::Closed);
            }
            item = stream.next() => item,
        };
        match item {
            Some(Ok(child)) => {
                self.restore(stream);
                Ok(Some(StreamChild { path: child.path.to_string() }))
            }
            Some(Err(error)) => {
                self.close_now();
                Err(stream_error(error))
            }
            None => {
                self.close_now();
                Ok(None)
            }
        }
    }

    pub async fn close_cursor(&self) {
        self.close_now();
    }
}

/// Research client backed by the actual Rust MemoryStream provider.
///
/// A future production constructor can replace this backend with the gRPC
/// provider without changing cursor semantics. The bridge itself does not
/// implement recovery or transport.
#[cfg(feature = "qualification")]
#[derive(uniffi::Object)]
pub struct StreamClientBridge {
    client: StreamClient<MemoryStream>,
    active_record_cursors: Arc<AtomicU64>,
}

#[cfg(feature = "qualification")]
#[uniffi::export(async_runtime = "tokio")]
impl StreamClientBridge {
    #[uniffi::constructor]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            client: StreamClient::new(Arc::new(MemoryStream::default())),
            active_record_cursors: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Test fixture helper that uses the canonical Rust append algorithm.
    pub async fn append(&self, path: String, value: Vec<u8>) -> Result<u64, StreamBridgeError> {
        let stream = self.client.stream(path).map_err(stream_error)?;
        match stream.append(value).await.map_err(stream_error)? {
            AppendOutcome::Committed(receipt) => Ok(receipt.end),
            AppendOutcome::TailConflict { actual_tail } => Ok(actual_tail),
        }
    }

    /// Opens the actual provider read BoxStream.
    pub async fn read(&self, path: String, from: u64, limit: u32) -> Result<Arc<RecordCursor>, StreamBridgeError> {
        let stream = self.client.stream(path).map_err(stream_error)?;
        let records = stream.read(from, limit).await.map_err(stream_error)?;
        Ok(RecordCursor::new(records, Arc::clone(&self.active_record_cursors)))
    }

    /// Opens the actual provider follow BoxStream.
    pub async fn follow(&self, path: String, from: u64) -> Result<Arc<RecordCursor>, StreamBridgeError> {
        let stream = self.client.stream(path).map_err(stream_error)?;
        let records = stream.follow(from).await.map_err(stream_error)?;
        Ok(RecordCursor::new(records, Arc::clone(&self.active_record_cursors)))
    }

    /// Opens the actual provider children BoxStream.
    pub async fn children(&self, parent: Option<String>, limit: u32) -> Result<Arc<ChildCursor>, StreamBridgeError> {
        let children = self.client.children(parent.as_deref(), limit).await.map_err(stream_error)?;
        Ok(ChildCursor::new(children))
    }

    /// Number of live record cursors, used only by the qualification probe.
    #[cfg(feature = "qualification")]
    pub fn active_record_cursors(&self) -> u64 {
        self.active_record_cursors.load(Ordering::Acquire)
    }
}

/// Research-only bridge over the actual authenticated Rust gRPC Stream client.
///
/// The remote provider and the local provider share the same cursor objects and
/// generated Kotlin surface; only the Rust-owned provider changes.
#[derive(uniffi::Object)]
pub struct RemoteStreamClientBridge {
    client: StreamClient<GrpcStreamClient>,
    active_record_cursors: Arc<AtomicU64>,
}

#[uniffi::export(async_runtime = "tokio")]
impl RemoteStreamClientBridge {
    /// Connects to an authenticated TLS Stream endpoint using the caller-pinned CA.
    #[uniffi::constructor]
    pub async fn connect(
        endpoint: String,
        bearer_token: String,
        ca_certificate_pem: Vec<u8>,
    ) -> Result<Arc<Self>, StreamBridgeError> {
        let provider = GrpcStreamClient::connect_with_ca_certificate(
            endpoint,
            bearer_token,
            ca_certificate_pem,
        )
        .await
        .map_err(|error| StreamBridgeError::Stream {
            code: "unavailable".to_owned(),
            detail: error.to_string(),
        })?;
        Ok(Arc::new(Self {
            client: StreamClient::new(Arc::new(provider)),
            active_record_cursors: Arc::new(AtomicU64::new(0)),
        }))
    }

    /// Opens the actual remote provider read BoxStream.
    pub async fn read(
        &self,
        path: String,
        from: u64,
        limit: u32,
    ) -> Result<Arc<RecordCursor>, StreamBridgeError> {
        let stream = self.client.stream(path).map_err(stream_error)?;
        let records = stream.read(from, limit).await.map_err(stream_error)?;
        Ok(RecordCursor::new(records, Arc::clone(&self.active_record_cursors)))
    }

    /// Opens the actual remote provider follow BoxStream.
    pub async fn follow(
        &self,
        path: String,
        from: u64,
    ) -> Result<Arc<RecordCursor>, StreamBridgeError> {
        let stream = self.client.stream(path).map_err(stream_error)?;
        let records = stream.follow(from).await.map_err(stream_error)?;
        Ok(RecordCursor::new(records, Arc::clone(&self.active_record_cursors)))
    }

    /// Opens the actual remote provider children BoxStream.
    pub async fn children(
        &self,
        parent: Option<String>,
        limit: u32,
    ) -> Result<Arc<ChildCursor>, StreamBridgeError> {
        let children = self
            .client
            .children(parent.as_deref(), limit)
            .await
            .map_err(stream_error)?;
        Ok(ChildCursor::new(children))
    }

    /// Appends through the canonical remote Stream client for fixture setup.
    pub async fn append(
        &self,
        path: String,
        value: Vec<u8>,
    ) -> Result<u64, StreamBridgeError> {
        let stream = self.client.stream(path).map_err(stream_error)?;
        match stream.append(value).await.map_err(stream_error)? {
            AppendOutcome::Committed(receipt) => Ok(receipt.end),
            AppendOutcome::TailConflict { actual_tail } => Ok(actual_tail),
        }
    }

}

#[cfg(feature = "qualification")]
#[uniffi::export]
impl RemoteStreamClientBridge {
    /// Number of live remote record BoxStreams, used only by qualification.
    pub fn active_record_cursors(&self) -> u64 {
        self.active_record_cursors.load(Ordering::Acquire)
    }
}
