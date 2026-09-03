//! Ordered Stream provider contract and deterministic in-memory implementation.

use acyclic_contracts::{Error, Result};
use async_trait::async_trait;
use std::{collections::BTreeMap, sync::Arc};
use tokio::sync::RwLock;

/// An ordered stream record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Record {
    /// Zero-based stream offset.
    pub offset: u64,
    /// Opaque record payload.
    pub payload: Vec<u8>,
}

/// Provider interface for ordered append-only streams.
#[async_trait]
pub trait StreamProvider: Send + Sync {
    /// Appends when the current length matches `expected_offset`.
    async fn append(&self, stream: &str, expected_offset: u64, payload: Vec<u8>) -> Result<Record>;
    /// Reads records at or after an offset.
    async fn read(&self, stream: &str, from: u64) -> Result<Vec<Record>>;
}

/// Process-local stream provider.
#[derive(Clone, Default)]
pub struct MemoryStream {
    streams: Arc<RwLock<BTreeMap<String, Vec<Record>>>>,
}

#[async_trait]
impl StreamProvider for MemoryStream {
    async fn append(&self, stream: &str, expected_offset: u64, payload: Vec<u8>) -> Result<Record> {
        let mut streams = self.streams.write().await;
        let records = streams.entry(stream.into()).or_default();
        if records.len() as u64 != expected_offset {
            return Err(Error::Conflict(stream.into()));
        }
        let record = Record {
            offset: expected_offset,
            payload,
        };
        records.push(record.clone());
        Ok(record)
    }

    async fn read(&self, stream: &str, from: u64) -> Result<Vec<Record>> {
        let streams = self.streams.read().await;
        Ok(streams.get(stream).map_or_else(Vec::new, |items| {
            items
                .iter()
                .filter(|item| item.offset >= from)
                .cloned()
                .collect()
        }))
    }
}
