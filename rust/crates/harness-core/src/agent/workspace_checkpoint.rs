//! Immutable, chunked workspace snapshots. The actor publishes only their
//! version-one identity, after every chunk is durable. Failed preparations
//! leave unreachable chunks; they cannot publish a partial table.
use super::{Error, WorkspaceEntry, WorkspaceTable};
use acyclic_stream::{
    AppendOutcome, AppendRequest, ReadRequest, StreamPath, StreamProvider, StreamProviderExt as _,
};
use futures::TryStreamExt as _;
use serde::{Deserialize, Serialize};

// A chunking size, not a bound on entries, snapshots or histories.
const CHUNK_BYTES: usize = 48 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TableRef {
    version: u32,
    digest: [u8; 32],
    bytes: u64,
}

fn encoded(entries: &[WorkspaceEntry]) -> Result<Vec<u8>, Error> {
    let table = WorkspaceTable::new(entries.to_vec())?;
    crate::contract::canonical_json_bytes(&table)
        .map_err(|_| Error::Record("workspace snapshot does not encode"))
}

impl TableRef {
    pub(crate) fn of(entries: &[WorkspaceEntry]) -> Result<Self, Error> {
        Ok(Self::from_bytes(&encoded(entries)?))
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        Self {
            version: 1,
            digest: *blake3::hash(bytes).as_bytes(),
            bytes: bytes.len() as u64,
        }
    }

    fn path(&self) -> Result<StreamPath, Error> {
        if self.version != 1 || self.bytes == 0 {
            return Err(Error::Record("invalid workspace snapshot reference"));
        }
        StreamPath::new(format!(
            "harness/workspace-snapshots/v1/{}",
            blake3::Hash::from_bytes(self.digest).to_hex()
        ))
        .map_err(Error::Stream)
    }

    pub(crate) fn check(&self) -> Result<(), Error> {
        self.path().map(|_| ())
    }

    pub(crate) fn verify(&self, entries: &[WorkspaceEntry]) -> Result<(), Error> {
        if self != &Self::of(entries)? {
            return Err(Error::Record("workspace snapshot identity differs"));
        }
        Ok(())
    }

    /// Write only a changed table, before its actor checkpoint can publish.
    /// Chunk keys and expected tails also make an interrupted preparation
    /// restartable without duplicating any prefix or guessing an outcome.
    pub(crate) async fn persist<P: StreamProvider + ?Sized>(
        &self,
        provider: &P,
        entries: &[WorkspaceEntry],
    ) -> Result<(), Error> {
        let bytes = encoded(entries)?;
        if self != &Self::from_bytes(&bytes) {
            return Err(Error::Record("workspace snapshot identity differs"));
        }
        if entries.is_empty() {
            return Ok(());
        }
        let path = self.path()?;
        for (sequence, chunk) in bytes.chunks(CHUNK_BYTES).enumerate() {
            let sequence = sequence as u64;
            let outcome = provider
                .append(AppendRequest {
                    path: path.clone(),
                    records: vec![chunk.to_vec().into()],
                    if_tail: Some(sequence),
                    idempotency_key: super::intent_key(
                        "workspace-snapshot",
                        &[
                            &blake3::Hash::from_bytes(self.digest).to_hex(),
                            &sequence.to_string(),
                        ],
                    )?,
                })
                .await
                .map_err(Error::Stream)?;
            if matches!(outcome, AppendOutcome::TailConflict { .. }) {
                let mut records = provider
                    .read(ReadRequest {
                        path: path.clone(),
                        from: sequence,
                        limit: 1,
                        key: None,
                        at_least: Default::default(),
                    })
                    .await
                    .map_err(Error::Stream)?;
                let record = records
                    .try_next()
                    .await
                    .map_err(Error::Stream)?
                    .ok_or(Error::Record("workspace snapshot chunk is missing"))?;
                if record.sequence != sequence || record.value.as_ref() != chunk {
                    return Err(Error::Record("workspace snapshot chunk differs"));
                }
            }
        }
        Ok(())
    }

    /// Materialise exactly this folded table; never replay the agent inbox.
    pub(crate) async fn load<P: StreamProvider + ?Sized>(
        &self,
        provider: &P,
    ) -> Result<Vec<WorkspaceEntry>, Error> {
        if self == &Self::of(&[])? {
            return Ok(Vec::new());
        }
        let path = self.path()?;
        let mut bytes = Vec::new();
        let mut from = 0;
        while (bytes.len() as u64) < self.bytes {
            let mut records = provider
                .read(ReadRequest {
                    path: path.clone(),
                    from,
                    limit: 1,
                    key: None,
                    at_least: Default::default(),
                })
                .await
                .map_err(Error::Stream)?;
            let record = records
                .try_next()
                .await
                .map_err(Error::Stream)?
                .ok_or(Error::Record("workspace snapshot chunk is missing"))?;
            let remaining = self.bytes - bytes.len() as u64;
            let expected = remaining.min(CHUNK_BYTES as u64);
            if record.sequence != from || record.value.len() as u64 != expected {
                return Err(Error::Record("workspace snapshot chunk length differs"));
            }
            bytes.extend_from_slice(&record.value);
            from += 1;
        }
        if self != &Self::from_bytes(&bytes) {
            return Err(Error::Record("workspace snapshot digest differs"));
        }
        let table: WorkspaceTable = serde_json::from_slice(&bytes)
            .map_err(|_| Error::Record("workspace snapshot is malformed"))?;
        Ok(table.entries().to_vec())
    }
}

#[cfg(test)]
pub(super) async fn check_cuts(checkpoint: &[u8], state: &super::AgentState) -> Result<(), Error> {
    let head = super::checkpoint::decode(checkpoint)?;
    let bytes = encoded(&state.volumes)?;
    let chunks = bytes.chunks(CHUNK_BYTES).collect::<Vec<_>>();
    for cut in 0..=chunks.len() {
        let stream = acyclic_stream::MemoryStream::default();
        if cut != 0 {
            stream
                .append(AppendRequest {
                    path: head.table.path()?,
                    records: chunks[..cut]
                        .iter()
                        .map(|chunk| chunk.to_vec().into())
                        .collect(),
                    if_tail: Some(0),
                    idempotency_key: super::intent_key("workspace-cut", &[&cut.to_string()])?,
                })
                .await
                .map_err(Error::Stream)?;
        }
        if cut < chunks.len() {
            assert!(
                head.clone().load(&stream).await.is_err(),
                "partial snapshot was admitted"
            );
        }
        head.table.persist(&stream, &state.volumes).await?;
        assert_eq!(head.clone().load(&stream).await?, *state);
        head.table.persist(&stream, &state.volumes).await?;
        assert_eq!(head.clone().load(&stream).await?, *state);
    }
    Ok(())
}
