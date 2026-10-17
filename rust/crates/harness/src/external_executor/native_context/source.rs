//! Native source chunks are selected together at one task-volume cursor.
use super::*;
use crate::task_volume::{FileKey, InputPath, Query, TaskVolume};
use acyclic_stream::StreamProvider;
use sha2::{Digest as _, Sha256};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Selection {
    pub manifest: InputPath,
    pub head: Option<String>,
    pub sha256: [u8; 32],
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    chunks: u32,
    length: u64,
}

impl Selection {
    pub(crate) async fn publish<P, F, Fut>(
        &self,
        requests: &TaskVolume<P>,
        results: &TaskVolume<P>,
        source: (&str, u64, acyclic_stream::CommitCondition),
        file: &crate::task_volume::FileValue,
        mut owner: F,
    ) -> Result<()>
    where
        P: StreamProvider + ?Sized,
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<(acyclic_stream::CommitCondition, Option<u64>)>>,
    {
        let generation = results
            .head()
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
        let mut first = Some(source.2);
        crate::task_volume::publish(results, generation, file, || {
            let owner = owner();
            let first = first.take();
            async move {
                let condition = match first {
                    Some(condition) => condition,
                    None => self.read(requests, source.0, source.1).await?.1,
                };
                let (owner, deadline) = owner.await?;
                Ok::<_, Error>((vec![owner, condition], deadline))
            }
        })
        .await?;
        Ok(())
    }

    pub(crate) fn prefix(&self, session: &str) -> Result<String> {
        if uuid::Uuid::parse_str(session).is_err() {
            return Err(invalid());
        }
        let prefix = format!("workspace/native/{session}/{}", hex::encode(self.sha256));
        if self.manifest.as_str() != format!("{prefix}/manifest.json") {
            return Err(invalid());
        }
        Ok(prefix)
    }

    pub(crate) async fn read<P: StreamProvider + ?Sized>(
        &self,
        files: &TaskVolume<P>,
        session: &str,
        maximum: u64,
    ) -> Result<(Vec<u8>, acyclic_stream::CommitCondition)> {
        let prefix = self.prefix(session)?;
        let key = FileKey::Input(self.manifest.clone());
        let first = files
            .snapshot(&Query::File(key.clone()), 1)
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
        let file = first.files.first().ok_or_else(invalid)?;
        let manifest: Manifest = decode(&file.bytes, acyclic_stream::MAX_RECORD_BYTES as u64)?;
        if manifest.length > maximum || u64::from(manifest.chunks) != manifest.length.div_ceil(8192)
        {
            return Err(invalid());
        }
        let mut keys = Vec::with_capacity(manifest.chunks as usize + 1);
        keys.push(key.clone());
        for index in 0..manifest.chunks {
            keys.push(FileKey::parse(&format!("{prefix}/chunk-{index}")).map_err(|_| invalid())?);
        }
        // One index cut includes the manifest and every selected chunk. No
        // project/history scan and no per-chunk provider read is necessary.
        let snapshot = files
            .snapshot_files(&keys, &Query::File(key))
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
        if snapshot.files.len() != keys.len() || snapshot.files.first() != Some(file) {
            return Err(invalid());
        }
        let mut bytes =
            Vec::with_capacity(usize::try_from(manifest.length).map_err(|_| invalid())?);
        for (index, chunk) in snapshot.files.iter().skip(1).enumerate() {
            if chunk.key != keys[index + 1]
                || chunk.bytes.len() as u64 != (manifest.length - bytes.len() as u64).min(8192)
            {
                return Err(invalid());
            }
            bytes.extend_from_slice(&chunk.bytes);
        }
        if bytes.len() as u64 != manifest.length || Sha256::digest(&bytes).as_slice() != self.sha256
        {
            return Err(invalid());
        }
        Ok((
            bytes,
            acyclic_stream::CommitCondition::Head {
                path: files.path().clone(),
                expected: snapshot.generation.position,
                commit_id: acyclic_stream::CommitId::from_bytes(snapshot.generation.commit),
            },
        ))
    }

    pub(crate) fn select<'a>(
        &self,
        bytes: &'a [u8],
        session: &str,
        workspace: &std::path::Path,
        invocation: &str,
    ) -> Result<(SessionFile<'a>, ContextCut)> {
        self.prefix(session)?;
        if Sha256::digest(bytes).as_slice() != self.sha256 {
            return Err(invalid());
        }
        let source = SessionFile::decode(bytes, bytes.len() as u64)?;
        let cut = source.cut(self.head.as_deref(), invocation)?;
        if cut.session != session
            || std::path::Path::new(&cut.workspace) != workspace
            || cut.tool != "acyclic_spawn"
        {
            return Err(invalid());
        }
        Ok((source, cut))
    }
}

#[cfg(any(test, feature = "test-support"))]
pub(super) fn fuzz(bytes: &[u8]) {
    if let Ok(value) = decode::<Selection>(bytes, acyclic_stream::MAX_RECORD_BYTES as u64) {
        let encoded = serde_json::to_vec(&value).unwrap();
        assert_eq!(
            decode::<Selection>(&encoded, encoded.len() as u64).unwrap(),
            value
        );
        let _ = value.prefix("00000000-0000-0000-0000-000000000001");
    }
    if let Ok(value) = decode::<Manifest>(bytes, acyclic_stream::MAX_RECORD_BYTES as u64) {
        let encoded = serde_json::to_vec(&value).unwrap();
        let restored: Manifest = decode(&encoded, encoded.len() as u64).unwrap();
        assert_eq!(
            (value.chunks, value.length),
            (restored.chunks, restored.length)
        );
    }
}

#[cfg(all(test, acyclic_harness_loom))]
mod models;
