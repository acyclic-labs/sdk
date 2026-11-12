use super::*;

const FAMILY: &str = "materialized-metadata-v1";

#[derive(Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct Fact {
    version: u32,
    file_id: crate::FileId,
    metadata: crate::WorkspaceMetadata,
}

fn key(root: crate::NativeRootIdentity, file: crate::NativeRootIdentity) -> [u8; 32] {
    let mut key = [0; 32];
    key[..16].copy_from_slice(&root.to_bytes());
    key[16..].copy_from_slice(&file.to_bytes());
    key
}

impl LocalCoreStateStore {
    pub(crate) async fn materialized_metadata(
        &self,
        root: crate::NativeRootIdentity,
        file: crate::NativeRootIdentity,
        file_id: crate::FileId,
    ) -> Result<Option<crate::WorkspaceMetadata>, LocalCoreStateStoreError> {
        let key = key(root, file);
        self.read_transaction(move |namespace| {
            let mut paths = namespace.record(FAMILY, &key)?;
            let read = |paths: &RecordPaths<'_>| {
                read_recoverable_mode(paths, !namespace.inspection)
                    .map(|value: Option<(Fact, _)>| value.map(|(fact, _)| fact))
            };
            let fact = if namespace.inspection {
                inspection::with_record(&mut paths, read)?
            } else {
                with_lock(&mut paths, read)?
            };
            fact.map(|fact| {
                if fact.version != 1 {
                    return Err(LocalCoreStateStoreError::Integrity);
                }
                Ok((fact.file_id == file_id).then_some(fact.metadata))
            })
            .transpose()
            .map(Option::flatten)
        })
        .await
    }

    pub(crate) fn record_materialized_metadata(
        &self,
        root: crate::NativeRootIdentity,
        file: crate::NativeRootIdentity,
        file_id: crate::FileId,
        metadata: crate::WorkspaceMetadata,
    ) -> Result<(), LocalCoreStateStoreError> {
        if self.inspection {
            return Err(LocalCoreStateStoreError::Integrity);
        }
        let key = key(root, file);
        let namespace = self.namespace()?;
        {
            namespace.durable_directory(Path::new(FAMILY))?;
            let mut paths = namespace.record(FAMILY, &key)?;
            with_lock(&mut paths, |paths| {
                let current = read_recoverable::<Fact>(paths)?;
                if current.as_ref().is_some_and(|(fact, _)| fact.version != 1) {
                    return Err(LocalCoreStateStoreError::Integrity);
                }
                let next = Fact {
                    version: 1,
                    file_id,
                    metadata,
                };
                if current.as_ref().is_none_or(|(fact, _)| *fact != next) {
                    write_journaled(paths, &next)?;
                }
                Ok(())
            })
        }
    }
}
