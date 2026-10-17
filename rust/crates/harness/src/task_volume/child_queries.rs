//! Lifecycle cuts exclude retained prompts, inbox bodies and earlier answers.
use super::*;

impl<P: StreamProvider + ?Sized> TaskVolume<P> {
    pub(crate) async fn child_references(&self) -> Result<Vec<Component>> {
        self.refresh().await?;
        let index = self.index.lock().await;
        Ok(index
            .child_inputs
            .keys()
            .filter(|reference| {
                index.files.contains_key(&FileKey::Child {
                    reference: (*reference).clone(),
                    file: ChildFile::Request,
                })
            })
            .cloned()
            .collect())
    }

    pub(crate) async fn child_request(&self, reference: &Component) -> Result<FileValue> {
        let key = FileKey::Child {
            reference: reference.clone(),
            file: ChildFile::Request,
        };
        let snapshot = self
            .snapshot_files(&[key.clone()], &Query::File(key))
            .await?;
        snapshot
            .files
            .into_iter()
            .next()
            .ok_or(Error::InvalidSegment)
    }

    pub(crate) async fn child_input_count(&self, reference: &Component) -> Result<u64> {
        self.refresh().await?;
        let index = self.index.lock().await;
        Ok(index
            .child_inputs
            .get(reference)
            .map_or(0, |inputs| inputs.len() as u64))
    }

    // The derived index preserves accepted order, independently of inbox names.
    pub(crate) async fn child_input(
        &self,
        reference: &Component,
        delivered: u64,
    ) -> Result<Option<FileValue>> {
        self.refresh().await?;
        let index = self.index.lock().await;
        let key = index
            .child_inputs
            .get(reference)
            .and_then(|inputs| usize::try_from(delivered).ok().and_then(|i| inputs.get(i)));
        Ok(key
            .and_then(|key| index.files.get(key))
            .map(|stored| stored.file.clone()))
    }

    pub(crate) async fn child_lifecycle(&self, reference: Option<&Component>) -> Result<Snapshot> {
        self.refresh().await?;
        let index = self.index.lock().await;
        let mut files = Vec::new();
        let candidates = match reference {
            Some(reference) => index
                .child_inputs
                .range(reference.clone()..=reference.clone()),
            None => index.child_inputs.range(..),
        };
        for (candidate, _) in candidates {
            let key = |file| FileKey::Child {
                reference: candidate.clone(),
                file,
            };
            for kind in [ChildFile::Control, ChildFile::Status, ChildFile::Delivered] {
                if let Some(stored) = index.files.get(&key(kind)) {
                    files.push(stored.file.clone());
                }
            }
            if let Some((_, stored)) = index
                .files
                .range(key(ChildFile::Result(0))..=key(ChildFile::Result(u64::MAX)))
                .next_back()
            {
                files.push(stored.file.clone());
            }
        }
        let generation = self.generation(&index);
        drop(index);
        let query = reference.map_or(Query::Children, |reference| Query::Child(reference.clone()));
        let watermarks = self.provider.watermarks(self.path.clone()).await?;
        Ok(Snapshot {
            files,
            cursor: self.cursor(&query, generation, watermarks)?,
            generation,
        })
    }
}
