use super::*;

impl ObjectContentStore {
    pub(super) async fn read_interval(
        &self,
        file: &FileRef,
        range: acyclic_fs::ByteRange,
    ) -> Result<acyclic_stream::OwnedBytes> {
        ContentGrant::verify_read(&self.verifier, &self.scope, file)?;
        let end = range
            .offset
            .checked_add(range.length)
            .filter(|end| *end <= file.descriptor().byte_length())
            .ok_or_else(|| Error::Invalid("Objects range exceeds the file".into()))?;
        let digest = self.content_digest(file)?;
        if range.length == 0 {
            let head = self.objects.head(digest).await.map_err(storage)?;
            self.verify_length(file, head.length)?;
            return Ok(acyclic_stream::OwnedBytes::from_static(b""));
        }
        // The version names immutable content, so a range needs no other pin.
        let observed = self
            .objects
            .get(digest, ByteRange::new(range.offset, end), range.length)
            .await
            .map_err(storage)?;
        self.verify_length(file, observed.head().length)?;
        if observed.start() != range.offset || observed.body().len() as u64 != range.length {
            return Err(Error::Storage("Objects returned another interval".into()));
        }
        Ok(observed.into_body())
    }
}
