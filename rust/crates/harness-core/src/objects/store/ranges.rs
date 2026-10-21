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
        let key = self.content_key(file)?;
        if range.length == 0 {
            let observed = self
                .objects
                .head(wire::HeadObjectRequest {
                    bucket: Some(self.bucket.clone()),
                    object_key: key,
                    ..Default::default()
                })
                .await
                .map_err(storage)?;
            let info = observed
                .object
                .ok_or_else(|| Error::Storage("Objects returned no content metadata".into()))?;
            acyclic_objects::v1::response::object_info(&info).map_err(storage)?;
            self.verify_metadata(file, &info, true)?;
            return Ok(acyclic_stream::OwnedBytes::from_static(b""));
        }
        let selection = Some(wire::ByteRange {
            selection: Some(wire::byte_range::Selection::Bytes(wire::InclusiveRange {
                start: range.offset,
                end: Some(end - 1),
            })),
        });
        // One get returns the body and its authoritative representation ETag
        // together. The reference pins that ETag after a full verified upload;
        // a replacement cannot pass the same check, including between calls.
        let observed = self
            .objects
            .get(
                wire::GetObjectRequest {
                    bucket: Some(self.bucket.clone()),
                    object_key: key,
                    range: selection,
                    ..Default::default()
                },
                range.length,
            )
            .await
            .map_err(storage)?;
        let length =
            acyclic_objects::v1::response::get_header(observed.header(), &selection, range.length)
                .map_err(storage)?;
        if length != range.length || observed.body().len() as u64 != range.length {
            return Err(Error::Storage("Objects returned another interval".into()));
        }
        let info = observed
            .header()
            .object
            .as_ref()
            .ok_or_else(|| Error::Storage("Objects returned no content metadata".into()))?;
        self.verify_metadata(file, info, true)?;
        Ok(observed.into_body())
    }
}
