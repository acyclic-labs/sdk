use super::*;

/// Immutable file plus optional user-visible label.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Attachment {
    /// Version-pinned file.
    pub file: FileRef,
    /// Optional caption; never interpreted as a path.
    pub label: Option<String>,
}

impl Attachment {
    /// Validates metadata under the caller's optional work observer.
    pub fn validate(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.file.validate(work)?;
        if let Some(label) = &self.label {
            validate_label(label, work)?;
        }
        Ok(())
    }
}

/// Encodes a validated, ordered attachment list in its single durable wire form.
pub fn encode_attachment_manifest(items: &[Attachment]) -> Result<Vec<u8>> {
    if items.len() as u64 > MAX_PORTABLE_COUNT as u64 {
        return Err(Error::Invalid("attachment count exceeds limit".into()));
    }
    for item in items {
        item.validate(&mut None)?;
    }
    serde_json::to_vec(items).map_err(|error| Error::Invalid(error.to_string()))
}

#[cfg(feature = "filesystem")]
pub(crate) fn manifest_owned_bytes(items: &[Attachment], capacity: usize) -> Result<u64> {
    let mut bytes = capacity
        .checked_mul(std::mem::size_of::<Attachment>())
        .ok_or_else(|| Error::Invalid("manifest vector ownership overflow".into()))?
        as u64;
    for item in items {
        bytes = bytes
            .checked_add(item.file.logical_owned_bytes()?)
            .and_then(|bytes| {
                bytes.checked_add(
                    item.label
                        .as_ref()
                        .map_or(0, |label| label.capacity() as u64),
                )
            })
            .ok_or_else(|| Error::Invalid("manifest field ownership overflow".into()))?;
    }
    Ok(bytes)
}

#[cfg(feature = "filesystem")]
fn guard<T, E: serde::de::Error>(
    result: Result<T>,
    failure: &mut Option<Error>,
) -> std::result::Result<T, E> {
    result.map_err(|error| {
        *failure = Some(error);
        E::custom("manifest admission interrupted")
    })
}
#[cfg(feature = "filesystem")]
struct ManifestSeed<'a> {
    maximum: usize,
    io: &'a crate::filesystem::PreparationIo,
    failure: &'a mut Option<Error>,
}
#[cfg(feature = "filesystem")]
impl<'de> serde::de::DeserializeSeed<'de> for ManifestSeed<'_> {
    type Value = Vec<Attachment>;
    fn deserialize<D: Deserializer<'de>>(
        self,
        decoder: D,
    ) -> std::result::Result<Self::Value, D::Error> {
        decoder.deserialize_seq(self)
    }
}
#[cfg(feature = "filesystem")]
impl<'de> serde::de::Visitor<'de> for ManifestSeed<'_> {
    type Value = Vec<Attachment>;
    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a bounded complete attachment array")
    }
    fn visit_seq<S: serde::de::SeqAccess<'de>>(
        self,
        mut sequence: S,
    ) -> std::result::Result<Self::Value, S::Error> {
        let mut items: Vec<Attachment> = Vec::new();
        loop {
            guard::<_, S::Error>(self.io.remaining().map_err(Into::into), self.failure)?;
            if items.len() == self.maximum {
                if sequence.next_element::<serde::de::IgnoredAny>()?.is_some() {
                    return guard::<_, S::Error>(
                        Err(self
                            .io
                            .invalid("attachment manifest exceeds member allowance")),
                        self.failure,
                    );
                }
                break;
            }
            let Some(item) = sequence.next_element::<Attachment>()? else {
                break;
            };
            guard::<_, S::Error>(
                item.validate(&mut Some(&mut self.io.history_io())),
                self.failure,
            )?;
            if items.len() == items.capacity() {
                let capacity = items.capacity().saturating_mul(2).max(4).min(self.maximum);
                let bytes = guard::<_, S::Error>(
                    capacity
                        .checked_mul(std::mem::size_of::<Attachment>())
                        .map(|bytes| bytes as u64)
                        .ok_or_else(|| self.io.invalid("manifest vector capacity overflow")),
                    self.failure,
                )?;
                guard::<_, S::Error>(self.io.allocation_attempt(bytes), self.failure)?;
                items.reserve_exact(capacity - items.len());
            }
            items.push(item);
        }
        Ok(items)
    }
}
/// Decodes one complete canonical manifest under the optional content ledger.
pub fn decode_attachment_manifest(
    reference: &FileRef,
    bytes: &[u8],
    item_count: Option<u32>,
    io: Option<&ContentIo>,
) -> Result<Vec<Attachment>> {
    crate::executor::with_history_io(io, |work| {
        reference.validate(work)?;
        if reference.descriptor().media_type() != "application/vnd.acyclic.harness.attachments+json"
        {
            return Err(content_error(
                io,
                crate::wire::ErrorCode::Invalid,
                "attachment manifest metadata or allowance is invalid",
            ));
        }
        reference.descriptor().verify(bytes, work)
    })?;
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        let mut failure = None;
        let result = io.decode(
            ManifestSeed {
                maximum: item_count.map_or(MAX_PORTABLE_COUNT, |count| count as usize),
                io,
                failure: &mut failure,
            },
            bytes,
            crate::wire::ErrorCode::Invalid,
        );
        let mut decoded = result.map_err(|transport| failure.take().unwrap_or(transport))?;
        let actual = (std::mem::size_of::<Vec<Attachment>>() as u64)
            .checked_add(manifest_owned_bytes(&decoded.0, decoded.0.capacity())?)
            .ok_or_else(|| io.invalid("manifest ownership overflow"))?;
        decoded.1.substitute(actual)?;
        let encoded_hold;
        let encoded = io.encode(&decoded.0, io.remaining()?.bytes_encoded, false)?;
        encoded_hold = io.guard_bytes(&encoded)?;
        if encoded.as_ref() != bytes {
            return Err(io.invalid("attachment manifest is not canonical or complete"));
        }
        drop(encoded);
        drop(encoded_hold);
        if item_count.is_some_and(|count| decoded.0.len() != count as usize) {
            return Err(io.invalid("attachment manifest count is incomplete"));
        }
        return crate::filesystem::retain_decoded(decoded);
    }
    let items: Vec<Attachment> = crate::contract::json_from_slice(bytes)
        .map_err(|error| Error::Invalid(format!("attachment manifest is invalid: {error}")))?;
    if encode_attachment_manifest(&items)? != bytes {
        return Err(content_error(
            io,
            crate::wire::ErrorCode::Invalid,
            "attachment manifest is not canonical or complete",
        ));
    }
    if item_count.is_some_and(|count| items.len() != count as usize) {
        return Err(content_error(
            io,
            crate::wire::ErrorCode::Invalid,
            "attachment manifest count is incomplete",
        ));
    }
    Ok(items)
}

/// Reads and verifies exact bytes with their retained ownership.
pub async fn verified_content_bytes<'a, V: ContentResidencyVerifier + ?Sized>(
    verifier: &'a V,
    reference: &'a FileRef,
    maximum_bytes: u64,
    io: Option<&'a ContentIo>,
) -> Result<(acyclic_stream::OwnedBytes, Option<ContentHold<'a>>)> {
    if reference.descriptor().byte_length() > maximum_bytes {
        return Err(content_error(
            io,
            crate::wire::ErrorCode::Invalid,
            "content exceeds selected byte allowance",
        ));
    }
    let hold;
    let bytes = verifier.read(reference, maximum_bytes, io)?.await?;
    #[cfg(feature = "filesystem")]
    {
        hold = match io {
            Some(io) => {
                if io.original_native_work().is_none() {
                    io.retain_owned(bytes.retained_capacity().unwrap_or(0) as u64)?;
                }
                io.guard_bytes(&bytes)?
            }
            None => None,
        };
    }
    #[cfg(not(feature = "filesystem"))]
    {
        hold = None;
    }
    crate::executor::with_history_io(io, |work| reference.descriptor().verify(&bytes, work))?;
    Ok((bytes, hold))
}

/// Loads canonical manifest bytes through their owning reader.
pub async fn load_manifest<'a, V: ContentResidencyVerifier + ?Sized>(
    verifier: &'a V,
    reference: &'a FileRef,
    item_count: u32,
    io: Option<&'a ContentIo>,
) -> Result<(Vec<Attachment>, Option<ContentHold<'a>>)> {
    if !verifier.supports_read(reference.volume(), io)? {
        return Err(content_error(
            io,
            crate::wire::ErrorCode::Unsupported,
            "manifest reader is unavailable",
        ));
    }
    let _bytes_hold;
    let bytes = verifier
        .read(reference, reference.descriptor().byte_length(), io)?
        .await?;
    #[cfg(feature = "filesystem")]
    {
        _bytes_hold = match io {
            Some(io) => {
                if io.original_native_work().is_none() {
                    io.retain_owned(bytes.retained_capacity().unwrap_or(0) as u64)?;
                }
                io.guard_bytes(&bytes)?
            }
            None => None,
        };
    }
    #[cfg(not(feature = "filesystem"))]
    {
        _bytes_hold = ();
    }
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        let items = decode_attachment_manifest(reference, &bytes, Some(item_count), Some(io))?;
        let retained = std::mem::size_of::<Vec<Attachment>>() as u64
            + manifest_owned_bytes(&items, items.capacity())?;
        return Ok((items, Some(io.guard_owned(retained)?)));
    }
    Ok((
        decode_attachment_manifest(reference, &bytes, Some(item_count), io)?,
        None,
    ))
}

/// Resolves a canonical manifest and verifies all members on the same ledger.
pub async fn verified_attachment_manifest<'a, V: ContentResidencyVerifier + ?Sized>(
    verifier: &'a V,
    reference: &'a FileRef,
    item_count: u32,
    limits: &'a Limits,
    io: Option<&'a ContentIo>,
) -> Result<(Vec<Attachment>, Option<ContentHold<'a>>)> {
    crate::executor::with_history_io(io, |work| {
        limits.validate_file(reference, work)?;
        if item_count as usize > limits.attachments {
            return Err(conversation_error(
                work,
                crate::wire::ErrorCode::Invalid,
                "attachment manifest count exceeds limits",
            ));
        }
        Ok(())
    })?;
    let output = load_manifest(verifier, reference, item_count, io).await?;
    for item in &output.0 {
        crate::executor::with_history_io(io, |work| {
            if let Some(observer) = work.as_deref_mut() {
                observer
                    .codec_charge(crate::executor::json::JsonCodecCharge::Inspect { items: 1 })?;
            }
            limits.validate_file(&item.file, work)
        })?;
        verified_content_bytes(verifier, &item.file, limits.file_bytes, io).await?;
    }
    Ok(output)
}

/// A complete ordered attachment list, inline or in one version-pinned file.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[allow(
    clippy::large_enum_variant,
    reason = "manifest refs retain the public tagged enum contract"
)]
pub enum ReferencedAttachments {
    /// Bounded small list retained directly in the event.
    Inline {
        /// Complete ordered attachments.
        #[serde(deserialize_with = "deserialize_inline_attachments")]
        items: Vec<Attachment>,
    },
    /// Complete canonical JSON list retained as a file, never a partial page.
    Manifest {
        /// Version-pinned file containing a canonical JSON array.
        #[serde(deserialize_with = "deserialize_manifest_file")]
        manifest: FileRef,
        /// Exact number of list members.
        item_count: u32,
    },
}

impl ReferencedAttachments {
    /// Validates metadata under the caller's optional work observer.
    pub fn validate(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        match self {
            Self::Inline { items } => {
                for item in items {
                    item.validate(work)?;
                }
            }
            Self::Manifest { manifest, .. } => {
                manifest.validate(work)?;
                if manifest.descriptor().media_type()
                    != "application/vnd.acyclic.harness.attachments+json"
                {
                    return Err(conversation_error(
                        work,
                        crate::wire::ErrorCode::Invalid,
                        "attachment manifest metadata is invalid",
                    ));
                }
            }
        }
        Ok(())
    }
}
