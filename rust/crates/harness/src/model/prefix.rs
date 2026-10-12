use super::*;

/// Immutable append-only shared model prefix segment.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelPrefix {
    version: u32,
    parent: Option<FileRef>,
    message_digest: [u8; 32],
    total_messages: usize,
    messages: Vec<ModelMessage>,
}
impl ModelPrefix {
    /// Media type of shared prefix segments.
    pub const MEDIA_TYPE: &'static str = "application/vnd.acyclic.harness.model-prefix+json";
    /// Selects all messages or the exact advancing suffix after a pinned parent.
    pub fn select(
        request: &PreparedModelRequest,
        parent: Option<(&FileRef, &PreparedModelRequest)>,
    ) -> Result<Self> {
        let (parent, offset) = if let Some((reference, prior)) = parent {
            reference.validate(&mut None)?;
            if reference.descriptor().media_type() != Self::MEDIA_TYPE
                || !request
                    .request()
                    .messages
                    .starts_with(&prior.request().messages)
            {
                return Err(Error::Invalid(
                    "model prefix changed its parent messages".into(),
                ));
            }
            (Some(reference), prior.request().messages.len())
        } else {
            (None, 0)
        };
        let messages = request
            .request()
            .messages
            .get(offset..)
            .ok_or_else(|| Error::Invalid("model prefix offset is invalid".into()))?;
        if messages.is_empty() {
            return Err(Error::Invalid(
                "model prefix must advance its parent".into(),
            ));
        }
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"harness:model-prefix:ordered:v1");
        for message in &request.request().messages {
            hasher.update(&crate::contract::canonical_json_digest(message)?);
        }
        Ok(Self {
            version: 1,
            parent: parent.cloned(),
            message_digest: *hasher.finalize().as_bytes(),
            total_messages: request.request().messages.len(),
            messages: messages.to_vec(),
        })
    }
    /// Exact canonical bytes staged once for sharing.
    pub fn canonical_bytes(
        &self,
        io: Option<&PreparationIo>,
    ) -> Result<acyclic_stream::OwnedBytes> {
        #[cfg(feature = "filesystem")]
        if let Some(io) = io {
            return io.encode(self, io.remaining()?.bytes_encoded, true);
        }
        let _ = io;
        Ok(crate::contract::canonical_json_bytes(self)?.into())
    }
}

impl PreparedModelRequest {
    /// Resolves and validates the pinned prefix chain on the optional ledger.
    pub async fn inherit(
        mut local: ModelRequest,
        prefix: &FileRef,
        verifier: &dyn crate::conversation::ContentResidencyVerifier,
        limits: Limits,
        io: Option<&PreparationIo>,
    ) -> Result<Self> {
        with_history_io(io, |work| limits.validate(work))?;
        let mut next = Some(clone_file(prefix, io)?);
        let mut segments = Vec::new();
        let mut visited = std::collections::BTreeSet::new();
        let mut expected_count = None;
        while let Some(reference) = next {
            charge_items(io, 1)?;
            with_history_io(io, |work| reference.validate(work))?;
            let identity = crate::executor::preparation_digest(&reference, io)?;
            #[cfg(feature = "filesystem")]
            if let Some(io) = io {
                io.reserve_decode(std::mem::size_of::<[u8; 32]>() as u64)?
                    .into_retained();
            }
            if reference.descriptor().media_type() != ModelPrefix::MEDIA_TYPE
                || !visited.insert(identity)
            {
                return Err(Error::Invalid(
                    "model prefix identity or cycle is invalid".into(),
                ));
            }
            if visited.len() > limits.context_messages {
                return Err(Error::Invalid(
                    "model prefix traversal exceeds limit".into(),
                ));
            }
            if !verifier.supports_read(reference.volume(), io)? {
                return Err(crate::executor::stock_boundary(
                    io,
                    Unsupported,
                    "context verifier has no bounded capability",
                ));
            }
            let _bytes_hold;
            let (bytes, hold) = crate::conversation::verified_content_bytes(
                verifier,
                &reference,
                reference.descriptor().byte_length(),
                io,
            )
            .await?;
            _bytes_hold = hold;
            let segment: ModelPrefix = match io {
                #[cfg(feature = "filesystem")]
                Some(io) => crate::filesystem::retain_decoded(io.decode(
                    std::marker::PhantomData::<ModelPrefix>,
                    &bytes,
                    Storage,
                )?),
                _ => crate::contract::json_from_slice(&bytes)
                    .map_err(|error| Error::Invalid(format!("invalid model prefix: {error}")))?,
            };
            #[cfg(feature = "filesystem")]
            let _canonical_hold;
            let canonical = segment.canonical_bytes(io)?;
            #[cfg(feature = "filesystem")]
            {
                _canonical_hold = io
                    .map(|io| io.guard_bytes(&canonical))
                    .transpose()?
                    .flatten();
            }
            charge_items(io, bytes.len() as u64)?;
            if segment.version != 1
                || segment.messages.is_empty()
                || expected_count.is_some_and(|count| count != segment.total_messages)
                || canonical.as_slice() != bytes.as_ref()
            {
                return Err(Error::Invalid(
                    "model prefix contract differs from request".into(),
                ));
            }
            let remaining = segment
                .total_messages
                .checked_sub(segment.messages.len())
                .ok_or_else(|| Error::Invalid("model prefix message count is invalid".into()))?;
            if segment.parent.is_none() != (remaining == 0) {
                return Err(Error::Invalid(
                    "model prefix parent count is invalid".into(),
                ));
            }
            let total = segment
                .total_messages
                .checked_add(local.messages.len())
                .ok_or_else(|| Error::Invalid("model prefix message count overflow".into()))?;
            if total > limits.context_messages {
                return Err(Error::Invalid(
                    "inherited model context exceeds message limit".into(),
                ));
            }
            expected_count = Some(remaining);
            next = segment.parent;
            crate::executor::stock_vec_push(
                &mut segments,
                (segment.messages, segment.message_digest),
                io,
            )?;
        }
        let mut messages = Vec::new();
        let mut hasher = blake3::Hasher::new();
        update_hasher(&mut hasher, b"harness:model-prefix:ordered:v1", io)?;
        for (segment, digest) in segments.into_iter().rev() {
            for message in &segment {
                update_hasher(
                    &mut hasher,
                    &crate::executor::preparation_digest(message, io)?,
                    io,
                )?;
            }
            for message in segment {
                crate::executor::stock_vec_push(&mut messages, message, io)?;
            }
            if hasher.finalize().as_bytes() != &digest {
                return Err(Error::Invalid(
                    "model prefix ordered messages differ".into(),
                ));
            }
        }
        let local_messages = std::mem::replace(&mut local.messages, messages);
        local.validate(limits, io)?;
        for message in local_messages {
            crate::executor::stock_vec_push(&mut local.messages, message, io)?;
        }
        for message in &local.messages {
            for file in message.content.file_refs() {
                charge_items(io, 1)?;
                if !verifier.supports_read(file.volume(), io)? {
                    return Err(crate::executor::stock_boundary(
                        io,
                        Unsupported,
                        "context verifier has no bounded capability",
                    ));
                }
                verifier
                    .verify(file, file.descriptor().byte_length(), io)?
                    .await?;
            }
        }
        Self::encode_admitted(local, limits, io)
    }
}
fn update_hasher(
    hasher: &mut blake3::Hasher,
    bytes: &[u8],
    io: Option<&PreparationIo>,
) -> Result<()> {
    #[cfg(feature = "filesystem")]
    let charge = acyclic_fs::WorkCounters {
        bytes_hashed: bytes.len() as u64,
        ..Default::default()
    };
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        io.preflight(charge)?;
    }
    hasher.update(bytes);
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        io.charge(charge)?;
    }
    let _ = io;
    Ok(())
}
