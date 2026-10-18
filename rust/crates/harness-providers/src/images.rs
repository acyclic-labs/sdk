//! Bounded views of owner-authorized files, used only during dispatch.

use crate::{Adapter, ImageInput, ResolvedModel, invalid};
use acyclic_harness::{
    Result,
    conversation::{ContentHold, ContentIo, FileRef, verified_content_bytes},
    model::{
        FileProjectionPolicy, ModelContentPart, ModelDataPart, ModelMessage, ModelRequest,
        ModelRole, NativeMediaIntent, NativeMediaPolicy, ToolResultContent,
    },
};
use acyclic_stream::OwnedBytes;

fn parts(message: &ModelMessage) -> impl Iterator<Item = (&FileRef, &FileProjectionPolicy, bool)> {
    message.content.parts().iter().flat_map(|part| {
        let direct = match part {
            ModelContentPart::File { file, policy } => {
                Some((file, policy, message.role == ModelRole::User))
            }
            _ => None,
        };
        let nested = match part {
            ModelContentPart::ToolResult {
                content: ToolResultContent::Parts { parts },
                ..
            } => Some(parts.as_slice()),
            _ => None,
        };
        direct
            .into_iter()
            .chain(nested.into_iter().flatten().filter_map(|part| match part {
                ModelDataPart::File { file, policy } => Some((file, policy, true)),
                _ => None,
            }))
    })
}

pub(crate) fn has_images(request: &ModelRequest) -> bool {
    request
        .messages
        .iter()
        .any(|message| files(message).next().is_some())
}

pub(crate) fn files(
    message: &ModelMessage,
) -> impl Iterator<Item = (&FileRef, &NativeMediaPolicy)> {
    parts(message).filter_map(|(file, policy, allowed)| match policy {
        FileProjectionPolicy::Native(policy)
            if allowed && matches!(policy.intent, NativeMediaIntent::Image { .. }) =>
        {
            Some((file, policy.as_ref()))
        }
        _ => None,
    })
}

pub(crate) fn support<'a>(
    provider: &'a ResolvedModel,
    file: &FileRef,
    policy: &NativeMediaPolicy,
) -> Result<Option<&'a ImageInput>> {
    if policy.configuration.is_some() {
        return Err(invalid(
            "image adapter has no implementation for native extension options",
        ));
    }
    let Some(limits) = provider.config.images.get(&provider.model.name) else {
        return Ok(None);
    };
    let supported = match file.descriptor().media_type() {
        "image/png" | "image/jpeg" | "image/webp" => true,
        "image/gif" => provider.config.adapter != Adapter::Gemini,
        "image/heic" | "image/heif" => provider.config.adapter == Adapter::Gemini,
        _ => false,
    };
    if !supported {
        return Ok(None);
    }
    if file.descriptor().byte_length() == 0
        || file.descriptor().byte_length() > limits.maximum_bytes.min(policy.maximum_bytes)
        || u64::from(limits.maximum_tokens) > policy.maximum_work
    {
        return Err(invalid("image exceeds its byte or processing allowance"));
    }
    Ok(Some(limits))
}

/// Checks count/aggregate limits using descriptors before reading any image body.
pub(crate) fn encoded_bytes(provider: &ResolvedModel, request: &ModelRequest) -> Result<u64> {
    let mut total = 0u64;
    let mut count = 0u32;
    for (file, policy) in request.messages.iter().flat_map(files) {
        let Some(limits) = support(provider, file, policy)? else {
            continue;
        };
        total = total
            .checked_add(file.descriptor().byte_length())
            .ok_or_else(|| invalid("image byte count overflows"))?;
        count = count
            .checked_add(1)
            .ok_or_else(|| invalid("image count overflows"))?;
        if total > limits.maximum_total_bytes || count > limits.maximum_images {
            return Err(invalid("request exceeds aggregate image limits"));
        }
    }
    total
        .checked_add(2)
        .and_then(|n| n.checked_div(3))
        .and_then(|n| n.checked_mul(4))
        .and_then(|n| n.checked_add(u64::from(count) * 4))
        .ok_or_else(|| invalid("encoded image size overflows"))
}

pub(crate) fn tokens(provider: &ResolvedModel, message: &ModelMessage) -> Result<u32> {
    files(message).try_fold(0u32, |total, (file, policy)| {
        let amount = support(provider, file, policy)?.map_or(0, |limits| limits.maximum_tokens);
        total
            .checked_add(amount)
            .ok_or_else(|| invalid("image token count overflows"))
    })
}

pub(crate) fn reference_bytes(provider: &ResolvedModel, message: &ModelMessage) -> Result<usize> {
    files(message).try_fold(0usize, |total, (file, policy)| {
        if support(provider, file, policy)?.is_none() {
            return Ok(total);
        }
        total
            .checked_add(crate::projection::encoded_len(file)?)
            .ok_or_else(|| invalid("image reference size overflows"))
    })
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn admit(
    provider: &ResolvedModel,
    request: &ModelRequest,
    work: &acyclic_fs::NativeMaterializationWork,
    allocation: &mut acyclic_fs::NativeAllocation,
) -> Result<()> {
    let encoded = crate::native::diagnostic(encoded_bytes(provider, request), allocation)?;
    if encoded != 0 {
        let mut hold = crate::native::reserve(work, encoded, 0, 0)?;
        allocation.absorb(&mut hold)?;
        work.charge(acyclic_fs::WorkCounters {
            items_examined: encoded,
            ..acyclic_fs::WorkCounters::UNCHARGED
        })?;
    }
    Ok(())
}

pub(crate) struct Loaded<'a> {
    values: Vec<(OwnedBytes, Option<ContentHold<'a>>)>,
    occurrences: Vec<usize>,
}
impl Loaded<'_> {
    pub(crate) fn iter(&self) -> impl Iterator<Item = &[u8]> {
        self.occurrences
            .iter()
            .map(|index| self.values[*index].0.as_ref())
    }
}

pub(crate) async fn load<'a>(
    provider: &'a ResolvedModel,
    request: &'a ModelRequest,
    io: Option<&'a ContentIo>,
) -> Result<Loaded<'a>> {
    load_unique::<true>(provider, request, io).await
}

pub(crate) async fn load_unique<'a, const UNIQUE: bool>(
    provider: &'a ResolvedModel,
    request: &'a ModelRequest,
    io: Option<&'a ContentIo>,
) -> Result<Loaded<'a>> {
    encoded_bytes(provider, request)?;
    let mut result = Loaded {
        values: Vec::new(),
        occurrences: Vec::new(),
    };
    let mut pinned = std::collections::BTreeMap::new();
    for (file, policy) in request.messages.iter().flat_map(files) {
        let Some(limits) = support(provider, file, policy)? else {
            continue;
        };
        let identity = (file.volume(), file.path(), file.version());
        if UNIQUE && let Some((previous, index)) = pinned.get(&identity) {
            if *previous != file {
                return Err(invalid("image identity has conflicting metadata"));
            }
            result.occurrences.push(*index);
            continue;
        }
        let reader = provider
            .files
            .reader
            .as_ref()
            .ok_or_else(|| invalid("file input requires an owner-authorized content reader"))?;
        let payload = verified_content_bytes(
            reader.as_ref(),
            file,
            limits.maximum_bytes.min(policy.maximum_bytes),
            io,
        )
        .await?;
        limits.parse(&payload.0, Some(file.descriptor().media_type()))?;
        let index = result.values.len();
        pinned.insert(identity, (file, index));
        result.values.push(payload);
        result.occurrences.push(index);
    }
    Ok(result)
}
