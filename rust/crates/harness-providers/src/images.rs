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

// JSON can escape every source byte into six output bytes. Counting needs no
// content reads; the same bound funds the temporary text graph at dispatch.
pub(crate) fn text_bytes(message: &ModelMessage) -> Result<u64> {
    parts(message).try_fold(0u64, |total, (file, policy, _)| {
        if matches!(policy, FileProjectionPolicy::BoundedFull) {
            file.descriptor()
                .byte_length()
                .checked_mul(6)
                .and_then(|bytes| total.checked_add(bytes))
                .ok_or_else(|| invalid("text projection size overflows"))
        } else {
            Ok(total)
        }
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
    let text = request.messages.iter().try_fold(0u64, |total, message| {
        total
            .checked_add(text_bytes(message)?)
            .ok_or_else(|| invalid("file projection size overflows"))
    })?;
    total
        .checked_add(2)
        .and_then(|n| n.checked_div(3))
        .and_then(|n| n.checked_mul(4))
        .and_then(|n| n.checked_add(u64::from(count) * 4))
        .and_then(|n| n.checked_add(text))
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

pub(crate) async fn load<'a>(
    provider: &'a ResolvedModel,
    request: &'a ModelRequest,
    io: Option<&'a ContentIo>,
) -> Result<Vec<(OwnedBytes, Option<ContentHold<'a>>)>> {
    encoded_bytes(provider, request)?;
    let mut result = Vec::new();
    for (file, policy, allowed) in request.messages.iter().flat_map(parts) {
        let limits = match policy {
            FileProjectionPolicy::BoundedFull => continue,
            FileProjectionPolicy::Native(policy)
                if allowed && matches!(policy.intent, NativeMediaIntent::Image { .. }) =>
            {
                let Some(limits) = support(provider, file, policy)? else {
                    continue;
                };
                Some((limits, policy.maximum_bytes))
            }
            _ => continue,
        };
        let reader = provider
            .files
            .reader
            .as_ref()
            .ok_or_else(|| invalid("file input requires an owner-authorized content reader"))?;
        let payload = verified_content_bytes(
            reader.as_ref(),
            file,
            limits.map_or(file.descriptor().byte_length(), |(limits, maximum)| {
                limits.maximum_bytes.min(maximum)
            }),
            io,
        )
        .await?;
        let Some((limits, _)) = limits else {
            result.push(payload);
            continue;
        };
        let bytes = &payload.0;
        let kind = imagesize::image_type(bytes).map_err(|_| invalid("invalid image header"))?;
        let matches = matches!(
            (kind, file.descriptor().media_type()),
            (imagesize::ImageType::Png, "image/png")
                | (imagesize::ImageType::Jpeg, "image/jpeg")
                | (imagesize::ImageType::Gif, "image/gif")
                | (imagesize::ImageType::Webp, "image/webp")
                | (imagesize::ImageType::Heif(_), "image/heic" | "image/heif")
        );
        if !matches {
            return Err(invalid("image header differs from its declared media type"));
        }
        let size = imagesize::blob_size(bytes).map_err(|_| invalid("invalid image dimensions"))?;
        if size.width == 0
            || size.height == 0
            || size.width > limits.maximum_dimension as usize
            || size.height > limits.maximum_dimension as usize
        {
            return Err(invalid("image exceeds its dimension allowance"));
        }
        result.push(payload);
    }
    Ok(result)
}
