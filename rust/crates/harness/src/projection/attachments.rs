use super::*;
use crate::conversation::{
    Attachment, ContentHold, ConversationMessage, FileRef, ReferencedAttachments,
};
use crate::model::{ImageDetail, NativeMediaIntent, NativeMediaPolicy};
use std::borrow::Cow;

fn allocate(bytes: u64, io: Option<&PreparationIo>) -> Result<()> {
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        io.retain_owned(bytes)?;
        io.allocation_attempt(bytes)?;
    }
    let _ = (bytes, io);
    Ok(())
}

pub(super) fn native_image(
    maximum_work: u64,
    io: Option<&PreparationIo>,
) -> Result<FileProjectionPolicy> {
    allocate(std::mem::size_of::<NativeMediaPolicy>() as u64, io)?;
    Ok(FileProjectionPolicy::Native(Box::new(NativeMediaPolicy {
        intent: NativeMediaIntent::Image {
            detail: ImageDetail::Auto,
        },
        maximum_bytes: crate::conversation::MAX_EXACT_JS_INTEGER,
        maximum_work,
        configuration: None,
    })))
}

pub(super) fn omission_text(count: usize, io: Option<&PreparationIo>) -> Result<String> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(io) = io.filter(|io| io.original_native_work().is_some()) {
        let (text, custody) = io
            .original_native_work()
            .expect("checked native work")
            .display(&format_args!(
                "[{count} additional attachments omitted from this bounded model context]"
            ))?;
        io.absorb_native_custody(custody)?;
        return Ok(text);
    }
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        use std::fmt::Write;
        let digits = if count == 0 {
            1
        } else {
            count.ilog10() as usize + 1
        };
        let length =
            digits + "[ additional attachments omitted from this bounded model context]".len();
        allocate(length as u64, Some(io))?;
        let charge = acyclic_fs::WorkCounters {
            bytes_encoded: length as u64,
            ..Default::default()
        };
        io.preflight(charge)?;
        let mut text = String::with_capacity(length);
        write!(
            &mut text,
            "[{count} additional attachments omitted from this bounded model context]"
        )
        .expect("String formatter");
        io.charge(charge)?;
        return Ok(text);
    }
    let _ = io;
    Ok(format!(
        "[{count} additional attachments omitted from this bounded model context]"
    ))
}

pub(super) async fn resolve<'a>(
    message: &'a ConversationMessage,
    resolver: &'a dyn ContentResidencyVerifier,
    maximum: usize,
    io: Option<&'a PreparationIo>,
) -> Result<(Cow<'a, [Attachment]>, Option<ContentHold<'a>>)> {
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        io.check_cancellation()?;
    }
    match &message.attachments {
        ReferencedAttachments::Inline { items } => {
            if items.len() > maximum {
                return Err(stock_boundary(
                    io,
                    Invalid,
                    "selected message exceeds attachment projection limit",
                ));
            }
            Ok((Cow::Borrowed(items), None))
        }
        ReferencedAttachments::Manifest {
            manifest,
            item_count,
        } => {
            if *item_count as usize > maximum {
                return Err(stock_boundary(
                    io,
                    Invalid,
                    "selected message exceeds attachment projection limit",
                ));
            }
            let (items, hold) =
                crate::conversation::load_manifest(resolver, manifest, *item_count, io).await?;
            Ok((Cow::Owned(items), hold))
        }
    }
}

pub(super) fn append(
    attachments: Cow<'_, [Attachment]>,
    source_hold: Option<ContentHold<'_>>,
    count: usize,
    parts: &mut Vec<ModelContentPart>,
    maximum_work: u64,
    io: Option<&PreparationIo>,
) -> Result<()> {
    let mut append = |file: FileRef| -> Result<()> {
        let policy = match file.descriptor().media_type() {
            "image/png" | "image/jpeg" | "image/gif" | "image/webp" => {
                native_image(maximum_work, io)?
            }
            _ => FileProjectionPolicy::Reference,
        };
        parts.push(ModelContentPart::File { file, policy });
        Ok(())
    };
    // Holds must outlive manifest payloads on success and failure.
    let _hold;
    let items = match attachments {
        Cow::Borrowed(items) => {
            for item in items.iter().take(count) {
                crate::model::charge_items(io, 1)?;
                with_history_io(io, |work| item.validate(work))?;
                append(crate::model::clone_file(&item.file, io)?)?;
            }
            return Ok(());
        }
        Cow::Owned(items) => items,
    };
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    let move_files = if let Some(io) = io.filter(|io| io.original_native_work().is_some()) {
        let hold = source_hold.as_ref().expect("native manifest ownership");
        let mut retained = 0_u64;
        for item in items.iter().take(count) {
            crate::model::charge_items(Some(io), 1)?;
            retained = retained
                .checked_add(item.file.logical_owned_bytes()?)
                .ok_or_else(|| {
                    stock_boundary(Some(io), Invalid, "projected file ownership overflows")
                })?;
        }
        let discarded = hold.bytes().checked_sub(retained).ok_or_else(|| {
            stock_boundary(
                Some(io),
                Invalid,
                "projected files exceed manifest ownership",
            )
        })?;
        _hold = Some(io.guard_owned(discarded)?);
        source_hold
            .expect("checked manifest ownership")
            .into_retained();
        true
    } else {
        _hold = source_hold;
        io.is_none()
    };
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let move_files = {
        _hold = source_hold;
        io.is_none()
    };
    for item in items.into_iter().take(count) {
        crate::model::charge_items(io, 1)?;
        with_history_io(io, |work| item.validate(work))?;
        append(if move_files {
            item.file
        } else {
            crate::model::clone_file(&item.file, io)?
        })?;
    }
    Ok(())
}
