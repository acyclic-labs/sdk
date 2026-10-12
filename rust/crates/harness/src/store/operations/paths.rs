use super::*;

pub(in crate::store) fn release_owned_after_drop<T>(
    result: Result<T>,
    bytes: u64,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<T> {
    let released = match work.as_deref_mut() {
        Some(observer) => observer.release_owned(bytes),
        None => Ok(()),
    };
    match result {
        Ok(value) => released.map(|()| value),
        Err(error) => Err(error),
    }
}

pub(in crate::store) fn publication_path(
    authority: &Authority,
    prefix: &str,
    last: Option<(&dyn std::fmt::Display, usize)>,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<(StreamPath, u64)> {
    if prefix.starts_with("harness/v2/conversation-")
        && authority.kind != crate::core::AggregateKind::Conversation
    {
        return Err(crate::store::guarded_boundary(
            work,
            crate::wire::ErrorCode::Invalid,
            "message index requires a conversation",
        ));
    }
    use std::fmt::Write as _;
    let canonical = authority.stream_path(work)?;
    let canonical_capacity = canonical.capacity() as u64;
    if prefix == "harness/v2/" && last.is_none() {
        let result = stream_path(&canonical, work);
        drop(canonical);
        return release_owned_after_drop(result, canonical_capacity, work);
    }
    let result = (|| {
        let suffix = canonical.strip_prefix("harness/v2/").ok_or_else(|| {
            crate::store::guarded_boundary(
                work,
                crate::wire::ErrorCode::Invalid,
                "publication authority path is invalid",
            )
        })?;
        let capacity = prefix
            .len()
            .checked_add(suffix.len())
            .and_then(|bytes| bytes.checked_add(last.map_or(0, |(_, bytes)| bytes + 1)))
            .ok_or_else(|| {
                crate::store::guarded_boundary(
                    work,
                    crate::wire::ErrorCode::Invalid,
                    "publication path length overflows",
                )
            })?;
        let mut text = String::new();
        let result = (|| {
            match work.as_deref_mut() {
                Some(observer) => observer.reserve_text(&mut text, capacity)?,
                None => text.reserve_exact(capacity),
            }
            if let Some(observer) = work.as_deref_mut() {
                observer.copy_bytes((prefix.len() + suffix.len()) as u64)?;
            }
            text.push_str(prefix);
            text.push_str(suffix);
            if let Some((last, _)) = last {
                struct Count(usize);
                impl std::fmt::Write for Count {
                    fn write_str(&mut self, value: &str) -> std::fmt::Result {
                        self.0 += value.len();
                        Ok(())
                    }
                }
                let mut count = Count(0);
                write!(&mut count, "/{last}").map_err(|_| {
                    crate::store::guarded_boundary(
                        work,
                        crate::wire::ErrorCode::Invalid,
                        "publication path formatting failed",
                    )
                })?;
                if let Some(observer) = work.as_deref_mut() {
                    observer.copy_bytes(count.0 as u64)?;
                }
                write!(&mut text, "/{last}").map_err(|_| {
                    crate::store::guarded_boundary(
                        work,
                        crate::wire::ErrorCode::Invalid,
                        "publication path formatting failed",
                    )
                })?;
            }
            stream_path(&text, work)
        })();
        let text_capacity = text.capacity() as u64;
        drop(text);
        release_owned_after_drop(result, text_capacity, work)
    })();
    drop(canonical);
    release_owned_after_drop(result, canonical_capacity, work)
}

fn stream_path(
    text: &str,
    work: &mut Option<&mut dyn crate::store::HistoryIo>,
) -> Result<(StreamPath, u64)> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(original) = work.as_deref().and_then(|io| io.original_native_work()) {
        let path = StreamPath::new_native(text, &original.runtime_meter()).map_err(|error| {
            Error::NativeWork {
                cause: original.runtime_error(error),
                custody: None,
            }
        })?;
        return path
            .map(|path| (path, 0))
            .map_err(|error| stream_error_with_work(error, work));
    }
    let retained = if let Some(observer) = work.as_deref_mut() {
        let bytes = text.len() as u64;
        observer.admit_allocation(bytes)?;
        observer.copy_bytes(bytes)?;
        observer.retain_owned(bytes)?;
        bytes
    } else {
        0
    };
    StreamPath::new(text)
        .map(|path| (path, retained))
        .map_err(|error| stream_error_with_work(error, work))
        .or_else(|error| release_owned_after_drop(Err(error), retained, work))
}

/// Freezes a producer-retained vector without replacing its original claim.
pub(crate) fn owned_bytes_from_retained_vec(
    bytes: Vec<u8>,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<(OwnedBytes, u64)> {
    let capacity = bytes.capacity() as u64;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(original) = work
        .as_deref()
        .and_then(|observer| observer.original_native_work())
        .cloned()
    {
        let custody = match work
            .as_deref_mut()
            .expect("original native observer")
            .take_native_custody(capacity)
        {
            Ok(custody) => custody,
            Err(error) => {
                drop(bytes);
                return release_owned_after_drop(Err(error), capacity, work);
            }
        };
        return OwnedBytes::from_vec_with_guard(bytes, custody, original.runtime_meter())
            .map(|bytes| (bytes, 0))
            .map_err(|failure| match failure.into_error() {
                acyclic_stream::PreparedOwnedBytesError::Admission(source) => Error::NativeWork {
                    cause: original.runtime_error(source),
                    custody: None,
                },
                _ => Error::NativeWork {
                    cause: acyclic_fs::NativeTreeMaterializationError::InvalidLayout,
                    custody: None,
                },
            });
    }
    Ok((
        OwnedBytes::from(bytes),
        if work.is_some() { capacity } else { 0 },
    ))
}
