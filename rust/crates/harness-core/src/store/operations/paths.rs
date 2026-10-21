use super::*;

pub(in crate::store) fn release_owned_after_drop<T>(
    result: Result<T>,
    bytes: u64,
    work: &mut Option<&mut dyn HistoryIo>,
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
    last: Option<&dyn std::fmt::Display>,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<(StreamPath, u64)> {
    use std::fmt::Write as _;
    if prefix.starts_with("harness/v2/conversation-")
        && authority.kind != crate::core::AggregateKind::Conversation
    {
        return Err(boundary(
            work,
            Invalid,
            "message index requires a conversation",
        ));
    }
    let mut quota = CodecQuota::new(work, 0)?;
    let canonical = authority.stream_path(quota.work())?;
    quota.adopt(canonical.capacity() as u64)?;
    let mut text = String::new();
    let input = if prefix == "harness/v2/" && last.is_none() {
        canonical.as_str()
    } else {
        let suffix = canonical.strip_prefix("harness/v2/").ok_or_else(|| {
            boundary(
                quota.work(),
                Invalid,
                "publication authority path is invalid",
            )
        })?;
        struct Count(usize);
        impl std::fmt::Write for Count {
            fn write_str(&mut self, value: &str) -> std::fmt::Result {
                self.0 = self.0.checked_add(value.len()).ok_or(std::fmt::Error)?;
                Ok(())
            }
        }
        let mut count = Count(0);
        if let Some(last) = last {
            write!(&mut count, "/{last}").map_err(|_| {
                boundary(quota.work(), Invalid, "publication path formatting failed")
            })?;
        }
        let capacity = prefix
            .len()
            .checked_add(suffix.len())
            .and_then(|bytes| bytes.checked_add(count.0))
            .ok_or_else(|| boundary(quota.work(), Invalid, "publication path length overflows"))?;
        match quota.work().as_deref_mut() {
            Some(io) => io.reserve_text(&mut text, capacity)?,
            None => text.reserve_exact(capacity),
        }
        quota.adopt(text.capacity() as u64)?;
        if let Some(io) = quota.work().as_deref_mut() {
            io.copy_bytes(capacity as u64)?;
        }
        text.push_str(prefix);
        text.push_str(suffix);
        if let Some(last) = last {
            write!(&mut text, "/{last}").map_err(|_| {
                boundary(quota.work(), Invalid, "publication path formatting failed")
            })?;
        }
        text.as_str()
    };
    let (path, held) = (|| {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(original) = quota
            .work()
            .as_deref()
            .and_then(|io| io.original_native_work())
        {
            let path = StreamPath::new_native(input, &original.runtime_meter())
                .map_err(|error| Error::from(original.runtime_error(error)))?;
            return path
                .map(|path| (path, 0))
                .map_err(|error| stream_error_with_work(error, quota.work()));
        }
        let retained = if let Some(observer) = quota.work().as_deref_mut() {
            let bytes = input.len() as u64;
            observer.admit_allocation(bytes)?;
            observer.copy_bytes(bytes)?;
            observer.retain_owned(bytes)?;
            bytes
        } else {
            0
        };
        StreamPath::new(input)
            .map(|path| (path, retained))
            .map_err(|error| stream_error_with_work(error, quota.work()))
            .or_else(|error| release_owned_after_drop(Err(error), retained, quota.work()))
    })()?;
    quota.adopt(held)?;
    drop(text);
    drop(canonical);
    quota.replace(held)?;
    quota.transfer();
    Ok((path, held))
}

/// Freezes a producer-retained vector without replacing its original claim.
#[doc(hidden)]
pub fn owned_bytes_from_retained_vec(
    bytes: Vec<u8>,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<(OwnedBytes, u64)> {
    let capacity = bytes.capacity() as u64;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(observer) = work.as_deref_mut()
        && let Some(original) = observer.original_native_work().cloned()
    {
        let custody = match observer.take_native_custody(capacity) {
            Ok(custody) => custody,
            Err(error) => {
                drop(bytes);
                return release_owned_after_drop(Err(error), capacity, work);
            }
        };
        let mut bytes = bytes;
        let mut custody = Some(custody);
        return OwnedBytes::from_vec_with_guard_in_place(
            &mut bytes,
            &mut custody,
            &original.runtime_meter(),
        )
        .map(|bytes| (bytes, 0))
        .map_err(|source| Error::from(original.runtime_error(source)));
    }
    Ok((
        OwnedBytes::from(bytes),
        if work.is_some() { capacity } else { 0 },
    ))
}
