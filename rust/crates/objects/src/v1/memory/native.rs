use super::*;

#[cfg(not(target_arch = "wasm32"))]
pub(in crate::v1) fn clone_native_text(
    value: &str,
    meter: &Arc<dyn NativeReadMeter>,
    retained: &mut ReadAllocation,
) -> Result<String, NativeObjectsError> {
    let mut allocation = ReadAllocation::new(meter, value.len())?;
    let mut text = String::new();
    text.try_reserve_exact(value.len())
        .map_err(|error| meter.allocation_failed(error))?;
    allocation.reconcile_capacity(text.capacity())?;
    meter.admit_codec(0, value.len() as u64, 0)?;
    text.push_str(value);
    meter.record_codec(0, value.len() as u64, 0)?;
    retained.absorb(&mut allocation)?;
    Ok(text)
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) fn clone_native_info(
    info: &wire::ObjectInfo,
    meter: &Arc<dyn NativeReadMeter>,
    retained: &mut ReadAllocation,
) -> Result<wire::ObjectInfo, NativeObjectsError> {
    Ok(wire::ObjectInfo {
        etag: clone_native_text(&info.etag, meter, retained)?,
        size: info.size,
        metadata: info
            .metadata
            .as_ref()
            .map(|metadata| clone_native_metadata(metadata, meter, retained))
            .transpose()?,
        last_modified: info.last_modified,
    })
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) fn clone_native_metadata(
    metadata: &wire::ObjectMetadata,
    meter: &Arc<dyn NativeReadMeter>,
    retained: &mut ReadAllocation,
) -> Result<wire::ObjectMetadata, NativeObjectsError> {
    let mut copied = wire::ObjectMetadata {
        content_type: clone_native_text(&metadata.content_type, meter, retained)?,
        content_encoding: clone_native_text(&metadata.content_encoding, meter, retained)?,
        cache_control: clone_native_text(&metadata.cache_control, meter, retained)?,
        content_disposition: clone_native_text(&metadata.content_disposition, meter, retained)?,
        content_language: clone_native_text(&metadata.content_language, meter, retained)?,
        expires_unix_seconds: metadata.expires_unix_seconds,
        user: BTreeMap::new(),
    };
    for (key, value) in &metadata.user {
        let key = clone_native_text(key, meter, retained)?;
        let value = clone_native_text(value, meter, retained)?;
        // Public retained entry payload; private BTree node layout is excluded.
        let bytes = std::mem::size_of::<(String, String)>();
        let mut entry = ReadAllocation::adopt(meter, bytes)?;
        meter.admit_codec(0, bytes as u64, 0)?;
        retained.absorb(&mut entry)?;
        copied.user.insert(key, value);
        meter.record_codec(0, bytes as u64, 0)?;
    }
    Ok(copied)
}

#[cfg(not(target_arch = "wasm32"))]
impl SharedInfo<String> {
    pub(super) fn text(
        value: &str,
        meter: &Arc<dyn NativeReadMeter>,
    ) -> Result<Self, NativeObjectsError> {
        let mut allocation = ReadAllocation::empty(meter);
        let text = clone_native_text(value, meter, &mut allocation)?;
        Self::new_measured(text, allocation, meter)
    }
}
#[cfg(not(target_arch = "wasm32"))]
impl<T> SharedInfo<T> {
    pub(super) fn new_measured(
        info: T,
        mut allocation: ReadAllocation,
        meter: &Arc<dyn NativeReadMeter>,
    ) -> Result<Self, NativeObjectsError> {
        let control = std::mem::size_of::<super::Guarded<T>>();
        meter.admit_codec(0, control as u64, 0)?;
        allocation.absorb(&mut ReadAllocation::new(meter, control)?)?;
        let result = Self(Arc::new(super::Guarded {
            value: info,
            _allocation: Some(allocation),
        }));
        meter.record_codec(0, control as u64, 0)?;
        Ok(result)
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) fn next_id_measured(
    state: &mut State,
    meter: &Arc<dyn NativeReadMeter>,
    retained: &mut ReadAllocation,
) -> Result<String, NativeObjectsError> {
    state.sequence = state
        .sequence
        .checked_add(1)
        .ok_or(Error::from(QuotaExceeded))?;
    meter.admit_codec(64, 0, 8)?;
    let hash = blake3::hash(&state.sequence.to_le_bytes()).to_hex();
    meter.record_codec(64, 0, 8)?;
    let mut bytes = [b'"'; 66];
    bytes[1..65].copy_from_slice(hash.as_bytes());
    clone_native_text(
        std::str::from_utf8(&bytes).map_err(|_| Error::from(Unavailable))?,
        meter,
        retained,
    )
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) fn encode_measured(
    message: &impl Message,
    meter: &Arc<dyn NativeReadMeter>,
) -> Result<OwnedBytes, NativeObjectsError> {
    let length = message.encoded_len();
    let mut allocation = ReadAllocation::new(meter, length)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(length)
        .map_err(|error| meter.allocation_failed(error))?;
    allocation.reconcile_capacity(bytes.capacity())?;
    meter.admit_codec(length as u64, 0, 0)?;
    message
        .encode(&mut bytes)
        .map_err(|_| Error::from(Unavailable))?;
    meter.record_codec(bytes.len() as u64, 0, 0)?;
    OwnedBytes::from_vec_with_guard(bytes, allocation, Arc::clone(meter))
        .map_err(|failure| super::native_preparation_error(failure.into_error()))
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) fn put_digest_measured(
    query: &mut wire::PutObjectHeader,
    body: &[u8],
    meter: &Arc<dyn NativeReadMeter>,
) -> Result<[u8; 32], NativeObjectsError> {
    let mutation = query.mutation.take();
    let encoded = encode_measured(query, meter);
    query.mutation = mutation;
    let encoded = encoded?;
    let domain = b"acyclic.objects.v1.mutation\0";
    let hash_bytes = body
        .len()
        .checked_add(domain.len() + 8 + 3 + 8 + 8 + 32)
        .and_then(|bytes| bytes.checked_add(encoded.len()))
        .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::OutOfMemory))?;
    meter.admit_codec(0, 0, hash_bytes as u64)?;
    let body_hash = blake3::hash(body);
    let digest = request::finish_digest("put", &encoded, body.len() as u64, *body_hash.as_bytes());
    meter.record_codec(0, 0, hash_bytes as u64)?;
    drop(encoded);
    Ok(digest)
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) fn native_vec<T>(
    count: usize,
    meter: &Arc<dyn NativeReadMeter>,
) -> Result<(Vec<T>, ReadAllocation), NativeObjectsError> {
    let bytes = count
        .checked_mul(std::mem::size_of::<T>())
        .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::OutOfMemory))?;
    let mut allocation = ReadAllocation::new(meter, bytes)?;
    let mut values = Vec::new();
    values
        .try_reserve_exact(count)
        .map_err(|error| meter.allocation_failed(error))?;
    allocation.reconcile_capacity(values.capacity() * std::mem::size_of::<T>())?;
    Ok((values, allocation))
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) fn get_native_locked(
    state: &State,
    query: &wire::GetObjectRequest,
    maximum_bytes: u64,
    meter: &Arc<dyn NativeReadMeter>,
    table_backend: bool,
) -> Result<ResolvedGet, NativeObjectsError> {
    meter.check()?;
    let name = request::get(query)?;
    if table_backend {
        meter.admit_read(0)?;
    }
    let (value, range, start, end) = get_selected_locked(state, name, query, maximum_bytes)?;
    let mut retained = ReadAllocation::empty(meter);
    let info = clone_native_info(&value.info, meter, &mut retained)?;
    Ok(ResolvedGet {
        header: wire::GetObjectHeader {
            object: Some(info),
            content_range: range,
        },
        body: value.body.clone(),
        start,
        end,
        allocation: Some(retained),
    })
}
