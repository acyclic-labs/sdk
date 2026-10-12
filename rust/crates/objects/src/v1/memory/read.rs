use super::*;

pub(super) struct ResolvedGet {
    pub(super) header: wire::GetObjectHeader,
    pub(super) body: StoredBody,
    pub(super) start: usize,
    pub(super) end: usize,
    #[cfg(not(target_arch = "wasm32"))]
    pub(super) allocation: Option<ReadAllocation>,
}

impl ResolvedGet {
    pub(super) async fn read(self, mode: BodyReadMode) -> Result<Object, BodyReadError> {
        #[cfg(not(target_arch = "wasm32"))]
        let allocation = self.allocation;
        let Self {
            header,
            body,
            start,
            end,
            ..
        } = self;
        let body = body.read(start, end, mode).await?;
        Ok(Object {
            header,
            body,
            #[cfg(not(target_arch = "wasm32"))]
            _header_custody: allocation,
        })
    }
}

pub(super) fn get_locked(
    state: &State,
    query: &wire::GetObjectRequest,
    maximum_bytes: u64,
) -> Result<ResolvedGet, Error> {
    let name = request::get(query)?;
    let (value, range, start, end) = get_selected_locked(state, name, query, maximum_bytes)?;
    Ok(ResolvedGet {
        header: wire::GetObjectHeader {
            object: Some((*value.info).clone()),
            content_range: range,
        },
        body: value.body.clone(),
        start,
        end,
        #[cfg(not(target_arch = "wasm32"))]
        allocation: None,
    })
}

pub(super) fn get_selected_locked<'a>(
    state: &'a State,
    name: &str,
    query: &wire::GetObjectRequest,
    maximum_bytes: u64,
) -> Result<(&'a Stored, Option<wire::ContentRange>, usize, usize), Error> {
    let value = state
        .buckets
        .get(name)
        .and_then(|bucket| bucket.objects.get(query.object_key.as_str()))
        .ok_or(Error::from(NotFound))?;
    read_condition(&value.info, &query.if_match, &query.if_none_match)?;
    let range = request::range(&query.range, value.info.size)?;
    let (start, end) = match &range {
        None => (0, value.body.len()),
        Some(range) => (
            usize::try_from(range.start).map_err(|_| Error::from(QuotaExceeded))?,
            usize::try_from(range.end + 1).map_err(|_| Error::from(QuotaExceeded))?,
        ),
    };
    if (end - start) as u64 > maximum_bytes {
        return Err(Error::from(QuotaExceeded).into());
    }
    Ok((value, range, start, end))
}
