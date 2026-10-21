use super::{ExaResponse, TinyFishResponse};
use crate::tool::edit::{allocation, charge, reserve_string, reserve_vec};
use crate::{NativeCustody, NativeWork, Result, SearchResponse, SearchResult, error};

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
use crate::filesystem::UploadRecord as Record;
#[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
use serde::de::DeserializeOwned as Record;

fn decode<T: Record>(bytes: &[u8], work: NativeWork<'_>) -> Result<(T, NativeCustody)> {
    if bytes.len() > super::MAXIMUM_RESPONSE_BYTES {
        return Err(crate::rejection(work, "search response exceeds byte limit"));
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = work {
        let io = crate::executor::PreparationIo::from_native_work(work)?;
        let decoded: T = io.decode_json(bytes).map_err(|failure| match failure {
            crate::Error::NativeJsonAdmission { .. } | crate::Error::NativeJson { .. } => {
                crate::rejection(Some(work), "invalid search response")
            }
            failure => failure,
        })?;
        let retained = (std::mem::size_of::<T>() as u64)
            .checked_add(decoded.logical_owned_bytes()?)
            .ok_or_else(|| error(Some(work), "search decode ownership overflow"))?;
        return Ok((decoded, Some(io.into_native_custody(retained)?)));
    }
    crate::executor::json::json_from_slice_bounded(
        std::marker::PhantomData::<T>,
        bytes,
        crate::executor::json::JsonBounds {
            maximum_bytes: super::MAXIMUM_RESPONSE_BYTES as u64,
            maximum_items: u64::MAX,
            maximum_depth: 256,
        },
        0,
        None,
        Some(&crate::executor::json::JsonValueSchema::None),
        &mut |_| Ok(()),
    )
    .map(|value| (value, None))
    .map_err(|_| crate::search::rejection(work, "invalid search response"))
}

fn finish(
    mut paid: (SearchResponse, NativeCustody, NativeCustody),
) -> Result<(SearchResponse, NativeCustody)> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let (Some(output), Some(input)) = (&mut paid.1, &mut paid.2) {
        // Raw DTO containers and unused sources have dropped. Moved strings
        // retain their existing ledger; only the final output capacity remains.
        output.absorb(input)?;
        output.resize(crate::native::response_bytes(&paid.0)?)?;
    }
    let _ = &mut paid;
    Ok((paid.0, paid.1))
}

pub(super) fn exa(
    bytes: &[u8],
    maximum: u32,
    work: NativeWork<'_>,
) -> Result<(SearchResponse, NativeCustody)> {
    normalize_exa(decode::<ExaResponse>(bytes, work)?, maximum, work)
}

fn normalize_exa(
    raw: (ExaResponse, NativeCustody),
    maximum: u32,
    work: NativeWork<'_>,
) -> Result<(SearchResponse, NativeCustody)> {
    let mut held = allocation(work, std::mem::size_of::<SearchResponse>())?;
    let mut results = Vec::new();
    reserve_vec(
        &mut held,
        &mut results,
        raw.0.results.len().min(maximum as usize),
    )?;
    for item in raw.0.results.into_iter().take(maximum as usize) {
        let snippet = if item.highlights.len() <= 1 {
            charge(&held, 0, item.highlights.len() as u64 + 1)?;
            item.highlights.into_iter().next().unwrap_or_default()
        } else {
            let length = item
                .highlights
                .iter()
                .try_fold(0_usize, |n, text| {
                    n.checked_add(text.len())
                        .ok_or_else(|| error(work, "search snippet size overflow"))
                })?
                .checked_add(item.highlights.len().saturating_sub(1))
                .ok_or_else(|| error(work, "search snippet size overflow"))?;
            let mut snippet = String::new();
            reserve_string(&mut held, &mut snippet, length)?;
            charge(&held, length as u64, item.highlights.len() as u64 + 1)?;
            for (index, text) in item.highlights.iter().enumerate() {
                if index > 0 {
                    snippet.push('\n');
                }
                snippet.push_str(text);
            }
            snippet
        };
        results.push(SearchResult {
            url: item.url,
            title: item.title.unwrap_or_default(),
            snippet,
        });
    }
    finish((SearchResponse { results }, held, raw.1))
}

#[cfg(all(test, feature = "filesystem", not(target_arch = "wasm32")))]
mod tests;

pub(super) fn tinyfish(
    bytes: &[u8],
    maximum: u32,
    work: NativeWork<'_>,
) -> Result<(SearchResponse, NativeCustody)> {
    let raw = decode::<TinyFishResponse>(bytes, work)?;
    let mut held = allocation(work, std::mem::size_of::<SearchResponse>())?;
    let mut results = Vec::new();
    reserve_vec(
        &mut held,
        &mut results,
        raw.0.results.len().min(maximum as usize),
    )?;
    for item in raw.0.results.into_iter().take(maximum as usize) {
        charge(&held, 0, 1)?;
        results.push(SearchResult {
            url: item.url,
            title: item.title,
            snippet: item.snippet,
        });
    }
    finish((SearchResponse { results }, held, raw.1))
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl Record for ExaResponse {
    fn diagnostic_label_bytes() -> u64 {
        crate::native::labels()
    }
    fn logical_owned_bytes(&self) -> Result<u64> {
        let initial = (self.results.capacity() as u64)
            .checked_mul(std::mem::size_of::<super::ExaResult>() as u64)
            .ok_or_else(overflow)?;
        self.results.iter().try_fold(initial, |n, item| {
            let n = n
                .checked_add(item.url.capacity() as u64)
                .and_then(|n| n.checked_add(item.title.as_ref().map_or(0, |t| t.capacity() as u64)))
                .and_then(|n| {
                    (item.highlights.capacity() as u64)
                        .checked_mul(std::mem::size_of::<String>() as u64)
                        .and_then(|s| n.checked_add(s))
                })
                .ok_or_else(overflow)?;
            item.highlights.iter().try_fold(n, |n, text| {
                n.checked_add(text.capacity() as u64).ok_or_else(overflow)
            })
        })
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl Record for TinyFishResponse {
    fn diagnostic_label_bytes() -> u64 {
        crate::native::labels()
    }
    fn logical_owned_bytes(&self) -> Result<u64> {
        let initial = (self.results.capacity() as u64)
            .checked_mul(std::mem::size_of::<super::TinyFishResult>() as u64)
            .ok_or_else(overflow)?;
        self.results.iter().try_fold(initial, |n, item| {
            n.checked_add(item.url.capacity() as u64)
                .and_then(|n| n.checked_add(item.title.capacity() as u64))
                .and_then(|n| n.checked_add(item.snippet.capacity() as u64))
                .ok_or_else(overflow)
        })
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn overflow() -> crate::Error {
    crate::Error::NativeBoundary {
        code: crate::wire::ErrorCode::Invalid,
        reason: "search decode ownership overflow",
    }
}
