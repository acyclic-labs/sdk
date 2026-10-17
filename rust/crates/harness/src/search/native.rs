use super::{SearchRequest, SearchResponse};
use crate::{Result, filesystem::UploadRecord};
#[cfg(feature = "test-support")]
use acyclic_fs::{NativeAllocation, NativeMaterializationWork};

pub(crate) fn labels() -> u64 {
    crate::executor::json::preparation_serde_diagnostic_label_bytes()
        + [
            "SearchRequest",
            "SearchResponse",
            "SearchResult",
            "ExaResponse",
            "ExaResult",
            "TinyFishResponse",
            "TinyFishResult",
            "query",
            "max_results",
            "results",
            "url",
            "title",
            "snippet",
            "highlights",
        ]
        .iter()
        .map(|s| s.len() as u64 + 4)
        .sum::<u64>()
}

impl UploadRecord for SearchRequest {
    fn logical_owned_bytes(&self) -> Result<u64> {
        Ok(self.query.capacity() as u64)
    }
    fn diagnostic_label_bytes() -> u64 {
        labels()
    }
}

impl UploadRecord for SearchResponse {
    fn diagnostic_label_bytes() -> u64 {
        labels()
    }
    fn logical_owned_bytes(&self) -> Result<u64> {
        self.results.iter().try_fold(
            (self.results.capacity() as u64)
                .checked_mul(std::mem::size_of::<super::SearchResult>() as u64)
                .ok_or(crate::Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Invalid,
                    reason: "search result ownership overflow",
                })?,
            |bytes, item| {
                bytes
                    .checked_add(item.url.capacity() as u64)
                    .and_then(|n| n.checked_add(item.title.capacity() as u64))
                    .and_then(|n| n.checked_add(item.snippet.capacity() as u64))
                    .ok_or(crate::Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Invalid,
                        reason: "search result ownership overflow",
                    })
            },
        )
    }
}

pub(crate) fn response_bytes(response: &SearchResponse) -> Result<u64> {
    response
        .logical_owned_bytes()?
        .checked_add(std::mem::size_of::<SearchResponse>() as u64)
        .ok_or(crate::Error::NativeBoundary {
            code: crate::wire::ErrorCode::Invalid,
            reason: "search result ownership overflow",
        })
}

#[cfg(feature = "test-support")]
pub(crate) fn copy_response(
    response: &SearchResponse,
    work: &NativeMaterializationWork,
) -> Result<(SearchResponse, NativeAllocation)> {
    let json = crate::tool::value(response, u64::MAX, Some(work))?;
    let io = crate::executor::PreparationIo::from_native_work(work)?;
    let copied = io.value_decode(&json.0, crate::wire::ErrorCode::Invalid)?;
    let custody = io.into_native_custody(response_bytes(&copied)?)?;
    Ok((copied, custody))
}
