use super::{FetchRequest, FetchResponse};
use crate::{Result, filesystem::UploadRecord};

fn labels() -> u64 {
    crate::executor::json::preparation_serde_diagnostic_label_bytes()
        + [
            "FetchRequest",
            "FetchResponse",
            "url",
            "source_url",
            "final_url",
            "status",
            "content_type",
            "content",
        ]
        .iter()
        .map(|label| label.len() as u64 + 4)
        .sum::<u64>()
}

impl UploadRecord for FetchRequest {
    fn logical_owned_bytes(&self) -> Result<u64> {
        Ok(self.url.capacity() as u64)
    }

    fn diagnostic_label_bytes() -> u64 {
        labels()
    }
}

impl UploadRecord for FetchResponse {
    fn logical_owned_bytes(&self) -> Result<u64> {
        (self.source_url.capacity() as u64)
            .checked_add(self.final_url.capacity() as u64)
            .and_then(|n| n.checked_add(self.content.capacity() as u64))
            .and_then(|n| {
                n.checked_add(
                    self.content_type
                        .as_ref()
                        .map_or(0, |s| s.capacity() as u64),
                )
            })
            .ok_or_else(overflow)
    }

    fn diagnostic_label_bytes() -> u64 {
        labels()
    }
}

#[cfg(feature = "search-http")]
pub(crate) fn response_bytes(response: &FetchResponse) -> Result<u64> {
    response
        .logical_owned_bytes()?
        .checked_add(std::mem::size_of::<FetchResponse>() as u64)
        .ok_or_else(overflow)
}

fn overflow() -> crate::Error {
    crate::Error::NativeBoundary {
        code: crate::wire::ErrorCode::Invalid,
        reason: "fetch response ownership overflow",
    }
}
