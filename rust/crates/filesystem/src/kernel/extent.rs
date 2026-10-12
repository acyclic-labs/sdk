//! Canonical authenticated sparse-file extent-page encoding.

use super::codec::{
    CanonicalDecodeError, DecodeLimits, Decoder, Encoder,
};
use super::types::{Extent, ExtentChild, ExtentKind, ExtentPage, ExtentPageError, digest_object};
use crate::storage::{ObjectId, ObjectKind, object_digest};
use super::allocation::{KernelAccounting, KernelOwnedAllocation};
use super::file_read::{ReadOwned, ReadDecodeError, reserve_decoded_items};
use super::codec::ReadAllocationStep;

const DOMAIN: &[u8; 8] = b"ACYFSEXT";
const VERSION: u16 = 1;
const LEAF_TAG: u8 = 1;
const INTERNAL_TAG: u8 = 2;

/// Uses the one canonical writer with caller-controlled reserve and header observations.
/// Encodes one canonical page with physical allocation observation.
pub fn encode_extent_page<E: From<CanonicalDecodeError>>(
    page: &ExtentPage, maximum_items: u32,
    observe: impl FnMut(super::codec::EncodeAllocationStep) -> Result<(), E>,
    allocation_error: impl FnOnce(std::collections::TryReserveError) -> E,
) -> Result<Vec<u8>, E> {
    let encoded_length = extent_page_encoded_length(page, maximum_items)?;
    let mut encoder = Encoder::with_exact_capacity(
        DOMAIN, VERSION, encoded_length, observe, allocation_error,
    )?;
    match page {
        ExtentPage::Leaf(extents) => {
            encoder.u8(LEAF_TAG);
            encoder.u32(count(extents.len())?);
            for extent in extents {
                encoder.u64(extent.offset);
                encoder.u64(extent.length);
                match extent.kind {
                    ExtentKind::Hole => encoder.u8(1),
                    ExtentKind::AllocatedZero => encoder.u8(2),
                    ExtentKind::Content {
                        object,
                        object_offset,
                    } => {
                        encoder.u8(3);
                        encoder.fixed(object.digest.as_bytes());
                        encoder.u64(object_offset);
                    }
                }
            }
        }
        ExtentPage::Internal(children) => {
            encoder.u8(INTERNAL_TAG);
            encoder.u32(count(children.len())?);
            for child in children {
                encoder.u64(child.first_offset);
                encoder.u64(child.end_offset);
                encoder.fixed(child.page.digest.as_bytes());
            }
        }
    }
    Ok(encoder.finish())
}

pub(crate) fn extent_page_encoded_length(
    page: &ExtentPage,
    maximum_items: u32,
) -> Result<usize, CanonicalDecodeError> {
    page.validate(maximum_items).map_err(invariant)?;
    let mut bytes = DOMAIN
        .len()
        .checked_add(2 + 1 + 4)
        .ok_or(CanonicalDecodeError::LengthOverflow)?;
    match page {
        ExtentPage::Leaf(extents) => {
            for extent in extents {
                let item_bytes = match extent.kind {
                    ExtentKind::Hole | ExtentKind::AllocatedZero => 8 + 8 + 1,
                    ExtentKind::Content { .. } => 8 + 8 + 1 + 32 + 8,
                };
                bytes = bytes
                    .checked_add(item_bytes)
                    .ok_or(CanonicalDecodeError::LengthOverflow)?;
            }
        }
        ExtentPage::Internal(children) => {
            bytes = children
                .len()
                .checked_mul(8 + 8 + 32)
                .and_then(|items| bytes.checked_add(items))
                .ok_or(CanonicalDecodeError::LengthOverflow)?;
        }
    }
    Ok(bytes)
}

pub(crate) fn decode_extent_page_with_accounting(
    bytes: &[u8], limits: DecodeLimits, accounting: &mut KernelAccounting<'_>,
) -> Result<(ExtentPage, u64, KernelOwnedAllocation), crate::FsError> {
    #[cfg(not(target_arch = "wasm32"))]
    let native = accounting.native_work().is_some();
    let decoded = super::file_read::decode_owned(accounting, |observe| decode_extent_page(bytes, limits, &mut |step| observe(step)));
    let page = match decoded {
        Ok(page) => page,
        Err(error) => {
            #[cfg(not(target_arch = "wasm32"))]
            if native {
                return Err(match error.into_original_fs_error() {
                    crate::FsError::Decode(error) => crate::FsError::FileRead(super::FileRangeReadError::Extent(super::ExtentReadError::Decode(error))),
                    error => error,
                });
            }
            return Err(match error {
                ReadDecodeError::Canonical(error) => crate::FsError::FileRead(super::FileRangeReadError::Extent(super::ExtentReadError::Decode(error))),
                ReadDecodeError::AllocationFailed(_) => crate::FsError::FileRead(super::FileRangeReadError::Extent(super::ExtentReadError::Decode(CanonicalDecodeError::AllocationFailed))),
                ReadDecodeError::Accounting(error) => error,
            });
        }
    };
    let owner = ReadOwned { value: page.0, custody: page.1 };
    let bytes = match &owner.value {
        ExtentPage::Leaf(items) => items.capacity() * std::mem::size_of::<Extent>(),
        ExtentPage::Internal(items) => items.capacity() * std::mem::size_of::<ExtentChild>(),
    };
    Ok((owner.value, crate::foundation::usize_to_u64(bytes), owner.custody))
}

/// Decodes one canonical page with physical allocation observation.
pub fn decode_extent_page<E: From<CanonicalDecodeError>>(
    bytes: &[u8], limits: DecodeLimits,
    observe: &mut impl FnMut(ReadAllocationStep) -> Result<(), E>,
) -> Result<ExtentPage, E> {
    let mut decoder = Decoder::new(bytes, DOMAIN, VERSION, limits.maximum_page_object_bytes(), observe)?;
    let page_tag = decoder.u8(observe)?;
    let item_count = decoder.u32(observe)?;
    if item_count > limits.maximum_page_items {
        return Err(CanonicalDecodeError::FieldTooLarge {
            observed: item_count,
            maximum: limits.maximum_page_items,
        }.into());
    }
    let capacity = usize::try_from(item_count).map_err(|_| CanonicalDecodeError::LengthOverflow)?;
    let page = match page_tag {
        LEAF_TAG => {
            let mut extents = Vec::new();
            reserve_decoded_items(&mut extents, capacity, observe)?;
            for _ in 0..item_count {
                observe(ReadAllocationStep::BeforeItem)?;
                let offset = decoder.u64(observe)?;
                let length = decoder.u64(observe)?;
                let kind = match decoder.u8(observe)? {
                    1 => ExtentKind::Hole,
                    2 => ExtentKind::AllocatedZero,
                    3 => ExtentKind::Content {
                        object: digest_object(ObjectKind::Blob, decoder.fixed::<32, E>(observe)?),
                        object_offset: decoder.u64(observe)?,
                    },
                    tag => {
                        return Err(CanonicalDecodeError::UnknownTag {
                            field: "extent_kind",
                            tag,
                        }.into());
                    }
                };
                extents.push(Extent {
                    offset,
                    length,
                    kind,
                });
                observe(ReadAllocationStep::Item)?;
            }
            ExtentPage::Leaf(extents)
        }
        INTERNAL_TAG => {
            let mut children = Vec::new();
            reserve_decoded_items(&mut children, capacity, observe)?;
            for _ in 0..item_count {
                observe(ReadAllocationStep::BeforeItem)?;
                children.push(ExtentChild {
                    first_offset: decoder.u64(observe)?,
                    end_offset: decoder.u64(observe)?,
                    page: digest_object(ObjectKind::ExtentPage, decoder.fixed::<32, E>(observe)?),
                });
                observe(ReadAllocationStep::Item)?;
            }
            ExtentPage::Internal(children)
        }
        tag => {
            return Err(CanonicalDecodeError::UnknownTag {
                field: "extent_page",
                tag,
            }.into());
        }
    };
    decoder.finish()?;
    page.validate(limits.maximum_page_items)
        .map_err(invariant)?;
    Ok(page)
}

/// Computes the typed authenticated identity of one sparse extent page.
pub fn extent_page_id(
    page: &ExtentPage,
    maximum_items: u32,
) -> Result<ObjectId, CanonicalDecodeError> {
    let bytes = encode_extent_page(page, maximum_items, |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()), |_| crate::kernel::CanonicalDecodeError::AllocationFailed)?;
    Ok(ObjectId {
        kind: ObjectKind::ExtentPage,
        digest: object_digest(ObjectKind::ExtentPage, &bytes),
    })
}

fn count(length: usize) -> Result<u32, CanonicalDecodeError> {
    u32::try_from(length).map_err(|_| CanonicalDecodeError::LengthOverflow)
}

fn invariant(error: ExtentPageError) -> CanonicalDecodeError {
    CanonicalDecodeError::Invariant(super::codec::CanonicalInvariant::Extent(error))
}

#[cfg(test)]
#[path = "tests/extent.rs"]
mod tests;
