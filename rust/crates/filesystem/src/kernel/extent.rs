//! Canonical authenticated sparse-file extent-page encoding.

use super::codec::{
    CanonicalDecodeError, DecodeLimits, DecodedPageKind, DecodedPageShape, Decoder, Encoder,
};
use super::types::{Extent, ExtentChild, ExtentKind, ExtentPage, ExtentPageError, digest_object};
use crate::storage::{ObjectId, ObjectKind, object_digest};

const DOMAIN: &[u8; 8] = b"ACYFSEXT";
const VERSION: u16 = 1;
const LEAF_TAG: u8 = 1;
const INTERNAL_TAG: u8 = 2;

/// Encodes one validated sparse extent page.
///
/// # Errors
///
/// Rejects invalid ordering, ranges, object classes, or bounds.
pub fn encode_extent_page(
    page: &ExtentPage,
    maximum_items: u32,
) -> Result<Vec<u8>, CanonicalDecodeError> {
    let encoded_length = extent_page_encoded_length(page, maximum_items)?;
    let mut encoder = Encoder::with_exact_capacity(DOMAIN, VERSION, encoded_length)?;
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

pub(crate) fn extent_page_decode_shape(
    bytes: &[u8],
    limits: DecodeLimits,
) -> Result<DecodedPageShape, CanonicalDecodeError> {
    let mut decoder = Decoder::new(bytes, DOMAIN, VERSION, limits.maximum_page_object_bytes())?;
    let page_tag = decoder.u8()?;
    let item_count = decoder.u32()?;
    if item_count > limits.maximum_page_items {
        return Err(CanonicalDecodeError::FieldTooLarge {
            observed: item_count,
            maximum: limits.maximum_page_items,
        });
    }
    let kind = match page_tag {
        LEAF_TAG => {
            for _ in 0..item_count {
                decoder.u64()?;
                decoder.u64()?;
                match decoder.u8()? {
                    1 | 2 => {}
                    3 => {
                        let _: [u8; 32] = decoder.fixed()?;
                        decoder.u64()?;
                    }
                    tag => {
                        return Err(CanonicalDecodeError::UnknownTag {
                            field: "extent_kind",
                            tag,
                        });
                    }
                }
            }
            DecodedPageKind::Leaf
        }
        INTERNAL_TAG => {
            for _ in 0..item_count {
                decoder.u64()?;
                decoder.u64()?;
                let _: [u8; 32] = decoder.fixed()?;
            }
            DecodedPageKind::Internal
        }
        tag => {
            return Err(CanonicalDecodeError::UnknownTag {
                field: "extent_page",
                tag,
            });
        }
    };
    decoder.finish()?;
    Ok(DecodedPageShape {
        kind,
        items: usize::try_from(item_count).map_err(|_| CanonicalDecodeError::LengthOverflow)?,
        nested_bytes: 0,
    })
}

/// Decodes and validates one bounded sparse extent page.
///
/// # Errors
///
/// Fails closed on malformed canonical bytes or semantic page invariants.
pub fn decode_extent_page(
    bytes: &[u8],
    limits: DecodeLimits,
) -> Result<ExtentPage, CanonicalDecodeError> {
    let mut decoder = Decoder::new(bytes, DOMAIN, VERSION, limits.maximum_page_object_bytes())?;
    let page_tag = decoder.u8()?;
    let item_count = decoder.u32()?;
    if item_count > limits.maximum_page_items {
        return Err(CanonicalDecodeError::FieldTooLarge {
            observed: item_count,
            maximum: limits.maximum_page_items,
        });
    }
    let capacity = usize::try_from(item_count).map_err(|_| CanonicalDecodeError::LengthOverflow)?;
    let page = match page_tag {
        LEAF_TAG => {
            let mut extents = Vec::new();
            extents
                .try_reserve_exact(capacity)
                .map_err(|_| CanonicalDecodeError::AllocationFailed)?;
            for _ in 0..item_count {
                let offset = decoder.u64()?;
                let length = decoder.u64()?;
                let kind = match decoder.u8()? {
                    1 => ExtentKind::Hole,
                    2 => ExtentKind::AllocatedZero,
                    3 => ExtentKind::Content {
                        object: digest_object(ObjectKind::Blob, decoder.fixed()?),
                        object_offset: decoder.u64()?,
                    },
                    tag => {
                        return Err(CanonicalDecodeError::UnknownTag {
                            field: "extent_kind",
                            tag,
                        });
                    }
                };
                extents.push(Extent {
                    offset,
                    length,
                    kind,
                });
            }
            ExtentPage::Leaf(extents)
        }
        INTERNAL_TAG => {
            let mut children = Vec::new();
            children
                .try_reserve_exact(capacity)
                .map_err(|_| CanonicalDecodeError::AllocationFailed)?;
            for _ in 0..item_count {
                children.push(ExtentChild {
                    first_offset: decoder.u64()?,
                    end_offset: decoder.u64()?,
                    page: digest_object(ObjectKind::ExtentPage, decoder.fixed()?),
                });
            }
            ExtentPage::Internal(children)
        }
        tag => {
            return Err(CanonicalDecodeError::UnknownTag {
                field: "extent_page",
                tag,
            });
        }
    };
    decoder.finish()?;
    page.validate(limits.maximum_page_items)
        .map_err(invariant)?;
    Ok(page)
}

/// Computes the typed authenticated identity of one sparse extent page.
///
/// # Errors
///
/// Returns the same validation failures as [`encode_extent_page`].
pub fn extent_page_id(
    page: &ExtentPage,
    maximum_items: u32,
) -> Result<ObjectId, CanonicalDecodeError> {
    let bytes = encode_extent_page(page, maximum_items)?;
    Ok(ObjectId {
        kind: ObjectKind::ExtentPage,
        digest: object_digest(ObjectKind::ExtentPage, &bytes),
    })
}

fn count(length: usize) -> Result<u32, CanonicalDecodeError> {
    u32::try_from(length).map_err(|_| CanonicalDecodeError::LengthOverflow)
}

fn invariant(error: ExtentPageError) -> CanonicalDecodeError {
    CanonicalDecodeError::Invariant(error.to_string())
}

#[cfg(test)]
#[path = "tests/extent.rs"]
mod tests;
