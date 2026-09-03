//! Canonical authenticated directory-page encoding.

use super::codec::{CanonicalDecodeError, DecodeLimits, Decoder, Encoder};
use super::codec::{DecodedPageKind, DecodedPageShape};
use super::types::{
    FileKind, LogicalName, NameEncoding, TreeChild, TreeEntry, TreePage, TreePageError,
    digest_object,
};
use crate::foundation::FileId;
use crate::storage::{ObjectId, ObjectKind, object_digest};

const DOMAIN: &[u8; 8] = b"ACYFSTRE";
const VERSION: u16 = 1;
const LEAF_TAG: u8 = 1;
const INTERNAL_TAG: u8 = 2;

/// Encodes one validated tree page into its stable versioned bytes.
///
/// # Errors
///
/// Rejects invalid ordering, bounds, or unrepresentable lengths.
pub fn encode_tree_page(
    page: &TreePage,
    maximum_items: u32,
) -> Result<Vec<u8>, CanonicalDecodeError> {
    match page {
        TreePage::Leaf(entries) => encode_tree_leaf_entries(entries, maximum_items),
        TreePage::Internal(children) => encode_tree_internal_children(
            children.iter().map(|child| (&child.first_name, child.page)),
            maximum_items,
        ),
    }
}

pub(crate) fn encode_tree_leaf_entries(
    entries: &[TreeEntry],
    maximum_items: u32,
) -> Result<Vec<u8>, CanonicalDecodeError> {
    if u32::try_from(entries.len()).unwrap_or(u32::MAX) > maximum_items {
        return Err(invariant(TreePageError::TooManyItems));
    }
    if entries.windows(2).any(|pair| pair[0].name >= pair[1].name) {
        return Err(invariant(TreePageError::NamesNotStrictlyOrdered));
    }
    let mut encoded_length = DOMAIN
        .len()
        .checked_add(2 + 1 + 4)
        .ok_or(CanonicalDecodeError::LengthOverflow)?;
    for entry in entries {
        encoded_length = encoded_length
            .checked_add(1 + 4 + 16 + 1)
            .and_then(|value| value.checked_add(entry.name.as_bytes().len()))
            .ok_or(CanonicalDecodeError::LengthOverflow)?;
    }
    let mut encoder = Encoder::with_exact_capacity(DOMAIN, VERSION, encoded_length)?;
    encoder.u8(LEAF_TAG);
    encoder.u32(count(entries.len())?);
    for entry in entries {
        encode_name(&mut encoder, &entry.name)?;
        encoder.fixed(&entry.file_id.into_bytes());
        encoder.u8(entry.kind.tag());
    }
    Ok(encoder.finish())
}

pub(crate) fn encode_tree_internal_children<'a, I>(
    children: I,
    maximum_items: u32,
) -> Result<Vec<u8>, CanonicalDecodeError>
where
    I: Clone + ExactSizeIterator<Item = (&'a LogicalName, ObjectId)>,
{
    let length = children.len();
    if length == 0 {
        return Err(invariant(TreePageError::EmptyInternalPage));
    }
    if u32::try_from(length).unwrap_or(u32::MAX) > maximum_items {
        return Err(invariant(TreePageError::TooManyItems));
    }
    let mut prior: Option<&LogicalName> = None;
    let mut encoded_length = DOMAIN
        .len()
        .checked_add(2 + 1 + 4)
        .ok_or(CanonicalDecodeError::LengthOverflow)?;
    for (name, page) in children.clone() {
        if prior.is_some_and(|value| value >= name) {
            return Err(invariant(TreePageError::NamesNotStrictlyOrdered));
        }
        if page.kind != ObjectKind::TreePage {
            return Err(invariant(TreePageError::WrongChildKind));
        }
        encoded_length = encoded_length
            .checked_add(1 + 4 + 32)
            .and_then(|value| value.checked_add(name.as_bytes().len()))
            .ok_or(CanonicalDecodeError::LengthOverflow)?;
        prior = Some(name);
    }
    let mut encoder = Encoder::with_exact_capacity(DOMAIN, VERSION, encoded_length)?;
    encoder.u8(INTERNAL_TAG);
    encoder.u32(count(length)?);
    for (name, page) in children {
        encode_name(&mut encoder, name)?;
        encoder.fixed(page.digest.as_bytes());
    }
    Ok(encoder.finish())
}

pub(crate) fn tree_page_decode_shape(
    bytes: &[u8],
    limits: DecodeLimits,
) -> Result<DecodedPageShape, CanonicalDecodeError> {
    let mut decoder = Decoder::new(bytes, DOMAIN, VERSION, limits.maximum_page_object_bytes())?;
    let tag = decoder.u8()?;
    let item_count = decoder.u32()?;
    if item_count > limits.maximum_page_items {
        return Err(CanonicalDecodeError::FieldTooLarge {
            observed: item_count,
            maximum: limits.maximum_page_items,
        });
    }
    let mut nested_bytes = 0_u64;
    let kind = match tag {
        LEAF_TAG => {
            for _ in 0..item_count {
                decoder.u8()?;
                let name_bytes = decoder.skip_bounded_bytes(limits.maximum_name_bytes)?;
                nested_bytes = nested_bytes
                    .checked_add(u64::try_from(name_bytes).unwrap_or(u64::MAX))
                    .ok_or(CanonicalDecodeError::LengthOverflow)?;
                let _: [u8; 16] = decoder.fixed()?;
                decoder.u8()?;
            }
            DecodedPageKind::Leaf
        }
        INTERNAL_TAG => {
            for _ in 0..item_count {
                decoder.u8()?;
                let name_bytes = decoder.skip_bounded_bytes(limits.maximum_name_bytes)?;
                nested_bytes = nested_bytes
                    .checked_add(u64::try_from(name_bytes).unwrap_or(u64::MAX))
                    .ok_or(CanonicalDecodeError::LengthOverflow)?;
                let _: [u8; 32] = decoder.fixed()?;
            }
            DecodedPageKind::Internal
        }
        value => {
            return Err(CanonicalDecodeError::UnknownTag {
                field: "tree_page",
                tag: value,
            });
        }
    };
    decoder.finish()?;
    Ok(DecodedPageShape {
        kind,
        items: usize::try_from(item_count).map_err(|_| CanonicalDecodeError::LengthOverflow)?,
        nested_bytes,
    })
}

/// Decodes and semantically validates one canonical tree page.
///
/// # Errors
///
/// Fails closed on unknown versions/tags, trailing/truncated bytes, allocation
/// bounds, malformed names, ordering, duplicate names, or invalid children.
pub fn decode_tree_page(
    bytes: &[u8],
    limits: DecodeLimits,
) -> Result<TreePage, CanonicalDecodeError> {
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
            let mut entries = Vec::new();
            entries
                .try_reserve_exact(capacity)
                .map_err(|_| CanonicalDecodeError::AllocationFailed)?;
            for _ in 0..item_count {
                let name = decode_name(&mut decoder, limits.maximum_name_bytes)?;
                let file_id = FileId::from_bytes(decoder.fixed()?);
                let kind = FileKind::from_tag(decoder.u8()?).map_err(invariant)?;
                entries.push(TreeEntry {
                    name,
                    file_id,
                    kind,
                });
            }
            TreePage::Leaf(entries)
        }
        INTERNAL_TAG => {
            let mut children = Vec::new();
            children
                .try_reserve_exact(capacity)
                .map_err(|_| CanonicalDecodeError::AllocationFailed)?;
            for _ in 0..item_count {
                let first_name = decode_name(&mut decoder, limits.maximum_name_bytes)?;
                let page = digest_object(ObjectKind::TreePage, decoder.fixed()?);
                children.push(TreeChild { first_name, page });
            }
            TreePage::Internal(children)
        }
        tag => {
            return Err(CanonicalDecodeError::UnknownTag {
                field: "tree_page",
                tag,
            });
        }
    };
    decoder.finish()?;
    page.validate(limits.maximum_page_items)
        .map_err(invariant)?;
    Ok(page)
}

/// Computes the typed authenticated identity of one validated tree page.
///
/// # Errors
///
/// Returns the same validation failures as [`encode_tree_page`].
pub fn tree_page_id(page: &TreePage, maximum_items: u32) -> Result<ObjectId, CanonicalDecodeError> {
    let bytes = encode_tree_page(page, maximum_items)?;
    Ok(ObjectId {
        kind: ObjectKind::TreePage,
        digest: object_digest(ObjectKind::TreePage, &bytes),
    })
}

fn encode_name(encoder: &mut Encoder, name: &LogicalName) -> Result<(), CanonicalDecodeError> {
    encoder.u8(name.encoding().tag());
    encoder.bounded_bytes(name.as_bytes())
}

fn decode_name(
    decoder: &mut Decoder<'_>,
    maximum_name_bytes: u32,
) -> Result<LogicalName, CanonicalDecodeError> {
    let encoding = NameEncoding::from_tag(decoder.u8()?).map_err(invariant)?;
    let bytes = decoder.bounded_bytes(maximum_name_bytes)?;
    LogicalName::new(encoding, bytes, maximum_name_bytes).map_err(invariant)
}

fn count(length: usize) -> Result<u32, CanonicalDecodeError> {
    u32::try_from(length).map_err(|_| CanonicalDecodeError::LengthOverflow)
}

fn invariant(error: TreePageError) -> CanonicalDecodeError {
    CanonicalDecodeError::Invariant(error.to_string())
}

#[cfg(test)]
#[path = "tests/tree.rs"]
mod tests;
