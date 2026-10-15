use super::*;

/// Physical representation of one logical byte interval.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExtentKind {
    /// Unallocated bytes that read as zero.
    Hole,
    /// Physically allocated bytes whose canonical value is zero.
    AllocatedZero,
    /// Bytes stored in one immutable blob span.
    Content {
        /// Typed immutable blob.
        object: ObjectId,
        /// First byte used from the blob.
        object_offset: u64,
    },
}

/// One non-empty logical file interval.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Extent {
    /// Inclusive logical byte offset.
    pub offset: u64,
    /// Positive logical byte length.
    pub length: u64,
    /// Physical representation.
    pub kind: ExtentKind,
}

/// One lower-bound-to-page entry in an extent B+tree.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExtentChild {
    /// First logical byte represented by the child.
    pub first_offset: u64,
    /// Exclusive logical end represented by the child.
    pub end_offset: u64,
    /// Authenticated extent page.
    pub page: ObjectId,
}

/// One immutable authenticated extent B+tree page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExtentPage {
    /// Ordered non-overlapping extents.
    Leaf(Vec<Extent>),
    /// Ordered child lower bounds.
    Internal(Vec<ExtentChild>),
}

impl ExtentPage {
    /// Validates bounds, ordering, non-overlap, and typed child/content identities.
    ///
    /// # Errors
    ///
    /// Rejects malformed extent pages before hashing or publication.
    pub fn validate(&self, maximum_items: u32) -> Result<(), ExtentPageError> {
        let count = match self {
            Self::Leaf(extents) => {
                let mut previous_end = None;
                for extent in extents {
                    if extent.length == 0 {
                        return Err(ExtentPageError::ZeroLength);
                    }
                    let end = extent
                        .offset
                        .checked_add(extent.length)
                        .ok_or(ExtentPageError::RangeOverflow)?;
                    if previous_end.is_some_and(|prior| extent.offset != prior) {
                        return Err(ExtentPageError::NonContiguous);
                    }
                    if let ExtentKind::Content {
                        object,
                        object_offset,
                    } = extent.kind
                    {
                        if object.kind != ObjectKind::Blob {
                            return Err(ExtentPageError::WrongContentKind);
                        }
                        object_offset
                            .checked_add(extent.length)
                            .ok_or(ExtentPageError::RangeOverflow)?;
                    }
                    previous_end = Some(end);
                }
                extents.len()
            }
            Self::Internal(children) => {
                if children.is_empty() {
                    return Err(ExtentPageError::EmptyInternalPage);
                }
                if children.windows(2).any(|pair| {
                    let [left, right] = pair else {
                        unreachable!("windows(2) always yields exactly two elements")
                    };
                    left.end_offset != right.first_offset
                }) {
                    return Err(ExtentPageError::NonContiguous);
                }
                if children
                    .iter()
                    .any(|child| child.first_offset >= child.end_offset)
                {
                    return Err(ExtentPageError::InvalidChildRange);
                }
                if children
                    .iter()
                    .any(|child| child.page.kind != ObjectKind::ExtentPage)
                {
                    return Err(ExtentPageError::WrongChildKind);
                }
                children.len()
            }
        };
        if u32::try_from(count).unwrap_or(u32::MAX) > maximum_items {
            return Err(ExtentPageError::TooManyItems);
        }
        Ok(())
    }
}

/// Authenticated extent-page invariant failures.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ExtentPageError {
    /// Extents must represent at least one logical byte.
    #[error("extent length is zero")]
    ZeroLength,
    /// Logical or content-offset arithmetic overflowed.
    #[error("extent range overflowed")]
    RangeOverflow,
    /// Extents/children overlap, have a gap, or are out of order.
    #[error("extent page ranges are not contiguous")]
    NonContiguous,
    /// Content extent references another object class.
    #[error("extent content is not a blob object")]
    WrongContentKind,
    /// Internal page has no routed children.
    #[error("internal extent page is empty")]
    EmptyInternalPage,
    /// Internal child has an empty or reversed range.
    #[error("extent child range is empty or reversed")]
    InvalidChildRange,
    /// Internal child references another object class.
    #[error("extent child is not an extent page")]
    WrongChildKind,
    /// Page exceeds the admitted item bound.
    #[error("extent page exceeds its item bound")]
    TooManyItems,
    /// Decoder encountered an unknown physical representation.
    #[error("unknown extent kind {0}")]
    UnknownKind(u8),
}
