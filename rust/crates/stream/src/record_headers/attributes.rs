use super::*;
use std::{cmp::Ordering, ops::Range};

#[cfg(not(target_arch = "wasm32"))]
pub(super) struct PackedAttribute {
    pub(super) name: Range<usize>,
    pub(super) value: OwnedBytes,
}

pub(super) enum AttributeStorage {
    Owned(Vec<Attribute>),
    #[cfg(not(target_arch = "wasm32"))]
    Packed {
        bytes: OwnedBytes,
        values: Vec<PackedAttribute>,
    },
}

impl AttributeStorage {
    fn get(&self, index: usize) -> Option<RecordAttribute<'_>> {
        match self {
            Self::Owned(values) => values.get(index).map(|attribute| RecordAttribute {
                name: attribute.name.as_str(),
                value: &attribute.value,
            }),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Packed { bytes, values } => {
                let attribute = values.get(index)?;
                // Private preparation validates complete UTF-8 name ranges,
                // either in its write arena or in an authenticated read page.
                let Some(Ok(name)) = bytes.get(attribute.name.clone()).map(std::str::from_utf8)
                else {
                    unreachable!("prepared header names preserve their UTF-8 source ranges")
                };
                Some(RecordAttribute {
                    name,
                    value: &attribute.value,
                })
            }
        }
    }

    pub(super) fn len(&self) -> usize {
        match self {
            Self::Owned(values) => values.len(),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Packed { values, .. } => values.len(),
        }
    }

    pub(super) fn control_bytes(&self) -> usize {
        match self {
            Self::Owned(values) => {
                values.capacity() * std::mem::size_of::<Attribute>()
                    + values
                        .iter()
                        .map(|attribute| attribute.name.capacity())
                        .sum::<usize>()
            }
            // The shared byte arena retains its own native allocation owner.
            #[cfg(not(target_arch = "wasm32"))]
            Self::Packed { values, .. } => {
                values.capacity() * std::mem::size_of::<PackedAttribute>()
            }
        }
    }
}

/// A borrowed canonical attribute. Its name can live in a preallocated arena.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecordAttribute<'a> {
    /// Canonical UTF-8 name.
    pub name: &'a str,
    /// Immutable value with its original byte owner.
    pub value: &'a OwnedBytes,
}

impl RecordAttribute<'_> {
    /// Materializes an ordinary owned attribute for an outbound representation.
    #[must_use]
    pub fn to_owned(self) -> Attribute {
        Attribute {
            name: self.name.to_owned(),
            value: self.value.clone(),
        }
    }
}

/// A view over canonical attributes, including prepared native storage.
#[derive(Clone, Copy)]
pub struct RecordAttributes<'a>(pub(super) Option<&'a AttributeStorage>);

impl<'a> RecordAttributes<'a> {
    /// Number of canonical attributes.
    #[must_use]
    pub fn len(self) -> usize {
        self.0.map_or(0, AttributeStorage::len)
    }

    /// Whether this header has no attributes.
    #[must_use]
    pub fn is_empty(self) -> bool {
        self.len() == 0
    }

    /// Borrows one attribute without allocating its name.
    #[must_use]
    pub fn get(self, index: usize) -> Option<RecordAttribute<'a>> {
        self.0.and_then(|storage| storage.get(index))
    }

    /// Iterates in canonical name order without materializing names.
    #[must_use]
    pub fn iter(self) -> RecordAttributeIter<'a> {
        RecordAttributeIter {
            attributes: self,
            remaining: 0..self.len(),
        }
    }

    /// Searches the canonical sorted names.
    pub fn binary_search_by(
        self,
        mut compare: impl FnMut(RecordAttribute<'a>) -> Ordering,
    ) -> Result<usize, usize> {
        let mut left = 0;
        let mut right = self.len();
        while left < right {
            let middle = left + (right - left) / 2;
            let Some(attribute) = self.get(middle) else {
                unreachable!("attribute search stays inside its immutable view")
            };
            match compare(attribute) {
                Ordering::Less => left = middle + 1,
                Ordering::Greater => right = middle,
                Ordering::Equal => return Ok(middle),
            }
        }
        Err(left)
    }

    /// Copies the attributes into ordinary caller-owned names.
    #[must_use]
    pub fn to_vec(self) -> Vec<Attribute> {
        self.iter().map(RecordAttribute::to_owned).collect()
    }
}

impl PartialEq for RecordAttributes<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.iter().eq(other.iter())
    }
}
impl Eq for RecordAttributes<'_> {}
impl std::fmt::Debug for RecordAttributes<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}
impl<'a> IntoIterator for RecordAttributes<'a> {
    type Item = RecordAttribute<'a>;
    type IntoIter = RecordAttributeIter<'a>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// An allocation-free iterator over canonical attribute views.
pub struct RecordAttributeIter<'a> {
    attributes: RecordAttributes<'a>,
    remaining: Range<usize>,
}
impl<'a> Iterator for RecordAttributeIter<'a> {
    type Item = RecordAttribute<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        self.remaining
            .next()
            .and_then(|index| self.attributes.get(index))
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.remaining.size_hint()
    }
}
impl DoubleEndedIterator for RecordAttributeIter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.remaining
            .next_back()
            .and_then(|index| self.attributes.get(index))
    }
}
impl ExactSizeIterator for RecordAttributeIter<'_> {}
