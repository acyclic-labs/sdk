//! Per-type retained heap storage, excluding the value's enclosing inline slot.
//!
//! Fields are exhaustive: adding a field or enum case requires an accounting impl.
//! Buffer capacities and Box layouts are measured directly. std B-tree allocation
//! topology is opaque; its node storage uses a type-specific occupancy bound,
//! including an empty retained root. No wire length or schema-wide slot is used.
use crate::{Error, Result};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    mem::size_of_val,
};

pub(crate) trait OwnedBytes {
    fn owned_heap_bytes(&self) -> Result<u64>;
}

macro_rules! owned_struct {
    ($ty:ty { $($field:ident),* $(,)? }) => {
        impl crate::core::ownership::OwnedBytes for $ty {
            fn owned_heap_bytes(&self) -> crate::Result<u64> {
                let Self { $($field),* } = self;
                crate::core::owned_sum([$(
                    crate::core::ownership::OwnedBytes::owned_heap_bytes($field)?
                ),*])
            }
        }
    };
}
pub(crate) use owned_struct;
macro_rules! owned_tuple {
    ($ty:ty, ($($field:ident),* $(,)?)) => {
        impl crate::core::ownership::OwnedBytes for $ty {
            fn owned_heap_bytes(&self) -> crate::Result<u64> {
                let Self($($field),*) = self;
                crate::core::owned_sum([$(
                    crate::core::ownership::OwnedBytes::owned_heap_bytes($field)?
                ),*])
            }
        }
    };
}
pub(crate) use owned_tuple;
macro_rules! owned_enum {
    ($ty:ty { $($variant:ident $(($($tuple:ident),* $(,)?))? $({$($field:ident),* $(,)?})?),* $(,)? }) => {
        impl crate::core::ownership::OwnedBytes for $ty {
            fn owned_heap_bytes(&self) -> crate::Result<u64> {
                match self {
                    $(Self::$variant $(($($tuple),*))? $({$($field),*})? => crate::core::owned_sum([
                        $($(crate::core::ownership::OwnedBytes::owned_heap_bytes($tuple)?,)*)?
                        $($(crate::core::ownership::OwnedBytes::owned_heap_bytes($field)?,)*)?
                    ])),*
                }
            }
        }
    };
}
pub(crate) use owned_enum;

macro_rules! inline {
    ($($ty:ty),* $(,)?) => {$(
        impl OwnedBytes for $ty {
            fn owned_heap_bytes(&self) -> Result<u64> { Ok(0) }
        }
    )*};
}
inline!(
    (),
    bool,
    u8,
    u16,
    u32,
    u64,
    u128,
    usize,
    i8,
    i16,
    i32,
    i64,
    i128,
    isize,
    f32,
    f64,
    char,
    str,
    uuid::Uuid
);
owned_enum!(crate::conversation::VolumeOperation { Read, Write });
impl OwnedBytes for String {
    fn owned_heap_bytes(&self) -> Result<u64> {
        Ok(self.capacity() as u64)
    }
}
impl<T: OwnedBytes + ?Sized> OwnedBytes for Box<T> {
    fn owned_heap_bytes(&self) -> Result<u64> {
        super::owned_sum([
            size_of_val(self.as_ref()) as u64,
            self.as_ref().owned_heap_bytes()?,
        ])
    }
}
impl<T: OwnedBytes> OwnedBytes for Option<T> {
    fn owned_heap_bytes(&self) -> Result<u64> {
        self.as_ref().map_or(Ok(0), OwnedBytes::owned_heap_bytes)
    }
}
impl<T: OwnedBytes> OwnedBytes for [T] {
    fn owned_heap_bytes(&self) -> Result<u64> {
        sequence_heap(0, self.iter())
    }
}
impl<T: OwnedBytes, const N: usize> OwnedBytes for [T; N] {
    fn owned_heap_bytes(&self) -> Result<u64> {
        self.as_slice().owned_heap_bytes()
    }
}
impl<T: OwnedBytes> OwnedBytes for Vec<T> {
    fn owned_heap_bytes(&self) -> Result<u64> {
        sequence_heap(super::owned_buffer::<T>(self.capacity())?, self.iter())
    }
}
impl<T: OwnedBytes> OwnedBytes for VecDeque<T> {
    fn owned_heap_bytes(&self) -> Result<u64> {
        sequence_heap(super::owned_buffer::<T>(self.capacity())?, self.iter())
    }
}
fn sequence_heap<'a, T: OwnedBytes + 'a>(
    bytes: u64,
    mut values: impl Iterator<Item = &'a T>,
) -> Result<u64> {
    values.try_fold(bytes, |sum, value| {
        super::owned_sum([sum, value.owned_heap_bytes()?])
    })
}
impl<A: OwnedBytes, B: OwnedBytes> OwnedBytes for (A, B) {
    fn owned_heap_bytes(&self) -> Result<u64> {
        super::owned_sum([self.0.owned_heap_bytes()?, self.1.owned_heap_bytes()?])
    }
}
impl<K: OwnedBytes, V: OwnedBytes> OwnedBytes for BTreeMap<K, V> {
    fn owned_heap_bytes(&self) -> Result<u64> {
        self.iter()
            .try_fold(tree_nodes::<(K, V)>(self.len())?, |sum, (key, value)| {
                super::owned_sum([sum, key.owned_heap_bytes()?, value.owned_heap_bytes()?])
            })
    }
}
impl<K: OwnedBytes> OwnedBytes for BTreeSet<K> {
    fn owned_heap_bytes(&self) -> Result<u64> {
        sequence_heap(tree_nodes::<K>(self.len())?, self.iter())
    }
}
pub(crate) fn tree_nodes<T>(len: usize) -> Result<u64> {
    // Current std B-trees have eleven slots, a two-word header, and twelve
    // child pointers in internal nodes. Non-root nodes have >= five entries.
    // The public API cannot distinguish a fresh empty tree from an empty
    // tree retaining its leaf root after removal of its last entry.
    let nodes = 1 + len.saturating_sub(1) / 5;
    tree_node_bytes::<T>()?
        .checked_mul(nodes as u64)
        .ok_or_else(|| Error::Invalid("tree ownership overflows".into()))
}

fn tree_node_bytes<T>() -> Result<u64> {
    // Account for header/slot/edge padding too, including over-aligned keys.
    let layout = std::alloc::Layout::array::<T>(11)
        .ok()
        .and_then(|slots| std::alloc::Layout::new::<[usize; 2]>().extend(slots).ok())
        .and_then(|(leaf, _)| leaf.extend(std::alloc::Layout::new::<[usize; 12]>()).ok());
    let bytes = layout
        .map(|(node, _)| node.pad_to_align().size())
        .ok_or_else(|| Error::Invalid("tree ownership overflows".into()))?;
    Ok(bytes as u64)
}
impl OwnedBytes for serde_json::Value {
    fn owned_heap_bytes(&self) -> Result<u64> {
        json_heap(self, 0)
    }
}
fn json_heap(value: &serde_json::Value, depth: usize) -> Result<u64> {
    if depth >= 256 {
        return Err(Error::Invalid(
            "JSON ownership depth exceeds codec limit".into(),
        ));
    }
    match value {
        serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::Number(_) => {
            Ok(0)
        }
        serde_json::Value::String(value) => value.owned_heap_bytes(),
        serde_json::Value::Array(values) => values.iter().try_fold(
            super::owned_buffer::<serde_json::Value>(values.capacity())?,
            |sum, value| super::owned_sum([sum, json_heap(value, depth + 1)?]),
        ),
        serde_json::Value::Object(values) => values.iter().try_fold(
            tree_nodes::<(String, serde_json::Value)>(values.len())?,
            |sum, (key, value)| {
                super::owned_sum([sum, key.owned_heap_bytes()?, json_heap(value, depth + 1)?])
            },
        ),
    }
}
