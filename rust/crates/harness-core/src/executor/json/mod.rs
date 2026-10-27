//! Bounded JSON at the executor's existing IO boundary.
use crate::{Error, Result};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;

mod admission;
mod arrays;
mod identity;
mod sequence;
pub(crate) use identity::borrowed_json_digest;
pub(crate) use sequence::canonical_sequence_digests;
#[cfg(test)]
mod item_budget_tests;
mod records;
#[doc(hidden)]
pub use identity::{
    canonical_json_digest, json_byte_length, json_byte_length_at_depth, serialized_json_digest,
};

/// Nesting accepted by saved preparation-record readers.
pub(crate) const RECORD_MAXIMUM_DEPTH: u32 = 128;
mod diagnostics;
#[cfg(feature = "filesystem")]
pub(crate) use diagnostics::json_typed_diagnostic_source_owned_bytes;
pub(crate) use diagnostics::{audited_serde_diagnostic_bytes, history_diagnostic_bytes};
pub use diagnostics::{
    json_typed_diagnostic_owned_bytes, preparation_serde_diagnostic_label_bytes,
};
#[cfg(feature = "filesystem")]
pub(crate) use serialization::canonical_digest_with_work;
pub(crate) use serialization::count_json_bytes_trusted;
mod copy;
mod decode;
pub(crate) mod layout;
#[cfg(test)]
pub(crate) use layout::JsonLayout;
#[cfg(feature = "filesystem")]
pub(crate) use layout::LayoutCost;
mod schema;
mod serialization;
#[cfg(test)]
pub(crate) use admission::admit_json_layout;
pub(crate) use admission::admit_value_layout;
#[cfg(test)]
pub(crate) use admission::assert_layout_covers_decode;
pub(crate) use admission::json_owned_value_shape_bounded;
#[cfg(test)]
pub(crate) use admission::layout_reservation;
#[cfg(feature = "filesystem")]
pub(crate) use admission::{admit_json, lexical_json_diagnostic_bytes};
pub use records::json_value_logical_owned_bytes;
pub(crate) use records::json_value_retained_owned_bytes;
pub use records::{json_from_slice_bounded, json_from_value_bounded};
pub use serialization::{
    canonical_json_bytes_bounded, json_bytes_bounded, json_value_bounded, json_value_copy_bounded,
};

/// Fixed JSON input failures, distinct from exhausted work allowances.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum JsonInputError {
    /// A closing container has no matching opener.
    #[error("invalid JSON nesting")]
    Nesting,
    /// The source ends inside a string or container.
    #[error("incomplete JSON document")]
    Incomplete,
    /// A value exceeds the fixed nesting limit.
    #[error("JSON nesting limit exceeded")]
    Depth,
    /// The document has a shape its typed decode rejects.
    #[error("JSON shape is outside its typed layout")]
    Shape,
}

fn input_error(kind: JsonInputError, code: Option<crate::wire::ErrorCode>) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(code) = code {
        return Error::NativeJsonAdmission { code, kind };
    }
    let _ = code;
    Error::Invalid(kind.to_string())
}

/// Finite byte, node and nesting admission for one document.
#[derive(Clone, Copy, Debug)]
pub struct JsonBounds {
    /// UTF-8 input/output bytes.
    pub maximum_bytes: u64,
    /// Values and keys, including replaced duplicate entries.
    pub maximum_items: u64,
    /// Zero-based nesting depth.
    pub maximum_depth: u32,
}

pub use schema::JsonValueSchema;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Origin {
    Slice,
    Owned,
    Borrowed,
}

/// Counts from the original source or resident-tree inspection.
#[derive(Clone, Copy, Debug)]
pub struct JsonValueShape {
    input_bytes: u64,
    lexical_items: u64,
    containers: u64,
    virtual_items: u64,
    raw_bytes: u64,
    raw_frames: u64,
    unresolved_raw: bool,
    origin: Origin,
    /// Longest string or key a typed layout walk decodes, which bounds the
    /// source text a decode diagnostic quotes.
    longest_string: u64,
}
impl JsonValueShape {
    /// Longest string or key of a typed decode, or the input when the shape
    /// came from an untyped walk.
    pub const fn diagnostic_source_bytes(self) -> u64 {
        if self.longest_string == 0 {
            self.input_bytes
        } else {
            self.longest_string
        }
    }
    /// Object and array openings when the original source was bytes.
    pub const fn lexical_containers(self) -> Option<u64> {
        match self.origin {
            Origin::Slice => Some(self.containers),
            _ => None,
        }
    }
    /// Bytes inspected on the original source.
    pub const fn input_bytes(self) -> u64 {
        self.input_bytes
    }
    /// Original values and map keys.
    pub const fn lexical_items(self) -> u64 {
        self.lexical_items
    }
    /// Nodes in decoded `RawValue` sources.
    pub const fn virtual_items(self) -> u64 {
        self.virtual_items
    }
    /// Whether `RawValue` requires the conservative byte-based allowance.
    pub const fn unresolved_raw(self) -> bool {
        self.unresolved_raw
    }
}

/// Work or storage admitted at the existing IO boundary.
#[derive(Clone, Copy, Debug)]
pub enum JsonCodecCharge {
    /// Encoding and structural traversal.
    Work {
        /// Encoded bytes.
        bytes: u64,
        /// Visited items.
        items: u64,
    },
    /// Storage held before allocation.
    Retain {
        /// Logical payload/slot bytes.
        bytes: u64,
    },
    /// Storage released after its data is freed.
    Release {
        /// Released bytes.
        bytes: u64,
    },
    /// Copy allowance before allocating its buffer.
    CopyAdmission {
        /// Bytes to copy.
        bytes: u64,
    },
    /// One genuine positive SDK `try_reserve` operation.
    AllocationOperation {
        /// Full requested capacity.
        bytes: u64,
    },
    /// Atomic conservative node-allocation bound for one complete object.
    AllocationOperations {
        /// Maximum number of physical node allocations, from entry count.
        operations: u64,
    },
    /// Copy into an admitted buffer.
    Copy {
        /// Copied bytes.
        bytes: u64,
    },
    /// Consumption of borrowed source bytes.
    Read {
        /// Consumed bytes.
        bytes: u64,
    },
    /// Resident nodes or virtual decoded bytes inspected.
    Inspect {
        /// Inspection units.
        items: u64,
    },
    /// Preflight for the audited schema and decoder scratch.
    Admission {
        /// Maximum new owned bytes.
        maximum_owned_bytes: u64,
        /// Counts from the original source pass.
        value_shape: JsonValueShape,
    },
}

fn boundary(reason: &'static str, code: Option<crate::wire::ErrorCode>) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(code) = code {
        return Error::NativeBoundary { code, reason };
    }
    let _ = code;
    Error::Invalid(reason.into())
}
fn checked<T>(value: Option<T>, reason: &'static str) -> Result<T> {
    value.ok_or_else(|| boundary(reason, Some(crate::wire::ErrorCode::Invalid)))
}

/// Closed codec/RawValue diagnostic allowance.
pub const fn sdk_json_diagnostic_bytes() -> u64 {
    let rendered = concat!(
        "memory allocation failed",
        " because the computed capacity exceeded the collection's maximum"
    )
    .len() as u64
        + "invalid request: ".len() as u64
        + " at line ".len() as u64
        + " column ".len() as u64
        + 2 * (u64::MAX.ilog10() as u64 + 1);
    4 * rendered
        + 2 * (std::mem::size_of::<serde_json::Error>()
            + std::mem::size_of::<std::io::Error>()
            + std::mem::size_of::<String>()
            + std::mem::size_of::<Error>()) as u64
}
fn nodes(shape: JsonValueShape) -> Option<u64> {
    if shape.unresolved_raw {
        Some(shape.input_bytes.max(shape.lexical_items))
    } else {
        shape.lexical_items.checked_add(shape.virtual_items)
    }
}
fn storage(shape: JsonValueShape) -> Option<u64> {
    let slots = nodes(shape)?.checked_mul(
        (10 * std::mem::size_of::<Value>() + std::mem::size_of::<(String, Value)>()) as u64,
    )?;
    let raw = if shape.unresolved_raw {
        shape
            .input_bytes
            .max(8)
            .checked_mul(4 * (u64::from(usize::BITS) + 2))?
    } else {
        shape
            .raw_bytes
            .checked_mul(4)?
            .checked_add(shape.raw_frames.checked_mul(16)?)?
    };
    slots
        .checked_add(raw)?
        .checked_add(sdk_json_diagnostic_bytes())
}
fn full(shape: JsonValueShape) -> Option<u64> {
    storage(shape)?.checked_add(shape.input_bytes.max(8).checked_mul(4)?)
}
/// New logical ownership of an admitted Value decoder.
pub fn json_value_decode_owned_bytes(shape: JsonValueShape) -> Result<u64> {
    checked(full(shape), "JSON owned bound overflow")
}
/// New storage needed by a genuine consuming Value conversion.
pub fn json_owned_value_conversion_additional_bytes(shape: JsonValueShape) -> Result<u64> {
    if shape.origin != Origin::Owned {
        return Err(boundary(
            "JSON conversion shape is not owned",
            Some(crate::wire::ErrorCode::Invalid),
        ));
    }
    checked(storage(shape), "JSON owned bound overflow")
}

#[cfg(test)]
mod layout_guard_tests;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod value_shape_tests;

#[cfg(test)]
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) mod baseline;
#[cfg(test)]
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
mod differential_properties;
