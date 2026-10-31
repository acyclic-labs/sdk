//! The renderer's one immutable item type at the bounded journal codec boundary.
//!
//! Wire bodies persist as exact UTF-8 strings. Their nested JSON never becomes
//! a second decoded tree, and unknown provider fields remain opaque bytes.
use crate::heap_layout::{Alloc, HeapLayout, JsonLayout};
pub use acyclic_model_render::{Family, ItemKind, Provenance, WireItem};
use serde_json::value::RawValue;

impl HeapLayout for Family {
    const LAYOUT: &'static JsonLayout = &JsonLayout::Scalar;
}
impl HeapLayout for ItemKind {
    const LAYOUT: &'static JsonLayout = &JsonLayout::Scalar;
}
impl HeapLayout for Provenance {
    const LAYOUT: &'static JsonLayout = &JsonLayout::record(&[
        ("family", Family::LAYOUT),
        ("provider", String::LAYOUT),
        ("model", String::LAYOUT),
        ("endpoint", String::LAYOUT),
    ]);
}
impl HeapLayout for WireItem {
    const LAYOUT: &'static JsonLayout = &JsonLayout::record(&[
        ("version", u32::LAYOUT),
        ("provenance", Provenance::LAYOUT),
        ("kind", ItemKind::LAYOUT),
        // RawValue::from_string moves the decoded String into its owned box.
        ("wire", String::LAYOUT),
        (
            "projections",
            &JsonLayout::map(
                Alloc::tree::<Family, Box<RawValue>>(),
                &[("*", String::LAYOUT)],
            ),
        ),
    ]);
}

#[cfg(feature = "filesystem")]
impl crate::filesystem::PreparationRecord for WireItem {
    fn decoded_owned_bytes(&self) -> crate::Result<Option<u64>> {
        let provenance = self.provenance();
        let mut projections = self.projections();
        // This inventory follows a fresh decode: an empty map has never owned
        // a root. Nonempty maps use the shared type-specific node bound.
        let nodes = if projections.len() == 0 {
            0
        } else {
            crate::core::ownership::tree_nodes::<(Family, Box<RawValue>)>(projections.len())?
        };
        let owned = crate::core::owned_sum([
            std::mem::size_of::<Self>() as u64,
            provenance.provider.capacity() as u64,
            provenance.model.capacity() as u64,
            provenance.endpoint.capacity() as u64,
            self.wire().get().len() as u64,
            nodes,
        ])?;
        projections
            .try_fold(owned, |owned, (_, wire)| {
                crate::core::owned_sum([owned, wire.get().len() as u64])
            })
            .map(Some)
    }
}

#[cfg(test)]
mod tests;
