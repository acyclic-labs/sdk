//! Layouts of the renderer's immutable wire items.
use crate::{Alloc, HeapLayout, JsonLayout};
use acyclic_model_render::{Effort, Family, ItemKind, Notice, Provenance, WireItem};
use serde_json::value::RawValue;

impl HeapLayout for Effort {
    const LAYOUT: &'static JsonLayout = &JsonLayout::Scalar;
}

impl HeapLayout for Family {
    const LAYOUT: &'static JsonLayout = &JsonLayout::Scalar;
}
impl HeapLayout for ItemKind {
    const LAYOUT: &'static JsonLayout = &JsonLayout::Scalar;
}
impl HeapLayout for Notice {
    const LAYOUT: &'static JsonLayout = &JsonLayout::record(&[
        ("source", String::LAYOUT),
        ("kind", String::LAYOUT),
        ("body", serde_json::Value::LAYOUT),
        ("wake", bool::LAYOUT),
        ("coalesce_key", <Option<String>>::LAYOUT),
    ]);
}
impl HeapLayout for Provenance {
    const LAYOUT: &'static JsonLayout = &JsonLayout::record(&[
        ("family", Family::LAYOUT),
        ("provider", String::LAYOUT),
        ("model", String::LAYOUT),
        ("endpoint", String::LAYOUT),
        ("credential_scope", <Option<[u8; 32]>>::LAYOUT),
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
