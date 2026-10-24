//! Layouts of the standard and dependency types records are built from.
use super::{Alloc, HeapLayout, JsonLayout};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;

macro_rules! scalars {
    ($($ty:ty),* $(,)?) => {$(
        impl HeapLayout for $ty {
            const LAYOUT: &'static JsonLayout = &JsonLayout::Scalar;
        }
    )*};
}
scalars!(
    (),
    bool,
    char,
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
    String,
    Box<str>,
    std::path::PathBuf,
    std::num::NonZeroU8,
    std::num::NonZeroU16,
    std::num::NonZeroU32,
    std::num::NonZeroU64,
    std::num::NonZeroUsize,
    uuid::Uuid,
);
impl HeapLayout for str {
    const LAYOUT: &'static JsonLayout = &JsonLayout::Scalar;
}
/// A borrowed record is laid out as the record it borrows.
impl<T: HeapLayout + ?Sized> HeapLayout for &T {
    const LAYOUT: &'static JsonLayout = T::LAYOUT;
}
impl HeapLayout for std::borrow::Cow<'_, str> {
    const LAYOUT: &'static JsonLayout = &JsonLayout::Scalar;
}
impl<T: ?Sized> HeapLayout for std::marker::PhantomData<T> {
    const LAYOUT: &'static JsonLayout = &JsonLayout::Scalar;
}
/// `Duration` decodes from `{"secs", "nanos"}` or `[secs, nanos]`.
impl HeapLayout for std::time::Duration {
    const LAYOUT: &'static JsonLayout = &JsonLayout::Node {
        seq: Some(Alloc::INLINE),
        map: Some(Alloc::INLINE),
        tagged: false,
        fields: &[],
        elements: &[],
        flatten: &[],
    };
}

/// Arbitrary JSON: arrays of values and `BTreeMap` objects.
static VALUE: JsonLayout = JsonLayout::Node {
    seq: Some(Alloc::vec::<Value>()),
    map: Some(Alloc::tree::<String, Value>()),
    tagged: false,
    fields: &[("*", &VALUE)],
    elements: &[&VALUE],
    flatten: &[],
};
impl HeapLayout for Value {
    const LAYOUT: &'static JsonLayout = &VALUE;
}
impl HeapLayout for serde_json::Map<String, Value> {
    const LAYOUT: &'static JsonLayout = &VALUE;
}

impl<T: HeapLayout> HeapLayout for Option<T> {
    const LAYOUT: &'static JsonLayout = T::LAYOUT;
}
impl<T: HeapLayout> HeapLayout for Box<T> {
    const LAYOUT: &'static JsonLayout = &JsonLayout::Boxed {
        bytes: size_of::<T>() as u64,
        inner: T::LAYOUT,
    };
}
/// serde decodes a box and moves it into the shared allocation, whose two
/// reference counts precede the value.
impl<T: HeapLayout> HeapLayout for Arc<T> {
    const LAYOUT: &'static JsonLayout = &JsonLayout::Boxed {
        bytes: (2 * size_of::<T>() + 2 * size_of::<usize>()) as u64,
        inner: T::LAYOUT,
    };
}
/// serde decodes a `String` and copies it into the shared allocation after
/// its two reference counts, rounded up to their alignment. The `String` and
/// the copy are alive together while it is copied.
impl HeapLayout for Arc<str> {
    const LAYOUT: &'static JsonLayout = &JsonLayout::Boxed {
        bytes: (2 * size_of::<usize>() + align_of::<usize>() - 1) as u64,
        inner: &JsonLayout::Scaled {
            times: 2,
            inner: &JsonLayout::Scalar,
        },
    };
}
/// A boxed slice decodes as a vector and shrinks in place.
impl<T: HeapLayout> HeapLayout for Box<[T]> {
    const LAYOUT: &'static JsonLayout = <Vec<T> as HeapLayout>::LAYOUT;
}
impl<T: HeapLayout> HeapLayout for Vec<T> {
    const LAYOUT: &'static JsonLayout = &JsonLayout::Node {
        seq: Some(Alloc::vec::<T>()),
        map: None,
        tagged: false,
        fields: &[],
        elements: &[T::LAYOUT],
        flatten: &[],
    };
}
impl<T: HeapLayout> HeapLayout for VecDeque<T> {
    const LAYOUT: &'static JsonLayout = <Vec<T> as HeapLayout>::LAYOUT;
}
impl<T: HeapLayout> HeapLayout for BTreeSet<T> {
    const LAYOUT: &'static JsonLayout = &JsonLayout::Node {
        seq: Some(Alloc::tree::<T, ()>()),
        map: None,
        tagged: false,
        fields: &[],
        elements: &[T::LAYOUT],
        flatten: &[],
    };
}
/// JSON object keys decode through the key type from their string, and a
/// key is charged only its string: a key type whose decode keeps more (a
/// box, or a converted copy) has no layout here and fails to compile.
impl<K: HeapLayout, V: HeapLayout> HeapLayout for BTreeMap<K, V> {
    const LAYOUT: &'static JsonLayout = {
        assert!(
            keeps_only_its_string(K::LAYOUT),
            "a map key's decode keeps more than its string"
        );
        &JsonLayout::Node {
            seq: None,
            map: Some(Alloc::tree::<K, V>()),
            tagged: false,
            fields: &[("*", V::LAYOUT)],
            elements: &[],
            flatten: &[],
        }
    };
}
/// Whether a key decoded with `layout` allocates nothing beyond its string.
const fn keeps_only_its_string(layout: &JsonLayout) -> bool {
    match layout {
        JsonLayout::Scalar | JsonLayout::Node { .. } => true,
        JsonLayout::Boxed { .. } | JsonLayout::Scaled { .. } => false,
        JsonLayout::Any { of, .. } => {
            let mut index = 0;
            while index < of.len() {
                if !keeps_only_its_string(of[index]) {
                    return false;
                }
                index += 1;
            }
            true
        }
    }
}
impl<T: HeapLayout, const N: usize> HeapLayout for [T; N] {
    const LAYOUT: &'static JsonLayout = &JsonLayout::Node {
        seq: Some(Alloc::INLINE),
        map: None,
        tagged: false,
        fields: &[],
        elements: &[T::LAYOUT],
        flatten: &[],
    };
}
macro_rules! tuples {
    ($(($($name:ident),+)),* $(,)?) => {$(
        impl<$($name: HeapLayout),+> HeapLayout for ($($name,)+) {
            const LAYOUT: &'static JsonLayout = &JsonLayout::Node {
                seq: Some(Alloc::INLINE),
                map: None,
                tagged: false,
                fields: &[],
                elements: &[$($name::LAYOUT),+, &JsonLayout::Scalar],
                flatten: &[],
            };
        }
    )*};
}
// The trailing scalar keeps a one-tuple's element from standing for every
// position of a longer array, which the decode rejects.
tuples!(
    (A),
    (A, B),
    (A, B, C),
    (A, B, C, D),
    (A, B, C, D, E),
    (A, B, C, D, E, F),
);

/// Any JSON a `Copy` type decodes: it keeps no heap, and only transient
/// strings and `Content` buffers, charged at every position, while it decodes.
static COPY: JsonLayout = JsonLayout::Node {
    seq: Some(Alloc::INLINE),
    map: Some(Alloc::INLINE),
    tagged: true,
    fields: &[("*", &COPY)],
    elements: &[&COPY],
    flatten: &[],
};
macro_rules! copy_layouts {
    ($($ty:ty),* $(,)?) => {$(
        const _: () = {
            const fn copy<T: Copy>() {}
            copy::<$ty>();
        };
        impl HeapLayout for $ty {
            const LAYOUT: &'static JsonLayout = &COPY;
        }
    )*};
}
copy_layouts!(
    acyclic_fs::access::VolumeOperation,
    acyclic_fs::access::Action,
    acyclic_fs::access::Effect,
    acyclic_fs::access::PrincipalKind,
    acyclic_fs::access::Relation,
    acyclic_fs::access::GenerationRange,
    acyclic_fs::access::Generations,
    acyclic_fs::LazySnapshotRef,
    acyclic_fs::JoinCommitWitness,
    acyclic_fs::WorkCounters,
);
/// `OwnedBytes` decodes a byte vector and shares it under one header.
impl HeapLayout for acyclic_stream::OwnedBytes {
    const LAYOUT: &'static JsonLayout = &JsonLayout::Boxed {
        bytes: 4 * size_of::<usize>() as u64,
        inner: <Vec<u8> as HeapLayout>::LAYOUT,
    };
}

/// The durable timer record names its identity types.
impl<T: HeapLayout, O: HeapLayout> HeapLayout for acyclic_actors::timers::TimerRecord<T, O> {
    const LAYOUT: &'static JsonLayout = &JsonLayout::Node {
        seq: Some(Alloc::INLINE),
        map: Some(Alloc::INLINE),
        tagged: false,
        fields: &[
            ("task_id", T::LAYOUT),
            ("operation_id", O::LAYOUT),
            ("deadline_unix_ms", &JsonLayout::Scalar),
        ],
        elements: &[
            T::LAYOUT,
            O::LAYOUT,
            &JsonLayout::Scalar,
            &JsonLayout::Scalar,
        ],
        flatten: &[],
    };
}

/// Layout markers for fields decoded by a custom function.
pub mod markers {
    use super::{Alloc, HeapLayout, JsonLayout};
    use std::marker::PhantomData;

    /// A map decoded from a JSON array of `[key, value]` pairs, collected
    /// into a vector and sorted before the tree is built.
    pub struct Entries<K, V>(PhantomData<(K, V)>);
    impl<K: HeapLayout, V: HeapLayout> HeapLayout for Entries<K, V> {
        const LAYOUT: &'static JsonLayout = &JsonLayout::Node {
            seq: Some(Alloc::entries::<K, V>()),
            map: None,
            tagged: false,
            fields: &[],
            elements: &[<(K, V) as HeapLayout>::LAYOUT],
            flatten: &[],
        };
    }
    /// A value decoded as a `serde_json::Value`, converted to `T`, and
    /// encoded again to prove it canonical: the source value, the record and
    /// the re-encoded value are alive together.
    pub struct Canonical<T>(PhantomData<T>);
    impl<T: HeapLayout> HeapLayout for Canonical<T> {
        const LAYOUT: &'static JsonLayout = &JsonLayout::Scaled {
            times: 3,
            inner: &JsonLayout::Any {
                tagged: false,
                of: &[<serde_json::Value as HeapLayout>::LAYOUT, T::LAYOUT],
            },
        };
    }
    /// Bytes decoded from base64 text: the text and the bytes it decodes to,
    /// which are never longer, are alive together.
    pub struct Base64Bytes;
    impl HeapLayout for Base64Bytes {
        const LAYOUT: &'static JsonLayout = &JsonLayout::Scaled {
            times: 2,
            inner: &JsonLayout::Scalar,
        };
    }
    /// A set decoded from a JSON array into a `BTreeSet<T>`.
    pub struct Set<T>(PhantomData<T>);
    impl<T: HeapLayout> HeapLayout for Set<T> {
        const LAYOUT: &'static JsonLayout = <std::collections::BTreeSet<T> as HeapLayout>::LAYOUT;
    }
}

copy_layouts!(
    acyclic_fs::OperationId,
    acyclic_fs::GenerationId,
    acyclic_fs::VolumeId,
    acyclic_fs::MaterializationPhase,
    acyclic_fs::MaterializationPreparationIdentity,
    acyclic_fs::MaterializationPreparationProgress,
    acyclic_fs::demand::SourceReference,
);

/// A struct as the derive lays out one without flattened members: an object
/// of its fields, or an array of them followed by a length-bounding scalar.
macro_rules! record {
    ($($name:literal => $layout:expr),* $(,)?) => {
        JsonLayout::Node {
            seq: Some(Alloc::INLINE),
            map: Some(Alloc::INLINE),
            tagged: false,
            fields: &[$(($name, $layout)),*],
            elements: &[$($layout,)* &JsonLayout::Scalar],
            flatten: &[],
        }
    };
}

/// An externally tagged enum: an object of one member per variant.
macro_rules! variants {
    ($($name:literal => $layout:expr),* $(,)?) => {
        JsonLayout::Node {
            seq: None,
            map: Some(Alloc::INLINE),
            tagged: false,
            fields: &[$(($name, $layout)),*],
            elements: &[],
            flatten: &[],
        }
    };
}

/// The materializer's recovery journal and the records it is built from, as
/// their derived `Deserialize` decodes them. Skipped custody fields decode
/// nothing.
mod materializer {
    use super::{Alloc, HeapLayout, JsonLayout};
    use acyclic_fs::{
        GenerationId, MaterializationEdit, MaterializationIntent, MaterializationJournal,
        MaterializationPhase, MaterializationPlan, MaterializationPreimage,
        MaterializationPreparationIdentity, MaterializationPreparationProgress,
        MaterializationPreparationSelection, OperationId, VolumeId, WorkCounters,
    };

    const BYTES: &JsonLayout = <Vec<u8> as HeapLayout>::LAYOUT;
    const STRING: &JsonLayout = &JsonLayout::Scalar;
    const GENERATION: &JsonLayout = <GenerationId as HeapLayout>::LAYOUT;

    impl HeapLayout for MaterializationEdit {
        const LAYOUT: &'static JsonLayout = &variants!(
            "Install" => &record!("path" => STRING, "image" => BYTES),
            "Remove" => &record!("path" => STRING),
            "Rename" => &record!("from" => STRING, "to" => STRING),
            "SetMetadata" => &record!("path" => STRING, "image" => BYTES),
        );
    }

    impl HeapLayout for MaterializationPreparationSelection {
        const LAYOUT: &'static JsonLayout = &record!(
            "provider_identity" => BYTES,
            "volume" => <VolumeId as HeapLayout>::LAYOUT,
            "generation" => GENERATION,
        );
    }

    impl HeapLayout for MaterializationIntent {
        const LAYOUT: &'static JsonLayout = &variants!(
            "GenerationTransition" => &record!("from" => GENERATION, "to" => GENERATION),
            "NativePreparation" => &record!(
                "selection" => <Vec<MaterializationPreparationSelection> as HeapLayout>::LAYOUT,
                "original" => BYTES,
            ),
        );
    }

    impl HeapLayout for MaterializationPlan {
        const LAYOUT: &'static JsonLayout = &record!(
            "operation_id" => <OperationId as HeapLayout>::LAYOUT,
            "intent" => <MaterializationIntent as HeapLayout>::LAYOUT,
            "edits" => <Vec<MaterializationEdit> as HeapLayout>::LAYOUT,
        );
    }

    impl HeapLayout for MaterializationPreimage {
        const LAYOUT: &'static JsonLayout = &record!("image" => BYTES);
    }

    impl HeapLayout for MaterializationJournal {
        const LAYOUT: &'static JsonLayout = &record!(
            "version" => &JsonLayout::Scalar,
            "revision" => &JsonLayout::Scalar,
            "plan" => <MaterializationPlan as HeapLayout>::LAYOUT,
            "root_identity" => <Option<[u8; 16]> as HeapLayout>::LAYOUT,
            "preparation_identity" =>
                <Option<MaterializationPreparationIdentity> as HeapLayout>::LAYOUT,
            "preparation_progress" =>
                <Option<MaterializationPreparationProgress> as HeapLayout>::LAYOUT,
            "work" => <WorkCounters as HeapLayout>::LAYOUT,
            "preimages" => <Vec<MaterializationPreimage> as HeapLayout>::LAYOUT,
            "phase" => <MaterializationPhase as HeapLayout>::LAYOUT,
            "applied" => &JsonLayout::Scalar,
            "restored" => &JsonLayout::Scalar,
        );
    }
}

/// Filesystem access policies and capabilities, as their derived
/// `Deserialize` decodes them.
mod access {
    use super::{Alloc, HeapLayout, JsonLayout};
    use acyclic_fs::access::{
        Action, Capability, Conditions, Effect, GenerationRange, Generations, Grant, Policy,
        PrincipalKind, PrincipalPattern, Relation, ResourcePattern, Rule,
    };
    use std::collections::{BTreeMap, BTreeSet};

    const STRING: &JsonLayout = &JsonLayout::Scalar;
    const OPTIONAL_STRING: &JsonLayout = <Option<String> as HeapLayout>::LAYOUT;
    const ACTIONS: &JsonLayout = <BTreeSet<Action> as HeapLayout>::LAYOUT;
    const ATTRIBUTES: &JsonLayout = <BTreeMap<String, String> as HeapLayout>::LAYOUT;

    impl HeapLayout for ResourcePattern {
        const LAYOUT: &'static JsonLayout = &record!(
            "lineage_root" => OPTIONAL_STRING,
            "volume" => OPTIONAL_STRING,
            "generations" => <Generations as HeapLayout>::LAYOUT,
            "path" => STRING,
        );
    }
    impl HeapLayout for PrincipalPattern {
        const LAYOUT: &'static JsonLayout = &record!(
            "kind" => <Option<PrincipalKind> as HeapLayout>::LAYOUT,
            "id" => OPTIONAL_STRING,
            "group" => OPTIONAL_STRING,
            "attributes" => ATTRIBUTES,
        );
    }
    impl HeapLayout for Conditions {
        const LAYOUT: &'static JsonLayout = &record!(
            "expires_at" => &JsonLayout::Scalar,
            "generations" => <Option<GenerationRange> as HeapLayout>::LAYOUT,
            "relation" => <Option<Relation> as HeapLayout>::LAYOUT,
            "role" => OPTIONAL_STRING,
            "tag" => OPTIONAL_STRING,
            "approvals" => &JsonLayout::Scalar,
        );
    }
    impl HeapLayout for Rule {
        const LAYOUT: &'static JsonLayout = &record!(
            "id" => STRING,
            "effect" => <Effect as HeapLayout>::LAYOUT,
            "principal" => <PrincipalPattern as HeapLayout>::LAYOUT,
            "actions" => ACTIONS,
            "resource" => <ResourcePattern as HeapLayout>::LAYOUT,
            "conditions" => <Conditions as HeapLayout>::LAYOUT,
        );
    }
    impl HeapLayout for Policy {
        const LAYOUT: &'static JsonLayout = &record!(
            "schema_version" => &JsonLayout::Scalar,
            "version" => &JsonLayout::Scalar,
            "layers" => <Vec<Vec<Rule>> as HeapLayout>::LAYOUT,
        );
    }
    impl HeapLayout for Grant {
        const LAYOUT: &'static JsonLayout = &record!(
            "actions" => ACTIONS,
            "resource" => <ResourcePattern as HeapLayout>::LAYOUT,
        );
    }
    impl HeapLayout for Capability {
        const LAYOUT: &'static JsonLayout = &record!(
            "layers" => <Vec<Vec<Grant>> as HeapLayout>::LAYOUT,
            "expires_at" => &JsonLayout::Scalar,
        );
    }
}
