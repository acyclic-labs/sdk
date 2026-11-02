//! Closing one settled child without confusing archive delivery with release.
//!
//! The archive commit fences the selected status and its consumed inbox cut,
//! and preserves that cut. Only the parent's membership-removal commit writes
//! the release receipt. Both records live outside fork inheritance.

use super::{AgentId, AgentPath, Error};
use acyclic_stream::StreamPath;
use serde::{Deserialize, Serialize};

/// Original settled output retained when a child is closed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[cfg_attr(feature = "wasm", tsify(large_number_types_as_bigints))]
#[serde(deny_unknown_fields)]
pub struct Settlement {
    /// Original status record, before archive changed the phase.
    pub status: u64,
    /// Exclusive consumed inbox cut of that status.
    pub cursor: u64,
    /// Exclusive original conversation cut, never an archive-generated answer.
    pub conversation: u64,
}

/// Immutable evidence that the parent consumed a child's terminal report and
/// removed its membership. The original report and outcome remain at `inbox`
/// in the parent's inbox; the receipt does not copy them.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[cfg_attr(feature = "wasm", tsify(large_number_types_as_bigints))]
#[serde(deny_unknown_fields)]
pub struct ChildRelease {
    /// Original zero-based `ChildDone` record consumed by the parent.
    pub inbox: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Versioned<T> {
    version: u32,
    value: T,
}

pub(super) fn path(parent: &AgentPath, child: &AgentId) -> Result<StreamPath, Error> {
    address(parent.id_text(), child.as_str())
}

/// Borrow the last three fixed-width segments instead of copying ancestry.
pub(super) fn for_child(child: &AgentPath) -> Result<StreamPath, Error> {
    let mut segments = child.as_str().rsplit('/');
    let child = segments
        .next()
        .ok_or(Error::Path("child has no identity"))?;
    if segments.next() != Some("children") {
        return Err(Error::Conflict(
            "a root has no parent membership to release",
        ));
    }
    let parent = segments.next().ok_or(Error::Path("child has no parent"))?;
    address(parent, child)
}

fn address(parent: &str, child: &str) -> Result<StreamPath, Error> {
    StreamPath::new(format!("harness/releases/{parent}/{child}")).map_err(Error::Stream)
}

impl Settlement {
    pub(super) fn path(child: &AgentPath) -> Result<StreamPath, Error> {
        StreamPath::new(format!("harness/settlements/{}", child.id_text())).map_err(Error::Stream)
    }

    pub(super) fn encode(&self) -> Result<Vec<u8>, Error> {
        encode(self)
    }
}

impl ChildRelease {
    pub(super) fn encode(&self) -> Result<Vec<u8>, Error> {
        encode(self)
    }

    pub(super) fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let receipt: Versioned<Self> = serde_json::from_slice(bytes)
            .map_err(|_| Error::Record("child release is malformed"))?;
        if receipt.version != 1 {
            return Err(Error::Record("unsupported child release version"));
        }
        Ok(receipt.value)
    }
}

fn encode(value: &impl Serialize) -> Result<Vec<u8>, Error> {
    serde_json::to_vec(&Versioned { version: 1, value })
        .map_err(|_| Error::Record("settlement receipt does not encode"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn release_encoding_rejects_changed_version_or_copied_outcome() {
        let receipt = ChildRelease { inbox: 5 };
        let encoded = receipt.encode().expect("receipt");
        assert_eq!(ChildRelease::decode(&encoded).expect("decode"), receipt);
        let original: serde_json::Value = serde_json::from_slice(&encoded).expect("JSON");
        for (key, value) in [
            ("version", serde_json::json!(2)),
            (
                "value",
                serde_json::json!({"inbox": 5, "outcome": "archived"}),
            ),
        ] {
            let mut changed = original.clone();
            changed[key] = value;
            assert!(ChildRelease::decode(&serde_json::to_vec(&changed).expect("JSON")).is_err());
        }
    }

    #[test]
    fn release_publication_work_does_not_copy_lineage() {
        let child = AgentId::from_bytes([3; 16]);
        let mut baseline = None;
        let mut copied = 0;
        for depth in [0, 16, 63] {
            let mut parent = AgentPath::root(&AgentId::from_bytes([1; 16]));
            for _ in 0..depth {
                parent = parent.child(&AgentId::from_bytes([2; 16])).expect("parent");
            }
            let negative = acyclic_test_support::measure(|| {
                std::hint::black_box(parent.as_str().to_owned());
            });
            assert!(negative.bytes_total > copied);
            copied = negative.bytes_total;
            let work = acyclic_test_support::measure(|| {
                std::hint::black_box(path(&parent, &child).expect("path"));
                std::hint::black_box(ChildRelease { inbox: 5 }.encode().expect("receipt"));
            });
            let current = (work.count_total, work.bytes_total);
            let descendant = parent.child(&child).expect("child");
            assert_eq!(
                path(&parent, &child).expect("path"),
                for_child(&descendant).expect("path")
            );
            let observation = acyclic_test_support::measure(|| {
                std::hint::black_box(for_child(&descendant).expect("path"));
                std::hint::black_box(ChildRelease { inbox: 5 }.encode().expect("receipt"));
            });
            assert_eq!((observation.count_total, observation.bytes_total), current);
            match baseline {
                None => baseline = Some(current),
                Some(baseline) => assert_eq!(current, baseline),
            }
        }
    }
}
