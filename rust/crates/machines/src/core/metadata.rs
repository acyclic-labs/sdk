//! Portable provider metadata projection; canonical Streams remains authoritative.
//! These fields label native resources for inspection and collision checks. They
//! never establish an operation settlement or authorize adoption by themselves.
use super::ForkSlot;
use crate::{IdempotencyKey, MachineContract};
use std::collections::BTreeMap;

/// Label marking sandboxes owned by this provider.
pub const LABEL_MANAGED: &str = "acyclic.managed";
/// Label holding the idempotency key of the mutation that created the sandbox.
pub const LABEL_KEY: &str = "acyclic.key";
/// Label holding the mutation kind (`create` or `fork`) behind [`LABEL_KEY`].
pub const LABEL_KIND: &str = "acyclic.kind";
/// Label holding the child index within a fork.
pub const LABEL_INDEX: &str = "acyclic.index";
/// Label holding the number of children in the admitted native request.
pub const LABEL_COUNT: &str = "acyclic.count";
/// Label holding the tenant the sandbox was created for.
pub const LABEL_TENANT: &str = "acyclic.tenant";
/// Label holding the serialized [`MachineContract`].
pub const LABEL_CONTRACT: &str = "acyclic.contract";
/// Value of [`LABEL_KIND`] for `create`.
pub const KIND_CREATE: &str = "create";
/// Value of [`LABEL_KIND`] for `fork`.
pub const KIND_FORK: &str = "fork";
/// Value of [`LABEL_KIND`] for `fork_machine` (live fork of a running machine).
pub const KIND_LIVE_FORK: &str = "live-fork";
/// Native live-fork metadata marker. It is not an operation settlement.
pub const LABEL_READY: &str = "acyclic.ready";
/// Label holding the sandbox a live-fork child was actually forked from: the requested machine
/// or, in a doubling fan-out, an earlier child of the same fork.
pub const LABEL_FORK_SOURCE: &str = "acyclic.fork_source";
/// Label holding the source machine of a live-fork child.
pub const LABEL_PARENT: &str = "acyclic.parent";
/// Deterministic sandbox name for the sandbox a create under `key` makes, or for child `index`
/// of a fork under `key`. Native name collisions remain transport refusals;
/// only the canonical operation record can establish replay or adoption.
#[must_use]
pub fn sandbox_name(key: IdempotencyKey, index: Option<u32>) -> String {
    match index {
        None => format!("acyclic-{key}"),
        Some(index) => format!("acyclic-{key}-{index}"),
    }
}

/// Deterministic name of the memory snapshot a checkpoint under `key` takes.
#[must_use]
pub fn checkpoint_name(key: IdempotencyKey) -> String {
    format!("acyclic-ckpt-{key}")
}

/// Builds the labels attached to a sandbox created (`slot` `None`) or forked under `key`.
#[must_use]
pub fn labels(
    key: IdempotencyKey,
    slot: Option<ForkSlot>,
    tenant: Option<&str>,
    contract: &MachineContract,
) -> BTreeMap<String, String> {
    let mut labels = BTreeMap::new();
    labels.insert(LABEL_MANAGED.to_owned(), "true".to_owned());
    labels.insert(LABEL_KEY.to_owned(), key.to_string());
    let kind = if slot.is_some() {
        KIND_FORK
    } else {
        KIND_CREATE
    };
    labels.insert(LABEL_KIND.to_owned(), kind.to_owned());
    if let Some(slot) = slot {
        labels.insert(LABEL_INDEX.to_owned(), slot.index.to_string());
        labels.insert(LABEL_COUNT.to_owned(), slot.count.to_string());
    }
    if let Some(tenant) = tenant {
        labels.insert(LABEL_TENANT.to_owned(), tenant.to_owned());
    }
    if let Ok(encoded) = serde_json::to_string(contract) {
        labels.insert(LABEL_CONTRACT.to_owned(), encoded);
    }
    labels
}

/// Labels for a native fork child: whatever it inherited from its parent minus the parent's
/// provider labels, plus `ours`.
#[must_use]
pub fn relabel(
    inherited: &BTreeMap<String, String>,
    ours: BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    let mut labels: BTreeMap<String, String> = inherited
        .iter()
        .filter(|(name, _)| !name.starts_with("acyclic."))
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect();
    labels.extend(ours);
    labels
}

/// Whether a sandbox is managed by this provider *for `tenant`*: it carries the managed label
/// and exactly the configured tenant label (none when no tenant is configured). Sandboxes of
/// other tenants in a shared organization are invisible to this provider.
#[must_use]
pub fn owned_by(labels: &BTreeMap<String, String>, tenant: Option<&str>) -> bool {
    labels
        .get(LABEL_MANAGED)
        .is_some_and(|value| value == "true")
        && labels.get(LABEL_TENANT).map(String::as_str) == tenant
}

/// Reads the contract stored by [`labels`], if present and well-formed.
#[must_use]
pub fn contract_from_labels(labels: &BTreeMap<String, String>) -> Option<MachineContract> {
    labels
        .get(LABEL_CONTRACT)
        .and_then(|encoded| serde_json::from_str(encoded).ok())
}

/// Label filter selecting every sandbox this provider manages for `tenant`.
///
/// Native APIs may not filter absence; callers still check [`owned_by`].
#[must_use]
pub fn managed_filter(tenant: Option<&str>) -> BTreeMap<String, String> {
    let mut filter = BTreeMap::from([(LABEL_MANAGED.to_owned(), "true".to_owned())]);
    if let Some(tenant) = tenant {
        filter.insert(LABEL_TENANT.to_owned(), tenant.to_owned());
    }
    filter
}
