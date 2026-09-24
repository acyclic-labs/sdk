//! Conversions between SDK Machines types and Daytona JSON.
//!
//! The SDK contract for a machine cannot be reconstructed from Daytona's own fields, so the
//! provider stores the serialized [`MachineContract`] in a sandbox label at creation and reads
//! it back on every observation. Everything here is a pure function over wire structs.

use std::{collections::BTreeMap, time::Duration};

use acyclic_machines::{
    CheckpointId, CheckpointObservation, Endpoint, ExpirationPolicy, IdempotencyKey,
    MachineContract, MachineId, MachineObservation, MachineState, ProviderError, SuspensionPolicy,
};

use crate::{
    DaytonaConfig,
    api::{CreateSandboxRequest, Sandbox, Snapshot},
};

/// Label marking sandboxes owned by this provider.
pub const LABEL_MANAGED: &str = "acyclic.managed";
/// Label holding the idempotency key of the mutation that created the sandbox.
pub const LABEL_KEY: &str = "acyclic.key";
/// Label holding the mutation kind (`create` or `fork`) behind [`LABEL_KEY`].
pub const LABEL_KIND: &str = "acyclic.kind";
/// Label holding the child index within a fork.
pub const LABEL_INDEX: &str = "acyclic.index";
/// Label holding the tenant the sandbox was created for.
pub const LABEL_TENANT: &str = "acyclic.tenant";
/// Label holding the serialized [`MachineContract`].
pub const LABEL_CONTRACT: &str = "acyclic.contract";
/// Value of [`LABEL_KIND`] for `create`.
pub const KIND_CREATE: &str = "create";
/// Value of [`LABEL_KIND`] for `fork`.
pub const KIND_FORK: &str = "fork";
/// Name of the single stable endpoint exposed per machine.
pub const ENDPOINT_NAME: &str = "toolbox";
/// Sandbox classes with pause/resume, memory snapshots, and native fork.
pub const VM_CLASSES: [&str; 2] = ["linux-vm", "windows"];

/// Auto-delete value that disables automatic deletion.
const AUTO_DELETE_DISABLED: i64 = -1;
/// Idle-interval value that disables an automatic transition.
const INTERVAL_DISABLED: u32 = 0;

/// Maps a Daytona sandbox state string to the public lifecycle state.
///
/// `snapshotting`, `forking`, and `resizing` are brief excursions of a started sandbox that
/// return to `started`; they report as running but are not settled (see [`is_settled`]).
#[must_use]
pub fn machine_state(daytona: Option<&str>) -> MachineState {
    match daytona.map(str::to_ascii_lowercase).as_deref() {
        Some(
            "creating" | "restoring" | "starting" | "pulling_snapshot" | "pending_build"
            | "building_snapshot",
        ) => MachineState::Starting,
        Some("resuming") => MachineState::Waking,
        Some("started" | "snapshotting" | "forking" | "resizing") => MachineState::Running,
        Some("stopping" | "pausing" | "archiving") => MachineState::Suspending,
        Some("stopped" | "paused" | "archived") => MachineState::Suspended,
        Some("destroying") => MachineState::Destroying,
        Some("destroyed") => MachineState::Destroyed,
        Some("error" | "build_failed") => MachineState::Failed,
        _ => MachineState::Indeterminate,
    }
}

/// Whether a Daytona sandbox state string is a settled state a wait loop can stop on.
#[must_use]
pub fn is_settled(daytona: Option<&str>) -> bool {
    let transient = matches!(
        daytona.map(str::to_ascii_lowercase).as_deref(),
        Some("snapshotting" | "forking" | "resizing")
    );
    !transient
        && !matches!(
            machine_state(daytona),
            MachineState::Starting
                | MachineState::Waking
                | MachineState::Suspending
                | MachineState::Destroying
        )
}

/// Whether a sandbox class supports pause, memory snapshots, and native fork.
#[must_use]
pub fn is_vm_class(class: Option<&str>) -> bool {
    class.is_some_and(|class| VM_CLASSES.iter().any(|vm| vm.eq_ignore_ascii_case(class)))
}

/// Parses `YYYY-MM-DDTHH:MM:SS[.fff][Z|+00:00]` into Unix milliseconds.
///
/// Returns `None` for anything else, including non-UTC offsets, which Daytona does not emit.
#[must_use]
pub fn parse_rfc3339_ms(value: &str) -> Option<u64> {
    let value = value.trim();
    let (date, rest) = value.split_once('T')?;
    let mut parts = date.split('-');
    let year: i64 = parts.next()?.parse().ok()?;
    let month: u32 = parts.next()?.parse().ok()?;
    let day: u32 = parts.next()?.parse().ok()?;
    if parts.next().is_some() || !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    let time = rest
        .strip_suffix('Z')
        .or_else(|| rest.strip_suffix("+00:00"))
        .or_else(|| rest.strip_suffix("-00:00"))?;
    let (clock, fraction) = match time.split_once('.') {
        Some((clock, fraction)) => (clock, Some(fraction)),
        None => (time, None),
    };
    let mut parts = clock.split(':');
    let hour: u64 = parts.next()?.parse().ok()?;
    let minute: u64 = parts.next()?.parse().ok()?;
    let second: u64 = parts.next()?.parse().ok()?;
    if parts.next().is_some() || hour > 23 || minute > 59 || second > 60 {
        return None;
    }
    let millis = match fraction {
        Some(fraction) if !fraction.is_empty() && fraction.chars().all(|c| c.is_ascii_digit()) => {
            let padded = format!("{fraction:0<3}");
            padded.get(..3)?.parse::<u64>().ok()?
        }
        Some(_) => return None,
        None => 0,
    };
    let days = days_from_civil(year, month, day);
    let seconds = days
        .checked_mul(86_400)?
        .checked_add(i64::try_from(hour * 3_600 + minute * 60 + second).ok()?)?;
    let seconds = u64::try_from(seconds).ok()?;
    seconds.checked_mul(1_000)?.checked_add(millis)
}

/// Days since 1970-01-01 for a proleptic Gregorian date (Howard Hinnant's algorithm).
fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let month_index = i64::from(if month > 2 { month - 3 } else { month + 9 });
    let day_of_year = (153 * month_index + 2) / 5 + i64::from(day) - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

/// Lowercase hex encoding of a byte slice.
#[must_use]
pub fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut out, byte| {
            // Writing to a String cannot fail.
            let _ = write!(out, "{byte:02x}");
            out
        })
}

fn whole_minutes(duration: Duration) -> u64 {
    duration.as_secs().div_ceil(60).max(1)
}

/// Whole idle minutes before Daytona pauses a VM sandbox; zero disables.
///
/// Suspension maps to Daytona *auto-pause*, never auto-stop: a pause keeps VM memory, a stop
/// discards it, and the public contract promises a suspended machine wakes where it left off.
#[must_use]
pub fn autopause_minutes(policy: SuspensionPolicy) -> u32 {
    match policy {
        SuspensionPolicy::Manual => INTERVAL_DISABLED,
        SuspensionPolicy::AfterIdle(idle) => u32::try_from(whole_minutes(idle)).unwrap_or(u32::MAX),
    }
}

/// Daytona lifetime fields `(autoDeleteInterval, ttlMinutes)` for an expiration policy.
///
/// # Errors
/// Returns [`ProviderError::Unsupported`] for a fixed-instant expiration, which Daytona's
/// creation-relative TTL cannot express exactly.
pub fn lifetime_fields(policy: ExpirationPolicy) -> Result<(i64, Option<u64>), ProviderError> {
    match policy {
        ExpirationPolicy::Never => Ok((AUTO_DELETE_DISABLED, None)),
        ExpirationPolicy::Idle(idle) => {
            Ok((i64::try_from(whole_minutes(idle)).unwrap_or(i64::MAX), None))
        }
        ExpirationPolicy::MaxAge(age) => Ok((AUTO_DELETE_DISABLED, Some(whole_minutes(age)))),
        ExpirationPolicy::AtUnixMs(_) => Err(ProviderError::Unsupported(
            "Daytona cannot expire a sandbox at a fixed instant; use MaxAge, Idle, or Never".into(),
        )),
    }
}

/// Reconstructs a suspension policy from Daytona's auto-pause minutes.
#[must_use]
pub fn suspension_from_autopause(minutes: Option<i64>) -> Option<SuspensionPolicy> {
    match minutes {
        Some(0) => Some(SuspensionPolicy::Manual),
        Some(value) if value > 0 => Some(SuspensionPolicy::AfterIdle(Duration::from_secs(
            u64::try_from(value).unwrap_or(u64::MAX).saturating_mul(60),
        ))),
        _ => None,
    }
}

/// Deterministic sandbox name for the sandbox a create under `key` makes, or for child `index`
/// of a fork under `key`. Names are organization-unique in Daytona, so a replay collides with
/// the first attempt instead of duplicating it.
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

/// Builds the labels attached to a sandbox created or forked under `key`.
#[must_use]
pub fn labels(
    key: IdempotencyKey,
    kind: &str,
    index: Option<u32>,
    tenant: Option<&str>,
    contract: &MachineContract,
) -> BTreeMap<String, String> {
    let mut labels = BTreeMap::new();
    labels.insert(LABEL_MANAGED.to_owned(), "true".to_owned());
    labels.insert(LABEL_KEY.to_owned(), key.to_string());
    labels.insert(LABEL_KIND.to_owned(), kind.to_owned());
    if let Some(index) = index {
        labels.insert(LABEL_INDEX.to_owned(), index.to_string());
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

/// Reads the contract stored by [`labels`], if present and well-formed.
#[must_use]
pub fn contract_from_labels(labels: &BTreeMap<String, String>) -> Option<MachineContract> {
    labels
        .get(LABEL_CONTRACT)
        .and_then(|encoded| serde_json::from_str(encoded).ok())
}

/// Label filter selecting every sandbox created or forked under `key`.
#[must_use]
pub fn key_filter(key: IdempotencyKey) -> BTreeMap<String, String> {
    BTreeMap::from([
        (LABEL_MANAGED.to_owned(), "true".to_owned()),
        (LABEL_KEY.to_owned(), key.to_string()),
    ])
}

/// Label filter selecting every sandbox this provider manages.
#[must_use]
pub fn managed_filter() -> BTreeMap<String, String> {
    BTreeMap::from([(LABEL_MANAGED.to_owned(), "true".to_owned())])
}

/// Daytona `domainAllowList` for the configured outbound domains; `None` leaves the
/// organization default in place.
#[must_use]
pub fn domain_allow_list(allow_domains: &[String]) -> Option<String> {
    (!allow_domains.is_empty()).then(|| allow_domains.join(","))
}

/// Builds the Daytona create body for a sandbox booted from `snapshot`.
///
/// `index` is `None` for a plain create and the child index for a checkpoint restore.
///
/// # Errors
/// Returns [`ProviderError::Unsupported`] when the expiration policy cannot be expressed.
pub fn create_request(
    config: &DaytonaConfig,
    snapshot: &str,
    key: IdempotencyKey,
    index: Option<u32>,
    contract: &MachineContract,
) -> Result<CreateSandboxRequest, ProviderError> {
    let (auto_delete_interval, ttl_minutes) = lifetime_fields(contract.expiration)?;
    let kind = if index.is_some() {
        KIND_FORK
    } else {
        KIND_CREATE
    };
    Ok(CreateSandboxRequest {
        name: Some(sandbox_name(key, index)),
        snapshot: snapshot.to_owned(),
        target: config.region.clone(),
        env: BTreeMap::new(),
        labels: labels(key, kind, index, config.tenant.as_deref(), contract),
        auto_stop_interval: Some(INTERVAL_DISABLED),
        auto_pause_interval: Some(autopause_minutes(contract.suspension)),
        auto_delete_interval: Some(auto_delete_interval),
        ttl_minutes,
        domain_allow_list: domain_allow_list(&config.allow_domains),
        network_block_all: None,
    })
}

/// Parses a Daytona sandbox id into a machine identity.
///
/// # Errors
/// Returns [`ProviderError::Rejected`] when Daytona hands back a non-UUID id.
pub fn machine_id(sandbox_id: &str) -> Result<MachineId, ProviderError> {
    MachineId::parse(sandbox_id).map_err(|_| {
        ProviderError::Rejected(format!("Daytona sandbox id is not a UUID: {sandbox_id}"))
    })
}

/// Parses a Daytona snapshot id into a checkpoint identity.
///
/// # Errors
/// Returns [`ProviderError::Rejected`] when Daytona hands back a non-UUID id.
pub fn checkpoint_id(snapshot_id: &str) -> Result<CheckpointId, ProviderError> {
    CheckpointId::parse(snapshot_id).map_err(|_| {
        ProviderError::Rejected(format!("Daytona snapshot id is not a UUID: {snapshot_id}"))
    })
}

/// The single stable endpoint of a sandbox: its toolbox under the proxy Daytona reports.
#[must_use]
pub fn endpoint(sandbox: &Sandbox) -> Option<Endpoint> {
    let proxy = sandbox.toolbox_proxy_url.as_deref()?.trim_end_matches('/');
    Some(Endpoint {
        name: ENDPOINT_NAME.to_owned(),
        uri: format!("{proxy}/{}", sandbox.id),
    })
}

/// Converts a sandbox into a machine observation.
///
/// The contract comes from the sandbox label, then `fallback`, in that order. Timestamps that
/// Daytona omits or that fail to parse fall back to `now_unix_ms`.
///
/// # Errors
/// Returns [`ProviderError::Rejected`] when the sandbox id is not a UUID or no contract is
/// available from either source.
pub fn sandbox_to_observation(
    sandbox: &Sandbox,
    fallback: Option<&MachineContract>,
    last_checkpoint: Option<CheckpointId>,
    now_unix_ms: u64,
) -> Result<MachineObservation, ProviderError> {
    let id = machine_id(&sandbox.id)?;
    let mut contract = contract_from_labels(&sandbox.labels)
        .or_else(|| fallback.cloned())
        .ok_or_else(|| {
            ProviderError::Rejected(format!(
                "sandbox {} carries no {LABEL_CONTRACT} label",
                sandbox.id
            ))
        })?;
    if let Some(policy) = suspension_from_autopause(sandbox.auto_pause_interval) {
        contract.suspension = policy;
    }
    let created_at_unix_ms = sandbox
        .created_at
        .as_deref()
        .and_then(parse_rfc3339_ms)
        .unwrap_or(now_unix_ms);
    let changed_at_unix_ms = sandbox
        .updated_at
        .as_deref()
        .and_then(parse_rfc3339_ms)
        .unwrap_or(created_at_unix_ms);
    Ok(MachineObservation {
        id,
        state: machine_state(sandbox.state.as_deref()),
        contract,
        endpoints: endpoint(sandbox).into_iter().collect(),
        last_checkpoint,
        created_at_unix_ms,
        changed_at_unix_ms,
    })
}

/// Converts a snapshot into a checkpoint observation.
///
/// `source` overrides Daytona's own `sourceSandboxId` when known from the registry.
///
/// # Errors
/// Returns [`ProviderError::Rejected`] when the snapshot or source id is not a UUID or the
/// source is unknown.
pub fn snapshot_to_checkpoint(
    snapshot: &Snapshot,
    source: Option<MachineId>,
    contract: MachineContract,
    forkable: bool,
    now_unix_ms: u64,
) -> Result<CheckpointObservation, ProviderError> {
    let id = checkpoint_id(&snapshot.id)?;
    let source = match (source, snapshot.source_sandbox_id.as_deref()) {
        (Some(source), _) => source,
        (None, Some(sandbox_id)) => machine_id(sandbox_id)?,
        (None, None) => {
            return Err(ProviderError::Rejected(format!(
                "snapshot {} has no known source sandbox",
                snapshot.id
            )));
        }
    };
    let active = snapshot
        .state
        .as_deref()
        .is_none_or(|state| state.eq_ignore_ascii_case("active"));
    Ok(CheckpointObservation {
        id,
        source,
        contract,
        forkable: forkable && active,
        created_at_unix_ms: snapshot
            .created_at
            .as_deref()
            .and_then(parse_rfc3339_ms)
            .unwrap_or(now_unix_ms),
    })
}

/// Whether a Daytona snapshot state string is settled (`active` or a failure).
#[must_use]
pub fn snapshot_settled(state: Option<&str>) -> bool {
    matches!(
        state.map(str::to_ascii_lowercase).as_deref(),
        Some("active" | "error" | "build_failed" | "inactive")
    )
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]
mod tests {
    use std::collections::BTreeSet;

    use acyclic_machines::{Budgets, Capability, CompatibilityPolicy, Image, Performance};

    use super::*;

    fn fixture(name: &str) -> String {
        std::fs::read_to_string(format!(
            "{}/tests/fixtures/{name}",
            env!("CARGO_MANIFEST_DIR")
        ))
        .unwrap()
    }

    fn key(suffix: u8) -> IdempotencyKey {
        IdempotencyKey::parse(&format!("00000000-0000-0000-0000-0000000000{suffix:02x}")).unwrap()
    }

    fn contract() -> MachineContract {
        MachineContract {
            image: Image::custom([7; 32]).unwrap(),
            capabilities: BTreeSet::from([
                Capability::LiveCheckpoint,
                Capability::LiveFork,
                Capability::SuspendResume,
            ]),
            compatibility: CompatibilityPolicy::BestEffort,
            compatibility_revision: [1; 32],
            performance: Performance::Elastic,
            suspension: SuspensionPolicy::AfterIdle(Duration::from_secs(15)),
            expiration: ExpirationPolicy::Never,
            network_policy_digest: [8; 32],
            budgets: Budgets::default(),
        }
    }

    #[test]
    fn started_sandbox_round_trips_to_a_running_observation() {
        let sandbox: Sandbox = serde_json::from_str(&fixture("sandbox_started.json")).unwrap();
        let observation = sandbox_to_observation(&sandbox, None, None, 0).unwrap();
        assert_eq!(
            observation.id.to_string(),
            "6f1d2c3b-4a5e-4f60-9b71-8c2d3e4f5a61"
        );
        assert_eq!(observation.state, MachineState::Running);
        assert_eq!(observation.contract.image, Image::custom([7; 32]).unwrap());
        assert_eq!(observation.contract.network_policy_digest, [8; 32]);
        // Daytona rounds the 15 s policy up to one minute; the readback reflects Daytona's truth.
        assert_eq!(
            observation.contract.suspension,
            SuspensionPolicy::AfterIdle(Duration::from_secs(60))
        );
        assert_eq!(observation.created_at_unix_ms, 1_789_121_730_250);
        assert_eq!(observation.changed_at_unix_ms, 1_789_121_762_000);
        assert_eq!(observation.endpoints.len(), 1);
        assert_eq!(
            observation.endpoints[0].uri,
            "https://proxy.app.daytona.io/toolbox/6f1d2c3b-4a5e-4f60-9b71-8c2d3e4f5a61"
        );
        assert_eq!(sandbox.sandbox_class.as_deref(), Some("linux-vm"));
        assert!(is_vm_class(sandbox.sandbox_class.as_deref()));
        assert!(!is_vm_class(Some("container")));
        // The unmodelled fields survive in `extra` rather than being dropped.
        assert_eq!(sandbox.extra.get("gpu"), Some(&serde_json::json!(0)));
        let reencoded: Sandbox =
            serde_json::from_str(&serde_json::to_string(&sandbox).unwrap()).unwrap();
        assert_eq!(reencoded, sandbox);
    }

    #[test]
    fn paused_sandbox_uses_fallback_contract_and_maps_to_suspended() {
        let sandbox: Sandbox = serde_json::from_str(&fixture("sandbox_paused.json")).unwrap();
        assert!(sandbox_to_observation(&sandbox, None, None, 0).is_err());
        let observation = sandbox_to_observation(&sandbox, Some(&contract()), None, 0).unwrap();
        assert_eq!(observation.state, MachineState::Suspended);
        assert_eq!(observation.contract.suspension, SuspensionPolicy::Manual);
    }

    #[test]
    fn labels_carry_the_contract_and_come_back_identical() {
        let labels = labels(key(1), KIND_FORK, Some(3), Some("org-demo"), &contract());
        assert_eq!(labels[LABEL_KEY], key(1).to_string());
        assert_eq!(labels[LABEL_KIND], KIND_FORK);
        assert_eq!(labels[LABEL_INDEX], "3");
        assert_eq!(labels[LABEL_TENANT], "org-demo");
        assert_eq!(contract_from_labels(&labels).unwrap(), contract());
    }

    #[test]
    fn fork_children_drop_the_parent_provider_labels() {
        let inherited = BTreeMap::from([
            ("team".to_owned(), "a".to_owned()),
            (LABEL_KEY.to_owned(), key(1).to_string()),
            (LABEL_KIND.to_owned(), KIND_CREATE.to_owned()),
        ]);
        let child = relabel(
            &inherited,
            labels(key(2), KIND_FORK, Some(0), None, &contract()),
        );
        assert_eq!(child["team"], "a");
        assert_eq!(child[LABEL_KEY], key(2).to_string());
        assert_eq!(child[LABEL_KIND], KIND_FORK);
        assert_eq!(child[LABEL_INDEX], "0");
    }

    #[test]
    fn create_request_serializes_to_the_specified_shape() {
        let mut config = DaytonaConfig::new("k");
        config.allow_domains = vec!["api.anthropic.com".into(), "github.com".into()];
        config.region = Some("us".into());
        let body =
            create_request(&config, "acyclic-worker-base", key(1), None, &contract()).unwrap();
        let json = serde_json::to_value(&body).unwrap();
        assert_eq!(json["name"], "acyclic-00000000-0000-0000-0000-000000000001");
        assert_eq!(json["snapshot"], "acyclic-worker-base");
        assert!(
            json.get("class").is_none(),
            "the class comes from the snapshot"
        );
        assert_eq!(json["target"], "us");
        assert_eq!(json["autoStopInterval"], 0);
        assert_eq!(json["autoPauseInterval"], 1);
        assert_eq!(json["autoDeleteInterval"], -1);
        assert!(json.get("ttlMinutes").is_none());
        assert_eq!(json["domainAllowList"], "api.anthropic.com,github.com");
        assert!(
            json.get("networkAllowList").is_none(),
            "that field takes CIDRs, not domains"
        );
        assert!(
            json.get("networkBlockAll").is_none(),
            "block-all excludes an allow list"
        );
        assert_eq!(json["labels"][LABEL_KIND], KIND_CREATE);
        let restore = create_request(&config, "ckpt", key(2), Some(4), &contract()).unwrap();
        assert_eq!(
            restore.name.as_deref(),
            Some("acyclic-00000000-0000-0000-0000-000000000002-4")
        );
        assert_eq!(restore.labels[LABEL_KIND], KIND_FORK);
        let mut aged = contract();
        aged.expiration = ExpirationPolicy::MaxAge(Duration::from_secs(90));
        let body = create_request(&config, "s", key(1), None, &aged).unwrap();
        assert_eq!(body.ttl_minutes, Some(2));
        aged.expiration = ExpirationPolicy::AtUnixMs(1);
        assert!(matches!(
            create_request(&config, "s", key(1), None, &aged),
            Err(ProviderError::Unsupported(_))
        ));
    }

    #[test]
    fn snapshot_fixture_becomes_a_forkable_checkpoint() {
        let snapshot: Snapshot = serde_json::from_str(&fixture("snapshot_active.json")).unwrap();
        assert_eq!(snapshot.sandbox_class.as_deref(), Some("linux-vm"));
        let checkpoint = snapshot_to_checkpoint(&snapshot, None, contract(), true, 0).unwrap();
        assert_eq!(
            checkpoint.id.to_string(),
            "9c8b7a6f-5e4d-4c3b-8a29-18f7e6d5c4b3"
        );
        assert_eq!(
            checkpoint.source.to_string(),
            "6f1d2c3b-4a5e-4f60-9b71-8c2d3e4f5a61"
        );
        assert!(checkpoint.forkable);
        assert_eq!(checkpoint.created_at_unix_ms, 1_789_122_300_000);
        let destroyed = snapshot_to_checkpoint(&snapshot, None, contract(), false, 0).unwrap();
        assert!(!destroyed.forkable);
    }

    #[test]
    fn list_fixture_filters_and_states_map() {
        let value: serde_json::Value = serde_json::from_str(&fixture("sandbox_list.json")).unwrap();
        let items: Vec<Sandbox> = serde_json::from_value(value["items"].clone()).unwrap();
        let managed = items
            .iter()
            .filter(|s| s.labels.get(LABEL_MANAGED).is_some_and(|v| v == "true"))
            .count();
        assert_eq!(managed, 3);
        assert_eq!(
            machine_state(items[1].state.as_deref()),
            MachineState::Starting
        );
        assert_eq!(
            machine_state(items[3].state.as_deref()),
            MachineState::Suspended
        );
        assert_eq!(machine_state(Some("whatever")), MachineState::Indeterminate);
        assert_eq!(machine_state(Some("forking")), MachineState::Running);
        assert!(!is_settled(Some("creating")));
        assert!(!is_settled(Some("forking")));
        assert!(!is_settled(Some("snapshotting")));
        assert!(is_settled(Some("started")));
        assert!(is_settled(Some("paused")));
    }

    #[test]
    fn policies_map_to_daytona_minutes() {
        assert_eq!(autopause_minutes(SuspensionPolicy::Manual), 0);
        assert_eq!(
            autopause_minutes(SuspensionPolicy::AfterIdle(Duration::from_secs(1))),
            1
        );
        assert_eq!(
            autopause_minutes(SuspensionPolicy::AfterIdle(Duration::from_secs(121))),
            3
        );
        assert_eq!(
            lifetime_fields(ExpirationPolicy::Never).unwrap(),
            (-1, None)
        );
        assert_eq!(
            lifetime_fields(ExpirationPolicy::Idle(Duration::from_secs(3600))).unwrap(),
            (60, None)
        );
        assert_eq!(
            lifetime_fields(ExpirationPolicy::MaxAge(Duration::from_secs(3600))).unwrap(),
            (-1, Some(60))
        );
        assert!(lifetime_fields(ExpirationPolicy::AtUnixMs(1)).is_err());
    }

    #[test]
    fn rfc3339_parsing_matches_known_instants() {
        assert_eq!(parse_rfc3339_ms("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(
            parse_rfc3339_ms("2000-03-01T00:00:00Z"),
            Some(951_868_800_000)
        );
        assert_eq!(
            parse_rfc3339_ms("2026-09-11T10:15:30.250Z"),
            Some(1_789_121_730_250)
        );
        assert_eq!(
            parse_rfc3339_ms("2026-09-11T10:15:30.2Z"),
            Some(1_789_121_730_200)
        );
        assert_eq!(
            parse_rfc3339_ms("2026-09-11T10:15:30+00:00"),
            Some(1_789_121_730_000)
        );
        assert_eq!(parse_rfc3339_ms("2026-09-11T10:15:30+02:00"), None);
        assert_eq!(parse_rfc3339_ms("garbage"), None);
    }
}
