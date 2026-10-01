//! Stable, natural JavaScript DTOs for the Machines provider.
//!
//! The simulator and all validation remain in `acyclic-machines`.  This module
//! only translates the checked Rust domain into the public camelCase DTOs used
//! by the TypeScript package.

#![allow(
    clippy::needless_pass_by_value,
    reason = "DTO conversion functions consume provider results"
)]

use acyclic_machines::{
    Budgets, Capability, CheckpointId, CheckpointObservation, CompatibilityPolicy, CreateMachine,
    Endpoint, EventFact, EventPage, ExpirationPolicy, ForkFidelity, Image, ImageQualification,
    MachineContract, MachineEvent, MachineId, MachineObservation, MachinePage, MachineState,
    MachinesProvider, MutationOutcome, OperationId, OperationObservation, OperationPhase, Pressure,
    ProviderError, SuspensionPolicy, UsageReceipt,
};
use hex::{decode, encode};
use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{self, Visitor},
};
use std::{collections::BTreeSet, num::NonZeroU32};
use tsify_next::Tsify;
use wasm_bindgen::prelude::*;

const MAX_SAFE: u64 = 9_007_199_254_740_991;

/// A public JavaScript number accepted only when it is an exact safe integer.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct SafeInput(u64);
impl<'de> Deserialize<'de> for SafeInput {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct SafeVisitor;
        impl<'de> Visitor<'de> for SafeVisitor {
            type Value = SafeInput;
            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a non-negative JavaScript safe integer")
            }
            fn visit_u64<E: de::Error>(self, value: u64) -> Result<Self::Value, E> {
                if value <= MAX_SAFE {
                    Ok(SafeInput(value))
                } else {
                    Err(E::custom("number exceeds JavaScript safe integer range"))
                }
            }
            #[allow(
                clippy::cast_sign_loss,
                reason = "the non-negative check precedes this conversion"
            )]
            fn visit_i64<E: de::Error>(self, value: i64) -> Result<Self::Value, E> {
                if value >= 0 {
                    self.visit_u64(value as u64)
                } else {
                    Err(E::custom("number must be non-negative"))
                }
            }
            #[allow(
                clippy::cast_precision_loss,
                clippy::cast_possible_truncation,
                clippy::cast_sign_loss,
                reason = "finite integral values bounded by MAX_SAFE convert exactly"
            )]
            fn visit_f64<E: de::Error>(self, value: f64) -> Result<Self::Value, E> {
                if value.is_finite()
                    && value >= 0.0
                    && value.fract() == 0.0
                    && value <= MAX_SAFE as f64
                {
                    Ok(SafeInput(value as u64))
                } else {
                    Err(E::custom("number must be an exact JavaScript safe integer"))
                }
            }
        }
        deserializer.deserialize_any(SafeVisitor)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct SafeNumber(u64);
impl Serialize for SafeNumber {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if self.0 > MAX_SAFE {
            return Err(serde::ser::Error::custom(
                "value exceeds JavaScript safe integer range",
            ));
        }
        #[allow(
            clippy::cast_precision_loss,
            reason = "the safe-integer check above proves this conversion is exact"
        )]
        {
            serializer.serialize_f64(self.0 as f64)
        }
    }
}

fn err(message: impl Into<String>) -> JsValue {
    JsValue::from_str(&message.into())
}
fn invalid(message: impl Into<String>) -> Result<JsValue, JsValue> {
    Err(err(message))
}

fn from_js<T: for<'de> Deserialize<'de>>(value: JsValue) -> Result<T, JsValue> {
    serde_wasm_bindgen::from_value(value).map_err(|e| err(e.to_string()))
}

fn reject_retired_mode(value: &JsValue) -> Result<(), JsValue> {
    // serde-wasm-bindgen visits known struct fields only, even with
    // deny_unknown_fields. Enforce the reserved field at the JS boundary.
    if js_sys::Reflect::has(value, &JsValue::from_str("performance"))? {
        return Err(err("unknown field performance"));
    }
    Ok(())
}
fn to_js<T: Serialize>(value: &T) -> Result<JsValue, JsValue> {
    let serializer = serde_wasm_bindgen::Serializer::new()
        .serialize_large_number_types_as_bigints(true)
        .serialize_missing_as_null(true);
    value.serialize(&serializer).map_err(|e| err(e.to_string()))
}
fn provider_error(error: ProviderError) -> JsValue {
    match error {
        ProviderError::NotFound(message) => err(format!("resource not found: {message}")),
        other => err(other.to_string()),
    }
}
fn id(kind: &str, value: String) -> Result<String, JsValue> {
    super::normalize_identity(kind.to_owned(), value)
}
fn parse_id<T>(
    kind: &str,
    value: String,
    parse: impl FnOnce(&str) -> Result<T, acyclic_machines::IdentityError>,
) -> Result<T, JsValue> {
    let value = id(kind, value)?;
    parse(&value).map_err(|e| err(e.to_string()))
}
fn key(value: String) -> Result<acyclic_machines::IdempotencyKey, JsValue> {
    parse_id(
        "idempotency",
        value,
        acyclic_machines::IdempotencyKey::parse,
    )
}
fn machine(value: String) -> Result<MachineId, JsValue> {
    parse_id("machine", value, MachineId::parse)
}
fn checkpoint(value: String) -> Result<CheckpointId, JsValue> {
    parse_id("checkpoint", value, CheckpointId::parse)
}
fn operation(value: String) -> Result<OperationId, JsValue> {
    parse_id("operation", value, OperationId::parse)
}

fn digest(value: &str, field: &str) -> Result<[u8; 32], JsValue> {
    let bytes = decode(value)
        .map_err(|_| err(format!("{field} must be a 64-character hexadecimal digest")))?;
    let bytes: [u8; 32] = bytes
        .try_into()
        .map_err(|_| err(format!("{field} must be a 64-character hexadecimal digest")))?;
    if bytes == [0; 32] {
        return Err(err(format!("{field} cannot be zero")));
    }
    Ok(bytes)
}
fn digest_out(value: [u8; 32]) -> String {
    encode(value)
}

#[derive(Deserialize, Serialize, Tsify)]
#[tsify(from_wasm_abi)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum ImageIn {
    ManagedOci {
        #[serde(rename = "digestHex")]
        digest_hex: String,
    },
    Custom {
        #[serde(rename = "digestHex")]
        digest_hex: String,
    },
    Checkpoint {
        #[serde(rename = "checkpointId")]
        checkpoint_id: String,
    },
}
fn image_in(value: ImageIn) -> Result<Image, JsValue> {
    match value {
        ImageIn::ManagedOci { digest_hex } => {
            Image::managed(digest(&digest_hex, "digestHex")?).map_err(provider_error)
        }
        ImageIn::Custom { digest_hex } => {
            Image::custom(digest(&digest_hex, "digestHex")?).map_err(provider_error)
        }
        ImageIn::Checkpoint { checkpoint_id } => Ok(Image::Checkpoint(checkpoint(checkpoint_id)?)),
    }
}

pub(crate) fn managed_oci(reference: String) -> Result<JsValue, JsValue> {
    let image = Image::oci(reference).map_err(provider_error)?;
    to_js(&image_out(&image))
}

#[derive(Serialize, Tsify)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum ImageOut {
    ManagedOci {
        #[serde(rename = "digestHex")]
        digest_hex: String,
    },
    Custom {
        #[serde(rename = "digestHex")]
        digest_hex: String,
    },
    Checkpoint {
        #[serde(rename = "checkpointId")]
        checkpoint_id: String,
    },
}
fn image_out(value: &Image) -> ImageOut {
    match value {
        Image::ManagedOci(d) => ImageOut::ManagedOci {
            digest_hex: digest_out(d.as_bytes()),
        },
        Image::Custom(d) => ImageOut::Custom {
            digest_hex: digest_out(d.as_bytes()),
        },
        Image::Checkpoint(id) => ImageOut::Checkpoint {
            checkpoint_id: id.to_string(),
        },
    }
}

#[derive(Deserialize, Serialize, Tsify)]
#[tsify(from_wasm_abi)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum CompatibilityIn {
    BestEffort {},
    Require {
        #[tsify(type = "readonly CapabilityIn[]")]
        capabilities: Vec<CapabilityIn>,
    },
}
#[derive(Clone, Copy, Debug, Deserialize, Serialize, Tsify)]
#[serde(rename_all = "kebab-case")]
pub enum CapabilityIn {
    ElasticCpu,
    ElasticMemory,
    LiveCheckpoint,
    LiveFork,
    SuspendResume,
    LiveMovement,
    DiskFork,
}
fn capability(value: CapabilityIn) -> Capability {
    match value {
        CapabilityIn::ElasticCpu => Capability::ElasticCpu,
        CapabilityIn::ElasticMemory => Capability::ElasticMemory,
        CapabilityIn::LiveCheckpoint => Capability::LiveCheckpoint,
        CapabilityIn::LiveFork => Capability::LiveFork,
        CapabilityIn::SuspendResume => Capability::SuspendResume,
        CapabilityIn::LiveMovement => Capability::LiveMovement,
        CapabilityIn::DiskFork => Capability::DiskFork,
    }
}
fn compatibility_in(value: CompatibilityIn) -> Result<CompatibilityPolicy, JsValue> {
    match value {
        CompatibilityIn::BestEffort {} => Ok(CompatibilityPolicy::BestEffort),
        CompatibilityIn::Require { capabilities } => {
            let values = capabilities
                .iter()
                .map(|&v| capability(v))
                .collect::<BTreeSet<_>>();
            if values.len() != capabilities.len() {
                return Err(err("capability set contains duplicates"));
            }
            if values.is_empty() {
                return Err(err("require compatibility needs capabilities"));
            }
            Ok(CompatibilityPolicy::Require(values))
        }
    }
}
fn capability_out(value: &Capability) -> CapabilityIn {
    match value {
        Capability::ElasticCpu => CapabilityIn::ElasticCpu,
        Capability::ElasticMemory => CapabilityIn::ElasticMemory,
        Capability::LiveCheckpoint => CapabilityIn::LiveCheckpoint,
        Capability::LiveFork => CapabilityIn::LiveFork,
        Capability::SuspendResume => CapabilityIn::SuspendResume,
        Capability::LiveMovement => CapabilityIn::LiveMovement,
        Capability::DiskFork => CapabilityIn::DiskFork,
    }
}
#[derive(Serialize, Tsify)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum CompatibilityOut {
    BestEffort,
    Require {
        #[tsify(type = "readonly CapabilityIn[]")]
        capabilities: Vec<CapabilityIn>,
    },
}
fn compatibility_out(value: &CompatibilityPolicy) -> CompatibilityOut {
    match value {
        CompatibilityPolicy::BestEffort => CompatibilityOut::BestEffort,
        CompatibilityPolicy::Require(values) => CompatibilityOut::Require {
            capabilities: values.iter().map(capability_out).collect(),
        },
    }
}

#[derive(Deserialize, Serialize, Tsify)]
#[tsify(from_wasm_abi)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum SuspensionIn {
    Manual {},
    AfterIdle {
        #[tsify(type = "number")]
        milliseconds: SafeInput,
    },
}
fn suspension_in(value: SuspensionIn) -> Result<SuspensionPolicy, JsValue> {
    match value {
        SuspensionIn::Manual {} => Ok(SuspensionPolicy::Manual),
        SuspensionIn::AfterIdle { milliseconds } => {
            let milliseconds = milliseconds.0;
            if milliseconds == 0 {
                return Err(err("idle duration must be nonzero"));
            }
            Ok(SuspensionPolicy::AfterIdle(
                std::time::Duration::from_millis(milliseconds),
            ))
        }
    }
}
#[derive(Deserialize, Serialize, Tsify)]
#[tsify(from_wasm_abi)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum ExpirationIn {
    Never {},
    MaxAge {
        #[tsify(type = "number")]
        milliseconds: SafeInput,
    },
    At {
        #[tsify(type = "number")]
        milliseconds: SafeInput,
    },
    Idle {
        #[tsify(type = "number")]
        milliseconds: SafeInput,
    },
}
fn expiration_in(value: ExpirationIn) -> Result<ExpirationPolicy, JsValue> {
    match value {
        ExpirationIn::Never {} => Ok(ExpirationPolicy::Never),
        ExpirationIn::MaxAge { milliseconds } => {
            let milliseconds = milliseconds.0;
            if milliseconds == 0 {
                return Err(err("expiration duration must be nonzero"));
            }
            Ok(ExpirationPolicy::MaxAge(std::time::Duration::from_millis(
                milliseconds,
            )))
        }
        ExpirationIn::At { milliseconds } => {
            let milliseconds = milliseconds.0;
            if milliseconds == 0 {
                return Err(err("expiration duration must be nonzero"));
            }
            Ok(ExpirationPolicy::AtUnixMs(milliseconds))
        }
        ExpirationIn::Idle { milliseconds } => {
            let milliseconds = milliseconds.0;
            if milliseconds == 0 {
                return Err(err("expiration duration must be nonzero"));
            }
            Ok(ExpirationPolicy::Idle(std::time::Duration::from_millis(
                milliseconds,
            )))
        }
    }
}
#[derive(Serialize, Tsify)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum TimedOut {
    Manual,
    AfterIdle {
        #[tsify(type = "number")]
        milliseconds: SafeNumber,
    },
    Never,
    MaxAge {
        #[tsify(type = "number")]
        milliseconds: SafeNumber,
    },
    At {
        #[tsify(type = "number")]
        milliseconds: SafeNumber,
    },
    Idle {
        #[tsify(type = "number")]
        milliseconds: SafeNumber,
    },
}
fn millis(value: std::time::Duration) -> u64 {
    u64::try_from(value.as_millis()).unwrap_or(u64::MAX)
}
fn suspension_out(value: &SuspensionPolicy) -> TimedOut {
    match value {
        SuspensionPolicy::Manual => TimedOut::Manual,
        SuspensionPolicy::AfterIdle(v) => TimedOut::AfterIdle {
            milliseconds: SafeNumber(millis(*v)),
        },
    }
}
fn expiration_out(value: &ExpirationPolicy) -> TimedOut {
    match value {
        ExpirationPolicy::Never => TimedOut::Never,
        ExpirationPolicy::MaxAge(v) => TimedOut::MaxAge {
            milliseconds: SafeNumber(millis(*v)),
        },
        ExpirationPolicy::AtUnixMs(v) => TimedOut::At {
            milliseconds: SafeNumber(*v),
        },
        ExpirationPolicy::Idle(v) => TimedOut::Idle {
            milliseconds: SafeNumber(millis(*v)),
        },
    }
}
#[derive(Deserialize, Serialize, Tsify)]
#[tsify(from_wasm_abi)]
#[serde(rename_all = "camelCase")]
#[tsify(large_number_types_as_bigints)]
pub struct BudgetsIn {
    spend_micros: u64,
    concurrency: u32,
}
#[derive(Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
#[tsify(large_number_types_as_bigints)]
pub struct BudgetsOut {
    spend_micros: u64,
    concurrency: u32,
}
fn budgets_in(value: &BudgetsIn) -> Budgets {
    Budgets {
        spend_micros: value.spend_micros,
        concurrency: value.concurrency,
    }
}
fn budgets_out(value: Budgets) -> BudgetsOut {
    BudgetsOut {
        spend_micros: value.spend_micros,
        concurrency: value.concurrency,
    }
}
#[derive(Deserialize, Serialize, Tsify)]
#[tsify(from_wasm_abi)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CreateIn {
    idempotency_key: String,
    image: ImageIn,
    compatibility: CompatibilityIn,
    suspension: SuspensionIn,
    expiration: ExpirationIn,
    network_policy_digest_hex: String,
    budgets: BudgetsIn,
}
fn create_in(value: CreateIn) -> Result<CreateMachine, JsValue> {
    Ok(CreateMachine {
        idempotency_key: key(value.idempotency_key)?,
        image: image_in(value.image)?,
        compatibility: compatibility_in(value.compatibility)?,
        suspension: suspension_in(value.suspension)?,
        expiration: expiration_in(value.expiration)?,
        network_policy_digest: digest(&value.network_policy_digest_hex, "networkPolicyDigestHex")?,
        budgets: budgets_in(&value.budgets),
    })
}

#[derive(Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
pub struct ContractOut {
    image: ImageOut,
    #[tsify(type = "readonly CapabilityIn[]")]
    capabilities: Vec<CapabilityIn>,
    compatibility: CompatibilityOut,
    compatibility_revision_hex: String,
    suspension: TimedOut,
    expiration: TimedOut,
    network_policy_digest_hex: String,
    budgets: BudgetsOut,
}
fn contract_out(value: &MachineContract) -> ContractOut {
    ContractOut {
        image: image_out(&value.image),
        capabilities: value.capabilities.iter().map(capability_out).collect(),
        compatibility: compatibility_out(&value.compatibility),
        compatibility_revision_hex: digest_out(value.compatibility_revision),
        suspension: suspension_out(&value.suspension),
        expiration: expiration_out(&value.expiration),
        network_policy_digest_hex: digest_out(value.network_policy_digest),
        budgets: budgets_out(value.budgets),
    }
}
#[derive(Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
pub struct QualificationOut {
    image: ImageOut,
    #[tsify(type = "readonly CapabilityIn[]")]
    capabilities: Vec<CapabilityIn>,
    compatibility_revision_hex: String,
}
fn qualification_out(value: ImageQualification) -> QualificationOut {
    QualificationOut {
        image: image_out(&value.image),
        capabilities: value.capabilities.iter().map(capability_out).collect(),
        compatibility_revision_hex: digest_out(value.compatibility_revision),
    }
}
fn state_out(value: MachineState) -> &'static str {
    match value {
        MachineState::Starting => "starting",
        MachineState::Running => "running",
        MachineState::Suspending => "suspending",
        MachineState::Suspended => "suspended",
        MachineState::Waking => "waking",
        MachineState::Destroying => "destroying",
        MachineState::Destroyed => "destroyed",
        MachineState::Failed => "failed",
        MachineState::Indeterminate => "indeterminate",
    }
}
#[derive(Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
pub struct EndpointOut {
    name: String,
    uri: String,
}
#[derive(Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
pub struct ObservationOut {
    id: String,
    #[tsify(
        type = "\"starting\" | \"running\" | \"suspending\" | \"suspended\" | \"waking\" | \"destroying\" | \"destroyed\" | \"failed\" | \"indeterminate\""
    )]
    state: &'static str,
    contract: ContractOut,
    #[tsify(type = "readonly EndpointOut[]")]
    endpoints: Vec<EndpointOut>,
    #[tsify(type = "string | null")]
    last_checkpoint: Option<String>,
    #[tsify(type = "number")]
    created_at_unix_ms: SafeNumber,
    #[tsify(type = "number")]
    changed_at_unix_ms: SafeNumber,
}
fn observation_out(value: MachineObservation) -> ObservationOut {
    ObservationOut {
        id: value.id.to_string(),
        state: state_out(value.state),
        contract: contract_out(&value.contract),
        endpoints: value
            .endpoints
            .into_iter()
            .map(|Endpoint { name, uri }| EndpointOut { name, uri })
            .collect(),
        last_checkpoint: value.last_checkpoint.map(|v| v.to_string()),
        created_at_unix_ms: SafeNumber(value.created_at_unix_ms),
        changed_at_unix_ms: SafeNumber(value.changed_at_unix_ms),
    }
}
#[derive(Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
pub struct CheckpointOut {
    id: String,
    source: String,
    contract: ContractOut,
    forkable: bool,
    #[tsify(type = "number")]
    created_at_unix_ms: SafeNumber,
}
fn checkpoint_out(value: CheckpointObservation) -> CheckpointOut {
    CheckpointOut {
        id: value.id.to_string(),
        source: value.source.to_string(),
        contract: contract_out(&value.contract),
        forkable: value.forkable,
        created_at_unix_ms: SafeNumber(value.created_at_unix_ms),
    }
}
#[derive(Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
pub struct PageOut {
    #[tsify(type = "readonly ObservationOut[]")]
    machines: Vec<ObservationOut>,
    #[tsify(type = "string | null")]
    next: Option<String>,
}
fn page_out(value: MachinePage) -> PageOut {
    PageOut {
        machines: value.machines.into_iter().map(observation_out).collect(),
        next: value.next.map(|v| v.to_string()),
    }
}

#[derive(Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
pub struct EventOut {
    machine: String,
    #[tsify(type = "number")]
    sequence: SafeNumber,
    #[tsify(type = "number")]
    observed_at_unix_ms: SafeNumber,
    fact: FactOut,
}
#[derive(Serialize, Tsify)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum FactOut {
    State {
        #[tsify(
            type = "\"starting\" | \"running\" | \"suspending\" | \"suspended\" | \"waking\" | \"destroying\" | \"destroyed\" | \"failed\" | \"indeterminate\""
        )]
        state: &'static str,
    },
    Pressure {
        #[tsify(type = "\"customer-budget\" | \"machine-limit\" | \"service-saturation\"")]
        pressure: &'static str,
    },
    #[serde(rename = "capacity-changed")]
    CapacityChanged,
}
fn pressure_out(value: Pressure) -> &'static str {
    match value {
        Pressure::CustomerBudget => "customer-budget",
        Pressure::MachineLimit => "machine-limit",
        Pressure::ServiceSaturation => "service-saturation",
    }
}
fn event_out(value: MachineEvent) -> EventOut {
    let fact = match value.fact {
        EventFact::State(v) => FactOut::State {
            state: state_out(v),
        },
        EventFact::Pressure(v) => FactOut::Pressure {
            pressure: pressure_out(v),
        },
        EventFact::CapacityChanged => FactOut::CapacityChanged,
    };
    EventOut {
        machine: value.machine.to_string(),
        sequence: SafeNumber(value.sequence),
        observed_at_unix_ms: SafeNumber(value.observed_at_unix_ms),
        fact,
    }
}
#[derive(Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
pub struct EventsOut {
    #[tsify(type = "readonly EventOut[]")]
    events: Vec<EventOut>,
    #[tsify(type = "number | null")]
    next_sequence: Option<SafeNumber>,
}
fn events_out(value: EventPage) -> EventsOut {
    EventsOut {
        events: value.events.into_iter().map(event_out).collect(),
        next_sequence: value.next_sequence.map(SafeNumber),
    }
}
#[derive(Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
pub struct UsageOut {
    machine: String,
    #[tsify(type = "number")]
    start_unix_ms: SafeNumber,
    #[tsify(type = "number")]
    end_unix_ms: SafeNumber,
    #[tsify(type = "bigint")]
    elastic_cpu_ns: u64,
    #[tsify(type = "bigint")]
    dedicated_cpu_ns: u64,
    #[tsify(type = "bigint")]
    private_resident_byte_seconds: u64,
    #[tsify(type = "bigint")]
    durable_private_bytes: u64,
    #[serde(with = "serde_bytes")]
    #[tsify(type = "Uint8Array")]
    lineage_receipt_sha256: Vec<u8>,
    #[tsify(type = "bigint")]
    egress_bytes: u64,
    #[serde(with = "serde_bytes")]
    #[tsify(type = "Uint8Array")]
    receipt: Vec<u8>,
}
fn usage_out(value: UsageReceipt) -> UsageOut {
    UsageOut {
        machine: value.machine.to_string(),
        start_unix_ms: SafeNumber(value.start_unix_ms),
        end_unix_ms: SafeNumber(value.end_unix_ms),
        elastic_cpu_ns: value.elastic_cpu_ns,
        dedicated_cpu_ns: value.dedicated_cpu_ns,
        private_resident_byte_seconds: value.private_resident_byte_seconds,
        durable_private_bytes: value.durable_private_bytes,
        lineage_receipt_sha256: value.lineage_receipt_sha256.to_vec(),
        egress_bytes: value.egress_bytes,
        receipt: value.receipt,
    }
}
#[derive(Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
pub struct OperationOut {
    id: String,
    #[tsify(type = "\"pending\" | \"succeeded\" | \"cancelled\" | \"indeterminate\" | \"failed\"")]
    phase: &'static str,
}

#[derive(Clone, Copy, Serialize, Tsify)]
#[serde(rename_all = "kebab-case")]
pub enum ForkFidelityOut {
    MemoryAndDisk,
    DiskOnly,
}
fn operation_out(value: OperationObservation) -> OperationOut {
    OperationOut {
        id: value.id.to_string(),
        phase: match value.phase {
            OperationPhase::Pending => "pending",
            OperationPhase::Succeeded => "succeeded",
            OperationPhase::Cancelled => "cancelled",
            OperationPhase::Indeterminate => "indeterminate",
            OperationPhase::Failed => "failed",
        },
    }
}
#[derive(Serialize, Tsify)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum MutationOut {
    Created {
        machine: ObservationOut,
    },
    Checkpointed {
        checkpoint: CheckpointOut,
    },
    Forked {
        #[tsify(type = "readonly ObservationOut[]")]
        machines: Vec<ObservationOut>,
    },
    MachineForked {
        source: String,
        fidelity: ForkFidelityOut,
        #[tsify(type = "readonly ObservationOut[]")]
        children: Vec<ObservationOut>,
    },
    Suspended {
        #[serde(rename = "machineId")]
        machine_id: String,
    },
    Woken {
        #[serde(rename = "machineId")]
        machine_id: String,
    },
    #[serde(rename = "suspension-policy-set")]
    SuspensionPolicySet {
        #[serde(rename = "machineId")]
        machine_id: String,
        policy: TimedOut,
    },
    #[serde(rename = "machine-destroyed")]
    MachineDestroyed {
        #[serde(rename = "machineId")]
        machine_id: String,
    },
    #[serde(rename = "checkpoint-destroyed")]
    CheckpointDestroyed {
        #[serde(rename = "checkpointId")]
        checkpoint_id: String,
    },
}
fn mutation_out(value: MutationOutcome) -> MutationOut {
    match value {
        MutationOutcome::Created(v) => MutationOut::Created {
            machine: observation_out(v),
        },
        MutationOutcome::Checkpointed(v) => MutationOut::Checkpointed {
            checkpoint: checkpoint_out(v),
        },
        MutationOutcome::Forked(v) => MutationOut::Forked {
            machines: v.into_iter().map(observation_out).collect(),
        },
        MutationOutcome::MachineForked {
            source,
            fidelity,
            children,
        } => MutationOut::MachineForked {
            source: source.to_string(),
            fidelity: match fidelity {
                ForkFidelity::MemoryAndDisk => ForkFidelityOut::MemoryAndDisk,
                ForkFidelity::DiskOnly => ForkFidelityOut::DiskOnly,
            },
            children: children.into_iter().map(observation_out).collect(),
        },
        MutationOutcome::Suspended(v) => MutationOut::Suspended {
            machine_id: v.to_string(),
        },
        MutationOutcome::Woken(v) => MutationOut::Woken {
            machine_id: v.to_string(),
        },
        MutationOutcome::SuspensionPolicySet(v, p) => MutationOut::SuspensionPolicySet {
            machine_id: v.to_string(),
            policy: suspension_out(&p),
        },
        MutationOutcome::MachineDestroyed(v) => MutationOut::MachineDestroyed {
            machine_id: v.to_string(),
        },
        MutationOutcome::CheckpointDestroyed(v) => MutationOut::CheckpointDestroyed {
            checkpoint_id: v.to_string(),
        },
    }
}

#[derive(Deserialize, Serialize, Tsify)]
#[tsify(from_wasm_abi)]
#[serde(rename_all = "camelCase")]
pub struct MachineKey {
    machine_id: String,
    idempotency_key: String,
}
#[derive(Deserialize, Serialize, Tsify)]
#[tsify(from_wasm_abi)]
#[serde(rename_all = "camelCase")]
pub struct CheckpointKey {
    checkpoint_id: String,
    idempotency_key: String,
}
#[derive(Deserialize, Serialize, Tsify)]
#[tsify(from_wasm_abi)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ForkIn {
    checkpoint_id: String,
    count: u32,
    idempotency_key: String,
}

#[derive(Deserialize, Serialize, Tsify)]
#[tsify(from_wasm_abi)]
#[serde(rename_all = "camelCase")]
pub struct MachineForkIn {
    machine_id: String,
    count: u32,
    idempotency_key: String,
}
#[derive(Deserialize, Serialize, Tsify)]
#[tsify(from_wasm_abi)]
#[serde(rename_all = "camelCase")]
pub struct PolicyIn {
    machine_id: String,
    policy: SuspensionIn,
    idempotency_key: String,
}
#[derive(Deserialize, Serialize, Tsify)]
#[tsify(from_wasm_abi)]
#[serde(rename_all = "camelCase")]
pub struct ListIn {
    #[tsify(type = "string | null")]
    after: Option<String>,
    limit: u32,
}
#[derive(Deserialize, Serialize, Tsify)]
#[tsify(from_wasm_abi)]
#[serde(rename_all = "camelCase")]
pub struct EventsIn {
    machine_id: String,
    #[tsify(type = "number | null")]
    after_sequence: Option<SafeInput>,
    limit: u32,
}
#[derive(Deserialize, Serialize, Tsify)]
#[tsify(from_wasm_abi)]
#[serde(rename_all = "camelCase")]
pub struct UsageIn {
    machine_id: String,
    #[tsify(type = "number")]
    start_unix_ms: SafeInput,
    #[tsify(type = "number")]
    end_unix_ms: SafeInput,
}

#[allow(
    clippy::too_many_lines,
    reason = "the public WASM operation dispatch is the stable ABI route table"
)]
pub async fn dispatch<P: MachinesProvider>(
    provider: &P,
    operation_name: &str,
    payload: JsValue,
) -> Result<JsValue, JsValue> {
    macro_rules! call {
        ($future:expr, $map:expr) => {{
            let value = $future.await.map_err(provider_error)?;
            to_js(&$map(value))
        }};
    }
    match operation_name {
        "qualifyImage" => {
            let input: ImageIn = from_js(payload)?;
            call!(provider.qualify_image(image_in(input)?), qualification_out)
        }
        "create" => {
            reject_retired_mode(&payload)?;
            let input: CreateIn = from_js(payload)?;
            call!(provider.create(create_in(input)?), mutation_out)
        }
        "inspectMachine" => {
            call!(
                provider.inspect_machine(machine(from_js(payload)?)?),
                observation_out
            )
        }
        "listMachines" => {
            let input: ListIn = from_js(payload)?;
            call!(
                provider.list_machines(input.after.map(machine).transpose()?, input.limit),
                page_out
            )
        }
        "checkpoint" => {
            let input: MachineKey = from_js(payload)?;
            call!(
                provider.checkpoint(machine(input.machine_id)?, key(input.idempotency_key)?),
                mutation_out
            )
        }
        "inspectCheckpoint" => {
            call!(
                provider.inspect_checkpoint(checkpoint(from_js(payload)?)?),
                checkpoint_out
            )
        }
        "fork" => {
            reject_retired_mode(&payload)?;
            let input: ForkIn = from_js(payload)?;
            let count = NonZeroU32::new(input.count).ok_or_else(|| err("count must be nonzero"))?;
            call!(
                provider.fork(
                    checkpoint(input.checkpoint_id)?,
                    count,
                    key(input.idempotency_key)?
                ),
                mutation_out
            )
        }
        "forkMachine" => {
            let input: MachineForkIn = from_js(payload)?;
            let count = NonZeroU32::new(input.count).ok_or_else(|| err("count must be nonzero"))?;
            call!(
                provider.fork_machine(
                    machine(input.machine_id)?,
                    count,
                    key(input.idempotency_key)?
                ),
                mutation_out
            )
        }
        "suspend" => {
            let input: MachineKey = from_js(payload)?;
            call!(
                provider.suspend(machine(input.machine_id)?, key(input.idempotency_key)?),
                mutation_out
            )
        }
        "wake" => {
            let input: MachineKey = from_js(payload)?;
            call!(
                provider.wake(machine(input.machine_id)?, key(input.idempotency_key)?),
                mutation_out
            )
        }
        "setSuspensionPolicy" => {
            let input: PolicyIn = from_js(payload)?;
            call!(
                provider.set_suspension_policy(
                    machine(input.machine_id)?,
                    suspension_in(input.policy)?,
                    key(input.idempotency_key)?
                ),
                mutation_out
            )
        }
        "destroyMachine" => {
            let input: MachineKey = from_js(payload)?;
            call!(
                provider.destroy_machine(machine(input.machine_id)?, key(input.idempotency_key)?),
                mutation_out
            )
        }
        "destroyCheckpoint" => {
            let input: CheckpointKey = from_js(payload)?;
            call!(
                provider.destroy_checkpoint(
                    checkpoint(input.checkpoint_id)?,
                    key(input.idempotency_key)?
                ),
                mutation_out
            )
        }
        "events" => {
            let input: EventsIn = from_js(payload)?;
            call!(
                provider.events(
                    machine(input.machine_id)?,
                    input.after_sequence.map(|v| v.0),
                    input.limit
                ),
                events_out
            )
        }
        "usage" => {
            let input: UsageIn = from_js(payload)?;
            call!(
                provider.usage(
                    machine(input.machine_id)?,
                    input.start_unix_ms.0,
                    input.end_unix_ms.0
                ),
                usage_out
            )
        }
        "recover" => {
            call!(provider.recover(key(from_js(payload)?)?), mutation_out)
        }
        "recoverOperation" => {
            let value = provider
                .recover_operation(key(from_js(payload)?)?)
                .await
                .map_err(provider_error)?;
            to_js(&value.to_string())
        }
        "inspectOperation" => {
            call!(
                provider.inspect_operation(operation(from_js(payload)?)?),
                operation_out
            )
        }
        "cancel" => {
            call!(
                provider.cancel(operation(from_js(payload)?)?),
                operation_out
            )
        }
        "watchOperation" => {
            let mut stream = provider
                .watch_operation(operation(from_js(payload)?)?)
                .await
                .map_err(provider_error)?;
            let mut values = Vec::new();
            while let Some(value) = futures::StreamExt::next(&mut stream).await {
                values.push(operation_out(value.map_err(provider_error)?));
            }
            to_js(&values)
        }
        _ => invalid("unknown Machines operation"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_digest_is_strict_and_lowercase() {
        let value = "AB".repeat(32);
        assert!(digest(&value, "digestHex").is_ok());
        assert_eq!(digest_out([0xab; 32]), "ab".repeat(32));
    }

    #[test]
    fn public_enum_spellings_are_kebab_case() {
        assert!(matches!(
            capability_out(&Capability::LiveCheckpoint),
            CapabilityIn::LiveCheckpoint
        ));
        assert_eq!(state_out(MachineState::Running), "running");
        assert_eq!(pressure_out(Pressure::CustomerBudget), "customer-budget");
    }
}
