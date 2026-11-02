//! Borrowed resource files under `live/agents/<p>/resources/`.
//!
//! The existing authorized reader supplies the original committed record and
//! its decoded value once. Projection never discovers a machine, re-reads a
//! source, polls a provider, walks descendants or publishes another record.
//! Subtree counters require the original indexed aggregate at its admitted cut;
//! inherited history and shared physical allocations are not added as new work.
//! These borrowed values describe admitted evidence, not source authorization.
//! The original reader binds each decoded producer value to that same record
//! before projection; a separately supplied source tuple cannot establish it.

use crate::executor::{
    ModelPriceError, ModelPriceRecord, ModelUsageProjection, ModelUsageRecord, ModelUsageTotals,
};
use acyclic_auth::addressing::AccountId;
use acyclic_stream::{Record, StreamPath};
use serde::Serialize;

/// Resource file projection layout. Original producer layouts remain distinct.
pub const VERSION: u32 = 1;

/// Original immutable identity; selected and originating streams stay distinct.
/// Account bytes are opaque and lossless, never interpreted as a host or name.
#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Source<'a> {
    /// Original authorized account's opaque bytes, without UTF-8 conversion.
    pub account: &'a [u8],
    /// Stream selected by the original read; may inherit from another origin.
    pub stream: &'a str,
    /// Producer stream retained by the immutable record, before fork inheritance.
    pub origin: &'a str,
    /// Original zero-based stream position, not a journal or inbox cursor.
    pub sequence: u64,
    /// Original publication identity, borrowed without hashing the value again.
    pub commit_id: &'a [u8; 32],
    /// Original authority observation, never a fresh projection timestamp.
    pub committed_at_micros: u64,
}

impl<'a> Source<'a> {
    /// Retain original evidence with no value parsing, hashing or copying.
    pub fn project(account: &'a AccountId, selected: &'a StreamPath, record: &'a Record) -> Self {
        Self {
            account: account.as_bytes(),
            stream: selected.as_str(),
            origin: record.origin.as_str(),
            sequence: record.sequence,
            commit_id: record.commit_id.as_bytes(),
            committed_at_micros: record.committed_at_micros,
        }
    }
}

/// Projection refusal retains missing evidence rather than creating a zero.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    /// The original usage, cache or placement layout cannot be interpreted.
    #[error(
        "resource source version is unsupported; recovery: supply the original producer's current layout"
    )]
    Version,
    /// A logical value length cannot fit the public byte counter.
    #[error(
        "resource byte count cannot be represented; recovery: preserve the original source with unknown byte coverage"
    )]
    ByteCount,
    /// Preserve the shared price/accounting authority's typed refusal.
    #[error(transparent)]
    Price(#[from] ModelPriceError),
}

/// One `cache/calls/<id>.json`: original policy/prefix, reported read/write
/// counters and selected cost. Call-level cache tokens do not identify an exact
/// cached prefix or prove residency on a physical host.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CacheCall<'a> {
    /// Resource file layout, independent of the retained usage-file layout.
    pub version: u32,
    /// Original committed usage source selected by the authorized read.
    pub source: Source<'a>,
    /// Original dispatch, token conventions, prefix policy and selected price.
    pub call: ModelUsageProjection<'a>,
    /// Signed nano-units in the call's explicitly selected currency.
    pub cache_savings_nano_units: Option<i128>,
}

impl<'a> CacheCall<'a> {
    /// Reuse the one usage/price interpretation; pending calls retain unknown
    /// completion counters and cannot supply a saving or a prefix-hit claim.
    /// The original journal reader must already have bound this usage to its
    /// committed dispatch, purpose and request; format/price checks here do not
    /// authenticate a separately supplied source and decoded value.
    pub fn project(
        source: Source<'a>,
        usage: &'a ModelUsageRecord,
        price: Option<&'a ModelPriceRecord>,
    ) -> Result<Self, Error> {
        if usage.version != ModelUsageRecord::VERSION
            || usage.cache.is_some_and(|cache| {
                cache.version != crate::executor::ModelCacheObservation::VERSION
            })
        {
            return Err(Error::Version);
        }
        let (call, cache_savings_nano_units) = usage.estimate_with_cache_savings(price)?;
        Ok(Self {
            version: VERSION,
            source,
            call,
            cache_savings_nano_units,
        })
    }
}

/// One `cache/prefixes/<id>.json`; an original domain-bound estimate, never a
/// provider observation inferred from an agent's location or its fork parent.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CacheResidency<'a> {
    /// Resource file layout, independent of the residency estimate's layout.
    pub version: u32,
    /// Original committed estimate source, not a later machine lookup.
    pub source: Source<'a>,
    /// Original domain, exact prefix key, observed write/hit and expiry evidence.
    pub estimate: &'a crate::executor::ModelCacheResidency,
    /// Unknown without a comparable original query coordinate.
    pub estimated_resident: Option<bool>,
}

impl<'a> CacheResidency<'a> {
    /// Compare only the supplied original domain/coordinate; no clock is read.
    /// Missing or foreign coordinates preserve unknown residency.
    pub fn project(
        source: Source<'a>,
        estimate: &'a crate::executor::ModelCacheResidency,
        query: Option<([u8; 16], u64)>,
    ) -> Result<Self, Error> {
        if estimate.version != crate::executor::ModelCacheResidency::VERSION {
            return Err(Error::Version);
        }
        Ok(Self {
            version: VERSION,
            source,
            estimate,
            estimated_resident: query
                .and_then(|(domain, at)| estimate.estimated_resident_at(domain, at)),
        })
    }
}

/// One selected original record in `sharing/`, paged by the existing reader.
/// Equal values in unrelated records never imply shared allocation custody.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Sharing<'a> {
    /// Resource file layout.
    pub version: u32,
    /// Original publication plus the independently selected inherited stream.
    pub source: Source<'a>,
    /// The immutable record's origin differs from the selected stream.
    pub inherited: bool,
    /// Exact original record value bytes; framing and physical work are uncovered.
    pub logical_value_bytes: u64,
    /// Unknown without original allocation-custody evidence.
    pub physical_shared_bytes: Option<u64>,
    /// Unknown without original allocation-custody evidence.
    pub physical_copied_bytes: Option<u64>,
}

impl<'a> Sharing<'a> {
    /// Borrow the original record identity and length without parsing, hashing
    /// or copying its value. Inheritance alone supplies no physical byte count.
    pub fn project(
        account: &'a AccountId,
        selected: &'a StreamPath,
        record: &'a Record,
    ) -> Result<Self, Error> {
        Ok(Self {
            version: VERSION,
            source: Source::project(account, selected, record),
            inherited: selected != &record.origin,
            logical_value_bytes: u64::try_from(record.value.len()).map_err(|_| Error::ByteCount)?,
            physical_shared_bytes: None,
            physical_copied_bytes: None,
        })
    }
}

/// One original filesystem authority fact in `storage/`, selected by the
/// existing proven accounting page. Retentions refer to logical generations;
/// neither equal roots nor multiple references prove physical bytes saved.
#[cfg(feature = "filesystem")]
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Storage<'a> {
    /// Resource file layout; this does not change the filesystem authority wire.
    pub version: u32,
    /// Original authorized account's opaque bytes, without UTF-8 conversion.
    pub account: &'a [u8],
    /// Original authority, commit, operation, settlement and logical transition.
    /// The original reader must bind the admitted authority to the agent's
    /// volume; the fact alone does not establish agent ownership or current use.
    pub fact: &'a acyclic_fs::accounting::AccountingFact,
    /// Unknown without original allocation-custody evidence.
    pub physical_shared_bytes: Option<u64>,
    /// Unknown without original allocation-custody evidence.
    pub physical_copied_bytes: Option<u64>,
}

#[cfg(feature = "filesystem")]
impl<'a> Storage<'a> {
    /// Borrow the original proven fact without another authority/object read,
    /// generation walk, sum, payload copy, clock or retained lookup table.
    /// AccountingPage.work measures projection work, not agent execution work.
    pub fn project(
        account: &'a AccountId,
        fact: &'a acyclic_fs::accounting::AccountingFact,
    ) -> Self {
        Self {
            version: VERSION,
            account: account.as_bytes(),
            fact,
            physical_shared_bytes: None,
            physical_copied_bytes: None,
        }
    }
}

/// `own.json` or `subtree.json` over the original aggregate's selected range.
/// Missing producers stay null. Model unknown counts remain in the original
/// ModelUsageTotals; a partial known subtotal must never be shown as a total.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Counters<'a> {
    /// Resource file layout, independent of the retained accounting layouts.
    pub version: u32,
    /// Original selected aggregate's publication and cut.
    pub source: Source<'a>,
    /// Existing accounting totals; each unknown count remains explicit.
    pub model: Option<&'a ModelUsageTotals>,
    /// Original filesystem operation work when its producer supplies coverage.
    /// Peak allocation is a peak, never an additive subtree byte total.
    #[cfg(feature = "filesystem")]
    pub work: Option<&'a acyclic_fs::WorkCounters>,
    /// Original authenticated Machine interval receipt, without inferred cost.
    #[cfg(feature = "machines")]
    pub machine: Option<&'a acyclic_machines::UsageReceipt>,
}

impl<'a> Counters<'a> {
    /// Supply only the original selected aggregate, not a sum reconstructed by
    /// walking agents. Work and Machine receipts require their own producer.
    pub fn project(source: Source<'a>, model: Option<&'a ModelUsageTotals>) -> Result<Self, Error> {
        if let Some(model) = model {
            model.validate()?;
        }
        Ok(Self {
            version: VERSION,
            source,
            model,
            #[cfg(feature = "filesystem")]
            work: None,
            #[cfg(feature = "machines")]
            machine: None,
        })
    }
}

/// Original logical Machine binding in `placements/<target>.json`.
/// A provider reference is not evidence for its physical host, NUMA or region.
#[cfg(feature = "machines")]
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Placement<'a> {
    /// Resource file layout, independent of the original binding layout.
    pub version: u32,
    /// Original settled binding's publication, not the current configuration.
    pub source: Source<'a>,
    /// Original provider-scoped machine, target and fork identity.
    pub binding: &'a crate::machines::MachineBinding,
    /// Original physical host identity; absent from a logical binding alone.
    pub host: Option<&'a str>,
    /// Original admitted region, unknown without its placement producer.
    pub region: Option<&'a str>,
    /// Original NUMA placement, unknown without its placement producer.
    pub numa: Option<u32>,
    /// Original core placement, unknown without its placement producer.
    pub core: Option<u32>,
    /// Original executor placement, unknown without its placement producer.
    pub executor_shard: Option<&'a str>,
}

#[cfg(feature = "machines")]
impl<'a> Placement<'a> {
    /// Retain a current-layout logical binding without a provider discovery
    /// read. Its opaque reference cannot authorize physical placement claims.
    /// The original authorized reader must first admit the settled record at
    /// position one and its selected agent/target through the shared Machine
    /// binding decoder. This borrowed projection does not decode or certify
    /// that source association again.
    pub fn project(
        source: Source<'a>,
        binding: &'a crate::machines::MachineBinding,
    ) -> Result<Self, Error> {
        if binding.version != crate::machines::MACHINE_BINDING_VERSION {
            return Err(Error::Version);
        }
        Ok(Self {
            version: VERSION,
            source,
            binding,
            host: None,
            region: None,
            numa: None,
            core: None,
            executor_shard: None,
        })
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
