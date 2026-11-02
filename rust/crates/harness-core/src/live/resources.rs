//! Borrowed resource files under `live/agents/<p>/resources/`.
//!
//! The existing authorized reader supplies the original committed record and
//! its decoded value once. Projection never discovers a machine, re-reads a
//! source, polls a provider, walks descendants or publishes another record.
//! Subtree counters require the original indexed aggregate at its admitted cut;
//! inherited history and shared physical allocations are not added as new work.

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
    pub account: &'a [u8],
    pub stream: &'a str,
    pub origin: &'a str,
    pub sequence: u64,
    pub commit_id: &'a [u8; 32],
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
    #[error(
        "resource source version is unsupported; recovery: supply the original producer's current layout"
    )]
    Version,
    #[error(
        "resource byte count cannot be represented; recovery: preserve the original source with unknown byte coverage"
    )]
    ByteCount,
    #[error(transparent)]
    Price(#[from] ModelPriceError),
}

/// One `cache/calls/<id>.json`: original policy/prefix, reported read/write
/// counters and selected cost. Call-level cache tokens do not identify an exact
/// cached prefix or prove residency on a physical host.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CacheCall<'a> {
    pub version: u32,
    pub source: Source<'a>,
    pub call: ModelUsageProjection<'a>,
    /// Signed nano-units in the call's explicitly selected currency.
    pub cache_savings_nano_units: Option<i128>,
}

impl<'a> CacheCall<'a> {
    /// Reuse the one usage/price interpretation; pending calls retain unknown
    /// completion counters and cannot supply a saving or a prefix-hit claim.
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
    pub version: u32,
    pub source: Source<'a>,
    pub estimate: &'a crate::executor::ModelCacheResidency,
    /// Unknown without a comparable original query coordinate.
    pub estimated_resident: Option<bool>,
}

impl<'a> CacheResidency<'a> {
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
    pub version: u32,
    pub source: Source<'a>,
    pub inherited: bool,
    /// Exact original record value bytes; framing and physical work are uncovered.
    pub logical_value_bytes: u64,
    pub physical_shared_bytes: Option<u64>,
    pub physical_copied_bytes: Option<u64>,
}

impl<'a> Sharing<'a> {
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

/// `own.json` or `subtree.json` over the original aggregate's selected range.
/// Missing producers stay null. Model unknown counts remain in the original
/// ModelUsageTotals; a partial known subtotal must never be shown as a total.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Counters<'a> {
    pub version: u32,
    pub source: Source<'a>,
    pub model: Option<&'a ModelUsageTotals>,
    #[cfg(feature = "filesystem")]
    pub work: Option<&'a acyclic_fs::WorkCounters>,
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
    pub version: u32,
    pub source: Source<'a>,
    pub binding: &'a crate::machines::MachineBinding,
    pub host: Option<&'a str>,
    pub region: Option<&'a str>,
    pub numa: Option<u32>,
    pub core: Option<u32>,
    pub executor_shard: Option<&'a str>,
}

#[cfg(feature = "machines")]
impl<'a> Placement<'a> {
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
