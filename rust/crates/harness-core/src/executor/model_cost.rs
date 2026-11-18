//! A price-selected estimate over the original committed usage, never an invoice.
use super::{ModelTokenUsage, ModelUsageRecord};
use serde::{Deserialize, Serialize};
use std::{borrow::Cow, collections::BTreeMap};

/// An explicitly selected JSONL price file, indexed by exact admitted names.
/// One file selects one currency and one revision per provider/model. Neither
/// incoming usage nor a clock can change that selection.
#[derive(Default)]
pub struct ModelPrices {
    currency: Option<String>,
    records: BTreeMap<String, BTreeMap<String, ModelPriceRecord>>,
}

impl ModelPrices {
    /// Decode the existing version-one price-record JSONL format. Empty files
    /// select no prices. Invalid or ambiguous records refuse the whole file.
    pub fn from_jsonl(bytes: &[u8]) -> crate::Result<Self> {
        let records = bytes
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.iter().all(u8::is_ascii_whitespace))
            .map(crate::contract::json_from_slice::<ModelPriceRecord>)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| crate::Error::Invalid(format!("price file: {error}")))?;
        Self::new(records).map_err(|error| crate::Error::Invalid(format!("price file: {error}")))
    }

    /// Admit caller-selected records without aliases or implicit revision order.
    pub fn new(
        records: impl IntoIterator<Item = ModelPriceRecord>,
    ) -> Result<Self, ModelPriceError> {
        let mut selected = Self::default();
        for record in records {
            record.validate()?;
            match &selected.currency {
                Some(currency) if currency != &record.currency => {
                    return Err(ModelPriceError::CurrencyMismatch);
                }
                None => selected.currency = Some(record.currency.clone()),
                _ => {}
            }
            if selected
                .records
                .entry(record.provider.clone())
                .or_default()
                .insert(record.model.clone(), record)
                .is_some()
            {
                return Err(ModelPriceError::InvalidRecord);
            }
        }
        Ok(selected)
    }

    /// The currency explicitly selected by this file, absent for an empty file.
    pub fn currency(&self) -> Option<&str> {
        self.currency.as_deref()
    }

    /// Start an empty aggregate in this admitted file's explicitly selected
    /// currency. An empty price file keeps monetary observations unknown.
    pub fn totals(&self) -> ModelUsageTotals {
        ModelUsageTotals {
            currency: self.currency.clone(),
            ..ModelUsageTotals::unpriced()
        }
    }

    /// Select only an exact provider/model binding, without allocating.
    pub fn price(&self, usage: &ModelUsageRecord) -> Option<&ModelPriceRecord> {
        self.records.get(&usage.provider)?.get(&usage.model)
    }
}

/// How a provider reports a separately priced token counter.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields, rename_all = "snake_case")]
pub enum TokenPrice {
    /// Already charged at the parent input/output rate; no extra charge.
    Included,
    /// Part of the parent count, charged at this rate instead of its rate.
    Subset {
        /// Rate in billionths of currency per million tokens; None is unknown.
        nano_units_per_million: Option<u64>,
    },
    /// Not part of the parent count, charged in addition to it.
    Additional {
        /// Rate in billionths of currency per million tokens; None is unknown.
        nano_units_per_million: Option<u64>,
    },
}

/// Immutable, caller-selected price record. No schedule, currency conversion or
/// model alias is inferred. Selection must retain this revision when replayed;
/// `effective_at` is provenance, not a clock or an automatic selection rule.
/// Prices may be absent. No provider/account lookup occurs in this projection.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelPriceRecord {
    /// Price record schema, currently one.
    pub version: u32,
    /// Publisher's immutable price revision.
    pub revision: String,
    /// Exact admitted provider name, without alias resolution.
    pub provider: String,
    /// Exact admitted model name, without alias resolution.
    pub model: String,
    /// Effective time in Unix seconds, supplied by the price publisher.
    pub effective_at: u64,
    /// Three uppercase ASCII currency letters.
    pub currency: String,
    /// Rates in billionths of the currency per million tokens. None is unknown.
    pub input_nano_units_per_million: Option<u64>,
    /// Output rate in the same units, before separately priced reasoning.
    pub output_nano_units_per_million: Option<u64>,
    /// Present included/subset cache-read and cache-write counts are disjoint.
    pub cached: TokenPrice,
    /// Cache creation relative to the original input count.
    pub cache_write: TokenPrice,
    /// Relative to the provider's original output count.
    pub reasoning: TokenPrice,
}

/// A refused price binding or incremental query update.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ModelPriceError {
    /// Unsupported format, absent revision or invalid currency.
    #[error("invalid model price record")]
    InvalidRecord,
    /// The selected price describes a different provider/model.
    #[error("model price differs from the admitted provider or model")]
    BindingMismatch,
    /// One aggregate cannot mix currencies without an exchange-rate source.
    #[error("usage total cannot combine different currencies")]
    CurrencyMismatch,
    /// A total cannot represent the result, or the prior contribution is absent.
    #[error("usage total overflows its representation")]
    Overflow,
    /// A replacement cannot change the selected call identity.
    #[error("usage replacement differs from its original call")]
    CallMismatch,
}

/// Selected price provenance and estimated token cost. Tool calls, storage,
/// taxes, subscription limits and invoice adjustments are not token charges
/// and are not estimated here. Their absence must not be shown as zero cost.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelCostEstimate<'a> {
    /// Selected immutable price revision; unknown when no price was selected.
    pub price_revision: Option<Cow<'a, str>>,
    /// Selected price's effective Unix seconds, without an inferred clock.
    pub effective_at: Option<u64>,
    /// Currency of the selected rate; unknown when no price was selected.
    pub currency: Option<Cow<'a, str>>,
    /// Billionths of currency, rounded up once per call. None means unknown
    /// usage/rates, contradictory subset counts, or arithmetic overflow.
    pub token_nano_units: Option<u64>,
}

/// Version-three usage-file row, retaining the original journal record's
/// dispatch identity and reported counter conventions.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelUsageProjection<'a> {
    /// Usage-file format, currently three; distinct from the journal record format.
    pub version: u32,
    /// Original journal record, borrowed while writing and owned after decoding.
    pub usage: Cow<'a, ModelUsageRecord>,
    /// Derived token estimate and the selected price provenance.
    pub estimated_cost: ModelCostEstimate<'a>,
}

impl ModelUsageProjection<'_> {
    /// Usage-file layout, distinct from the source journal and aggregate layouts.
    pub const VERSION: u32 = 3;

    // Called after currency admission. A saved projection cannot turn pending
    // usage or an absent price selection into a known charge.
    fn contribution(&self, priced: bool) -> (ModelTokenUsage, Option<u64>) {
        if !self.usage.completed {
            return (ModelTokenUsage::default(), None);
        }
        let cost = if priced && self.estimated_cost.currency.is_some() {
            self.estimated_cost.token_nano_units
        } else {
            None
        };
        (self.usage.tokens, cost)
    }
}

fn currency_valid(currency: &str) -> bool {
    currency.len() == 3 && currency.bytes().all(|byte| byte.is_ascii_uppercase())
}

fn charge(tokens: u64, rate: Option<u64>) -> Option<u128> {
    // Known zero units cost zero even when their unit price is unknown.
    Some(u128::from(tokens) * u128::from(if tokens == 0 { 0 } else { rate? }))
}

struct UsageEstimate<'a> {
    call: ModelUsageProjection<'a>,
    input_tokens: Option<u64>,
    input_cost: Option<u128>,
}

fn family_cost(
    parent: Option<u64>,
    rate: Option<u64>,
    children: &[(Option<u64>, TokenPrice)],
) -> Option<u128> {
    family(parent, rate, children)?.cost
}

struct Family {
    tokens: Option<u64>,
    cost: Option<u128>,
}

// One interpretation of included/subset/additional counters serves both the
// public inclusive count and billing. Unknown prices do not erase known units.
fn family(
    parent: Option<u64>,
    rate: Option<u64>,
    children: &[(Option<u64>, TokenPrice)],
) -> Option<Family> {
    let mut parent = parent?;
    let mut inclusive = parent;
    let mut tokens_total = Some(parent);
    let mut extra = Some(0u128);
    for &(tokens, price) in children {
        match price {
            TokenPrice::Included => {
                if let Some(tokens) = tokens {
                    inclusive = inclusive.checked_sub(tokens)?;
                }
            }
            TokenPrice::Subset {
                nano_units_per_million,
            } => {
                let tokens = tokens?;
                inclusive = inclusive.checked_sub(tokens)?;
                parent = parent.checked_sub(tokens)?;
                extra = extra
                    .and_then(|extra| extra.checked_add(charge(tokens, nano_units_per_million)?));
            }
            TokenPrice::Additional {
                nano_units_per_million,
            } => {
                let tokens = tokens?;
                tokens_total = tokens_total.and_then(|total| total.checked_add(tokens));
                extra = extra
                    .and_then(|extra| extra.checked_add(charge(tokens, nano_units_per_million)?));
            }
        }
    }
    Some(Family {
        tokens: tokens_total,
        cost: charge(parent, rate).and_then(|ordinary| ordinary.checked_add(extra?)),
    })
}

impl ModelPriceRecord {
    /// Validate an immutable caller-selected price before source admission.
    pub fn validate(&self) -> Result<(), ModelPriceError> {
        if self.version != 1
            || self.revision.is_empty()
            || self.provider.is_empty()
            || self.model.is_empty()
            || !currency_valid(&self.currency)
        {
            return Err(ModelPriceError::InvalidRecord);
        }
        Ok(())
    }

    fn validate_for(&self, usage: &ModelUsageRecord) -> Result<(), ModelPriceError> {
        self.validate()?;
        if self.provider != usage.provider || self.model != usage.model {
            return Err(ModelPriceError::BindingMismatch);
        }
        Ok(())
    }
}

impl ModelUsageRecord {
    /// Net input-token savings against billing the same reads and writes at the
    /// selected ordinary input rate. Cache creation premiums can make this
    /// negative. Billionths of the selected currency, truncated toward zero
    /// once after subtraction. Unknown counters/rates/overflow remain unknown.
    /// Uses the same price interpretation as `estimate`, without IO or allocation.
    pub fn cache_savings(&self, price: &ModelPriceRecord) -> Result<Option<i128>, ModelPriceError> {
        price.validate_for(self)?;
        let selected = self.completed.then(|| self.input_family(price)).flatten();
        Ok(self.cache_savings_validated(price, selected))
    }

    fn input_family(&self, price: &ModelPriceRecord) -> Option<u128> {
        family_cost(
            self.tokens.input,
            price.input_nano_units_per_million,
            &[
                (self.tokens.cached, price.cached),
                (self.tokens.cache_write, price.cache_write),
            ],
        )
    }

    fn cache_savings_validated(
        &self,
        price: &ModelPriceRecord,
        selected_input: Option<u128>,
    ) -> Option<i128> {
        if !self.completed {
            return None;
        }
        let ordinary = |rate| match rate {
            TokenPrice::Included => TokenPrice::Included,
            TokenPrice::Subset { .. } => TokenPrice::Subset {
                nano_units_per_million: price.input_nano_units_per_million,
            },
            TokenPrice::Additional { .. } => TokenPrice::Additional {
                nano_units_per_million: price.input_nano_units_per_million,
            },
        };
        let input = |cached, written| {
            family_cost(
                self.tokens.input,
                price.input_nano_units_per_million,
                &[
                    (self.tokens.cached, cached),
                    (self.tokens.cache_write, written),
                ],
            )
            .and_then(|amount| i128::try_from(amount).ok())
        };
        input(ordinary(price.cached), ordinary(price.cache_write))
            .zip(selected_input.and_then(|amount| i128::try_from(amount).ok()))
            .map(|(ordinary, cached)| (ordinary - cached) / 1_000_000)
    }

    /// Projects this call against one explicitly selected price, with the
    /// signed cache savings against pricing every input token as ordinary input.
    /// Both views use the one price admission, without duplicating billing or
    /// allocating an owned call. The counterfactual is evaluated only on demand.
    ///
    /// # Errors
    ///
    /// Refuses a price that does not admit this call.
    pub fn estimate_with_cache_savings<'a>(
        &'a self,
        price: Option<&'a ModelPriceRecord>,
    ) -> Result<(ModelUsageProjection<'a>, Option<i128>), ModelPriceError> {
        let estimate = self.estimate_parts(price)?;
        Ok((
            estimate.call,
            price.and_then(|price| self.cache_savings_validated(price, estimate.input_cost)),
        ))
    }

    /// Projects one original row against one explicitly selected immutable
    /// price. Constant work, zero allocation/copy/IO/wait. A mismatched binding
    /// is rejected rather than silently priced as another model.
    pub fn estimate<'a>(
        &'a self,
        price: Option<&'a ModelPriceRecord>,
    ) -> Result<ModelUsageProjection<'a>, ModelPriceError> {
        self.estimate_parts(price).map(|estimate| estimate.call)
    }

    // Retain inclusive units from the same original billing interpretation.
    pub(crate) fn estimate_with_input<'a>(
        &'a self,
        price: Option<&'a ModelPriceRecord>,
    ) -> Result<(ModelUsageProjection<'a>, Option<u64>), ModelPriceError> {
        let estimate = self.estimate_parts(price)?;
        Ok((estimate.call, estimate.input_tokens))
    }

    fn estimate_parts<'a>(
        &'a self,
        price: Option<&'a ModelPriceRecord>,
    ) -> Result<UsageEstimate<'a>, ModelPriceError> {
        let mut input_tokens = None;
        let mut estimated_cost = ModelCostEstimate {
            price_revision: None,
            effective_at: None,
            currency: None,
            token_nano_units: None,
        };
        let mut input_cost = None;
        if let Some(price) = price {
            price.validate_for(self)?;
            estimated_cost.price_revision = Some(Cow::Borrowed(&price.revision));
            estimated_cost.effective_at = Some(price.effective_at);
            estimated_cost.currency = Some(Cow::Borrowed(&price.currency));
            estimated_cost.token_nano_units = self
                .completed
                .then(|| {
                    let input = family(
                        self.tokens.input,
                        price.input_nano_units_per_million,
                        &[
                            (self.tokens.cached, price.cached),
                            (self.tokens.cache_write, price.cache_write),
                        ],
                    )?;
                    input_tokens = input.tokens;
                    input_cost = input.cost;
                    let output = family_cost(
                        self.tokens.output,
                        price.output_nano_units_per_million,
                        &[(self.tokens.reasoning, price.reasoning)],
                    )?;
                    u64::try_from(input.cost?.checked_add(output)?.div_ceil(1_000_000)).ok()
                })
                .flatten();
        }
        Ok(UsageEstimate {
            input_tokens,
            call: ModelUsageProjection {
                version: ModelUsageProjection::VERSION,
                usage: Cow::Borrowed(self),
                estimated_cost,
            },
            input_cost,
        })
    }
}

/// Partial sum with explicit unknown observations. An empty selection has a
/// zero sum and zero unknowns; it is distinct from one unpriced call.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct UsageTotal {
    /// Sum of known contributions, which is only a partial sum when unknown > 0.
    pub known: u64,
    /// Number of selected observations whose value is unknown.
    pub unknown: u64,
}

impl UsageTotal {
    /// Add one original observation, preserving unknown values and refusing
    /// overflow before changing the preceding subtotal.
    pub fn push(&mut self, value: Option<u64>) -> Result<(), ModelPriceError> {
        *self = self.update(None, value)?;
        Ok(())
    }

    /// Complete value only when every selected observation was known. UIs must
    /// use this rather than present the partial `known` subtotal as a total.
    pub fn value(self) -> Option<u64> {
        (self.unknown == 0).then_some(self.known)
    }

    /// Combine disjoint subtotals without discarding unknown coverage.
    pub fn combine(self, other: Self) -> Result<Self, ModelPriceError> {
        Ok(Self {
            known: self
                .known
                .checked_add(other.known)
                .ok_or(ModelPriceError::Overflow)?,
            unknown: self
                .unknown
                .checked_add(other.unknown)
                .ok_or(ModelPriceError::Overflow)?,
        })
    }

    fn update(
        self,
        previous: Option<Option<u64>>,
        value: Option<u64>,
    ) -> Result<Self, ModelPriceError> {
        let mut next = self;
        match previous {
            Some(Some(value)) => {
                next.known = next
                    .known
                    .checked_sub(value)
                    .ok_or(ModelPriceError::Overflow)?
            }
            Some(None) => {
                next.unknown = next
                    .unknown
                    .checked_sub(1)
                    .ok_or(ModelPriceError::Overflow)?
            }
            None => {}
        }
        match value {
            Some(value) => {
                next.known = next
                    .known
                    .checked_add(value)
                    .ok_or(ModelPriceError::Overflow)?
            }
            None => {
                next.unknown = next
                    .unknown
                    .checked_add(1)
                    .ok_or(ModelPriceError::Overflow)?
            }
        }
        Ok(next)
    }
}

/// Incremental query projection for an optional currency and an explicitly selected
/// agent or tree. Consume each settled usage-file row once at its original
/// query frontier; this is not another journal or duplicate/replay detector.
/// Storage is fixed-size, independent of calls, history and children. A price
/// revision change replaces the affected query contribution, not old records.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelUsageTotals {
    /// Aggregate projection format, currently two.
    pub version: u32,
    /// One explicitly selected currency, or unknown. No conversion or automatic
    /// selection from incoming rows is inferred.
    pub currency: Option<String>,
    /// Selected original calls, including admitted calls without completion.
    pub calls: u64,
    /// Calls with an original committed terminal observation.
    pub completed: u64,
    /// Original input units; retain the provider's inclusive/exclusive convention.
    pub input: UsageTotal,
    /// Reported cache-read units, without adding them to input again.
    pub cached: UsageTotal,
    /// Reported cache-creation units.
    pub cache_write: UsageTotal,
    /// Original output units; retain the provider's reasoning convention.
    pub output: UsageTotal,
    /// Reported reasoning units, without adding them to output again.
    pub reasoning: UsageTotal,
    /// Estimated token charge only; never an invoice or account balance.
    pub token_nano_units: UsageTotal,
}

impl ModelUsageTotals {
    /// The single layout accepted by incremental and range projections.
    pub const VERSION: u32 = 2;

    /// Starts an empty query projection, taking ownership of its currency label.
    pub fn new(currency: String) -> Result<Self, ModelPriceError> {
        if !currency_valid(&currency) {
            return Err(ModelPriceError::InvalidRecord);
        }
        Ok(Self {
            currency: Some(currency),
            ..Self::unpriced()
        })
    }

    /// Starts a token-only query without choosing a currency. Even priced rows
    /// contribute unknown charges here; their original token units still count.
    /// This performs no allocation and never guesses a currency or price.
    pub fn unpriced() -> Self {
        Self {
            version: Self::VERSION,
            currency: None,
            calls: 0,
            completed: 0,
            input: UsageTotal::default(),
            cached: UsageTotal::default(),
            cache_write: UsageTotal::default(),
            output: UsageTotal::default(),
            reasoning: UsageTotal::default(),
            token_nano_units: UsageTotal::default(),
        }
    }

    /// All fallible arithmetic precedes installation, so a refusal preserves
    /// the preceding query result. No history scan, allocation or wait.
    pub fn push(&mut self, row: &ModelUsageProjection<'_>) -> Result<(), ModelPriceError> {
        self.update(None, row)
    }

    /// Replaces one contribution at the query owner's original frontier, for
    /// completion or a selected price revision change. No rescanning old calls.
    /// The owner must supply the exact previously installed contribution.
    pub fn replace(
        &mut self,
        previous: &ModelUsageProjection<'_>,
        row: &ModelUsageProjection<'_>,
    ) -> Result<(), ModelPriceError> {
        if !previous.usage.same_admission_as(&row.usage) {
            return Err(ModelPriceError::CallMismatch);
        }
        self.update(Some(previous), row)
    }

    /// Combine disjoint original selections at the range aggregate's admitted
    /// cut. This does not discover descendants, deduplicate inherited calls or
    /// authorize the selection: those belong to the original indexed query.
    /// Both selections must use the same explicit currency (or both unpriced).
    /// Fixed checked arithmetic precedes installation; no allocation, history
    /// scan, ancestor update, IO or wait occurs, including on refusal.
    pub fn merge(&mut self, other: &Self) -> Result<(), ModelPriceError> {
        self.validate()?;
        other.validate()?;
        if self.currency != other.currency {
            return Err(ModelPriceError::CurrencyMismatch);
        }
        let calls = self
            .calls
            .checked_add(other.calls)
            .ok_or(ModelPriceError::Overflow)?;
        let completed = self
            .completed
            .checked_add(other.completed)
            .ok_or(ModelPriceError::Overflow)?;
        let input = self.input.combine(other.input)?;
        let cached = self.cached.combine(other.cached)?;
        let cache_write = self.cache_write.combine(other.cache_write)?;
        let output = self.output.combine(other.output)?;
        let reasoning = self.reasoning.combine(other.reasoning)?;
        let token_nano_units = self.token_nano_units.combine(other.token_nano_units)?;
        self.calls = calls;
        self.completed = completed;
        self.input = input;
        self.cached = cached;
        self.cache_write = cache_write;
        self.output = output;
        self.reasoning = reasoning;
        self.token_nano_units = token_nano_units;
        Ok(())
    }

    /// Validate one original aggregate at the live/range projection boundary.
    /// Known subtotals remain partial whenever their unknown count is nonzero.
    pub fn validate(&self) -> Result<(), ModelPriceError> {
        self.validate_header()?;
        if self.completed > self.calls
            || (self.currency.is_none()
                && (self.token_nano_units.known != 0
                    || self.token_nano_units.unknown != self.calls))
            || [
                self.input,
                self.cached,
                self.cache_write,
                self.output,
                self.reasoning,
                self.token_nano_units,
            ]
            .iter()
            .any(|value| {
                value.unknown > self.calls
                    || value.unknown < self.calls - self.completed
                    || (value.unknown == self.calls && value.known != 0)
            })
        {
            return Err(ModelPriceError::InvalidRecord);
        }
        Ok(())
    }

    fn validate_header(&self) -> Result<(), ModelPriceError> {
        if self.version != Self::VERSION
            || self
                .currency
                .as_deref()
                .is_some_and(|currency| !currency_valid(currency))
        {
            return Err(ModelPriceError::InvalidRecord);
        }
        Ok(())
    }

    fn update(
        &mut self,
        previous: Option<&ModelUsageProjection<'_>>,
        row: &ModelUsageProjection<'_>,
    ) -> Result<(), ModelPriceError> {
        self.validate_header()?;
        if row.version != ModelUsageProjection::VERSION
            || row.usage.version != ModelUsageRecord::VERSION
            || previous.is_some_and(|old| {
                old.version != ModelUsageProjection::VERSION
                    || old.usage.version != ModelUsageRecord::VERSION
            })
        {
            return Err(ModelPriceError::InvalidRecord);
        }
        if let Some(selected) = self.currency.as_deref() {
            if row
                .estimated_cost
                .currency
                .as_deref()
                .is_some_and(|currency| currency != selected)
                || previous.is_some_and(|old| {
                    old.estimated_cost
                        .currency
                        .as_deref()
                        .is_some_and(|currency| currency != selected)
                })
            {
                return Err(ModelPriceError::CurrencyMismatch);
            }
        }
        let calls = self
            .calls
            .checked_add(u64::from(previous.is_none()))
            .ok_or(ModelPriceError::Overflow)?;
        let completed = self
            .completed
            .checked_sub(u64::from(previous.is_some_and(|old| old.usage.completed)))
            .and_then(|count| count.checked_add(u64::from(row.usage.completed)))
            .ok_or(ModelPriceError::Overflow)?;
        let priced = self.currency.is_some();
        let (tokens, cost) = row.contribution(priced);
        let old = previous.map(|row| row.contribution(priced));
        let old_tokens = old.map(|(tokens, _)| tokens);
        let input = self
            .input
            .update(old_tokens.map(|tokens| tokens.input), tokens.input)?;
        let cached = self
            .cached
            .update(old_tokens.map(|tokens| tokens.cached), tokens.cached)?;
        let cache_write = self.cache_write.update(
            old_tokens.map(|tokens| tokens.cache_write),
            tokens.cache_write,
        )?;
        let output = self
            .output
            .update(old_tokens.map(|tokens| tokens.output), tokens.output)?;
        let reasoning = self
            .reasoning
            .update(old_tokens.map(|tokens| tokens.reasoning), tokens.reasoning)?;
        let cost = self
            .token_nano_units
            .update(old.map(|(_, cost)| cost), cost)?;
        self.calls = calls;
        self.completed = completed;
        self.input = input;
        self.cached = cached;
        self.cache_write = cache_write;
        self.output = output;
        self.reasoning = reasoning;
        self.token_nano_units = cost;
        Ok(())
    }
}

#[cfg(kani)]
mod proofs {
    use super::*;

    #[kani::proof]
    fn contribution_replacement_conserves_full_width_counts() {
        let total = UsageTotal {
            known: kani::any(),
            unknown: kani::any(),
        };
        let previous: Option<Option<u64>> = kani::any();
        let value: Option<u64> = kani::any();
        // Wider signed arithmetic independently detects underflow before the
        // new contribution is added, including a replacement that would hide it.
        let removed_known = i128::from(total.known) - i128::from(previous.flatten().unwrap_or(0));
        let removed_unknown =
            i128::from(total.unknown) - i128::from(previous.is_some_and(|prior| prior.is_none()));
        let known = removed_known + i128::from(value.unwrap_or(0));
        let unknown = removed_unknown + i128::from(value.is_none());
        let representable = removed_known >= 0
            && removed_unknown >= 0
            && known <= i128::from(u64::MAX)
            && unknown <= i128::from(u64::MAX);
        let result = total.update(previous, value);
        assert_eq!(result.is_ok(), representable);
        if let Ok(next) = result {
            assert_eq!(i128::from(next.known), known);
            assert_eq!(i128::from(next.unknown), unknown);
            if let Some(prior) = previous {
                // Removing exactly the admitted replacement restores both
                // counters; counting either value twice violates this oracle.
                assert_eq!(next.update(Some(value), prior), Ok(total));
            }
        } else {
            assert_eq!(result, Err(ModelPriceError::Overflow));
        }
    }
}
