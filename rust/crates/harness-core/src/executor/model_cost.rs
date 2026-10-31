//! A price-selected estimate over the original committed usage, never an invoice.
use super::{ModelTokenUsage, ModelUsageRecord};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;

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

/// Version-two usage-file row. The original version-one journal record is
/// unchanged, including its dispatch identity and reported counter conventions.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelUsageProjection<'a> {
    /// Usage-file format, currently two; distinct from the journal record format.
    pub version: u32,
    /// Original journal record, borrowed while writing and owned after decoding.
    pub usage: Cow<'a, ModelUsageRecord>,
    /// Derived token estimate and the selected price provenance.
    pub estimated_cost: ModelCostEstimate<'a>,
}

fn currency_valid(currency: &str) -> bool {
    currency.len() == 3 && currency.bytes().all(|byte| byte.is_ascii_uppercase())
}

fn charge(tokens: u64, rate: Option<u64>) -> Option<u128> {
    // Known zero units cost zero even when their unit price is unknown.
    Some(u128::from(tokens) * u128::from(if tokens == 0 { 0 } else { rate? }))
}

fn family_cost(
    parent: Option<u64>,
    rate: Option<u64>,
    children: &[(Option<u64>, TokenPrice)],
) -> Option<u128> {
    let mut parent = parent?;
    let mut inclusive = parent;
    let mut extra = 0u128;
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
                extra = extra.checked_add(charge(tokens, nano_units_per_million)?)?;
            }
            TokenPrice::Additional {
                nano_units_per_million,
            } => {
                extra = extra.checked_add(charge(tokens?, nano_units_per_million)?)?;
            }
        }
    }
    charge(parent, rate)?.checked_add(extra)
}

impl ModelUsageRecord {
    /// Projects one original row against one explicitly selected immutable
    /// price. Constant work, zero allocation/copy/IO/wait. A mismatched binding
    /// is rejected rather than silently priced as another model.
    pub fn estimate<'a>(
        &'a self,
        price: Option<&'a ModelPriceRecord>,
    ) -> Result<ModelUsageProjection<'a>, ModelPriceError> {
        let mut estimated_cost = ModelCostEstimate {
            price_revision: None,
            effective_at: None,
            currency: None,
            token_nano_units: None,
        };
        if let Some(price) = price {
            if price.version != 1 || price.revision.is_empty() || !currency_valid(&price.currency) {
                return Err(ModelPriceError::InvalidRecord);
            }
            if price.provider != self.provider || price.model != self.model {
                return Err(ModelPriceError::BindingMismatch);
            }
            estimated_cost.price_revision = Some(Cow::Borrowed(&price.revision));
            estimated_cost.effective_at = Some(price.effective_at);
            estimated_cost.currency = Some(Cow::Borrowed(&price.currency));
            estimated_cost.token_nano_units = self
                .completed
                .then(|| {
                    let input = family_cost(
                        self.tokens.input,
                        price.input_nano_units_per_million,
                        &[
                            (self.tokens.cached, price.cached),
                            (self.tokens.cache_write, price.cache_write),
                        ],
                    )?;
                    let output = family_cost(
                        self.tokens.output,
                        price.output_nano_units_per_million,
                        &[(self.tokens.reasoning, price.reasoning)],
                    )?;
                    u64::try_from(input.checked_add(output)?.div_ceil(1_000_000)).ok()
                })
                .flatten();
        }
        Ok(ModelUsageProjection {
            version: 2,
            usage: Cow::Borrowed(self),
            estimated_cost,
        })
    }
}

/// Partial sum with explicit unknown observations. An empty selection has a
/// zero sum and zero unknowns; it is distinct from one unpriced call.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UsageTotal {
    /// Sum of known contributions, which is only a partial sum when unknown > 0.
    pub known: u64,
    /// Number of selected observations whose value is unknown.
    pub unknown: u64,
}

impl UsageTotal {
    /// Complete value only when every selected observation was known. UIs must
    /// use this rather than present the partial `known` subtotal as a total.
    pub fn value(self) -> Option<u64> {
        (self.unknown == 0).then_some(self.known)
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

/// Incremental query projection for one currency and an explicitly selected
/// agent or tree. Consume each settled usage-file row once at its original
/// query frontier; this is not another journal or duplicate/replay detector.
/// Storage is fixed-size, independent of calls, history and children. A price
/// revision change replaces the affected query contribution, not old records.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelUsageTotals {
    /// Aggregate projection format, currently one.
    pub version: u32,
    /// One explicitly selected currency; no conversion is inferred.
    pub currency: String,
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
    /// Starts an empty query projection, taking ownership of its currency label.
    pub fn new(currency: String) -> Result<Self, ModelPriceError> {
        if !currency_valid(&currency) {
            return Err(ModelPriceError::InvalidRecord);
        }
        Ok(Self {
            version: 1,
            currency,
            calls: 0,
            completed: 0,
            input: UsageTotal::default(),
            cached: UsageTotal::default(),
            cache_write: UsageTotal::default(),
            output: UsageTotal::default(),
            reasoning: UsageTotal::default(),
            token_nano_units: UsageTotal::default(),
        })
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
        if previous.usage.dispatch != row.usage.dispatch
            || previous.usage.purpose != row.usage.purpose
            || previous.usage.provider != row.usage.provider
            || previous.usage.model != row.usage.model
        {
            return Err(ModelPriceError::CallMismatch);
        }
        self.update(Some(previous), row)
    }

    fn update(
        &mut self,
        previous: Option<&ModelUsageProjection<'_>>,
        row: &ModelUsageProjection<'_>,
    ) -> Result<(), ModelPriceError> {
        if row.version != 2
            || row.usage.version != 1
            || previous.is_some_and(|old| old.version != 2 || old.usage.version != 1)
        {
            return Err(ModelPriceError::InvalidRecord);
        }
        if row
            .estimated_cost
            .currency
            .as_deref()
            .is_some_and(|currency| currency != self.currency)
        {
            return Err(ModelPriceError::CurrencyMismatch);
        }
        if previous.is_some_and(|old| {
            old.estimated_cost
                .currency
                .as_deref()
                .is_some_and(|currency| currency != self.currency)
        }) {
            return Err(ModelPriceError::CurrencyMismatch);
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
        let tokens = if row.usage.completed {
            row.usage.tokens
        } else {
            ModelTokenUsage::default()
        };
        let old = previous.map(|row| {
            if row.usage.completed {
                row.usage.tokens
            } else {
                ModelTokenUsage::default()
            }
        });
        let input = self
            .input
            .update(old.map(|tokens| tokens.input), tokens.input)?;
        let cached = self
            .cached
            .update(old.map(|tokens| tokens.cached), tokens.cached)?;
        let cache_write = self
            .cache_write
            .update(old.map(|tokens| tokens.cache_write), tokens.cache_write)?;
        let output = self
            .output
            .update(old.map(|tokens| tokens.output), tokens.output)?;
        let reasoning = self
            .reasoning
            .update(old.map(|tokens| tokens.reasoning), tokens.reasoning)?;
        let cost = self.token_nano_units.update(
            previous.map(|row| row.estimated_cost.token_nano_units),
            row.estimated_cost.token_nano_units,
        )?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn record(tokens: ModelTokenUsage) -> ModelUsageRecord {
        ModelUsageRecord {
            version: 1,
            completed: true,
            dispatch: crate::model::ModelDispatch {
                operation_id: crate::OperationId::from_bytes([31; 16]),
                step: 0,
                request_digest: [7; 32],
            },
            purpose: crate::executor::ModelPurpose::Response,
            provider: "generated-provider".into(),
            model: "generated-model".into(),
            agent_path: Some("/selected-agent".into()),
            tokens,
            latency_ns: None,
        }
    }

    fn price() -> ModelPriceRecord {
        // Generated prices for arithmetic controls, never provider price data.
        ModelPriceRecord {
            version: 1,
            revision: "generated-1".into(),
            provider: "generated-provider".into(),
            model: "generated-model".into(),
            effective_at: 1,
            currency: "USD".into(),
            input_nano_units_per_million: Some(11),
            output_nano_units_per_million: Some(23),
            cached: TokenPrice::Subset {
                nano_units_per_million: Some(3),
            },
            cache_write: TokenPrice::Subset {
                nano_units_per_million: Some(17),
            },
            reasoning: TokenPrice::Included,
        }
    }

    proptest! {
        #[test]
        fn selected_prices_preserve_units_unknowns_and_incremental_replacements(
            input in 1u64..1_000_000, cached in 1u64..1_000_000,
            written in 1u64..1_000_000, output in 1u64..1_000_000,
            reasoning in 1u64..1_000_000,
        ) {
            let row = record(ModelTokenUsage {
                input: Some(input + cached + written), cached: Some(cached), cache_write: Some(written),
                output: Some(output + reasoning), reasoning: Some(reasoning),
            });
            let inclusive = price();
            let projected = row.estimate(Some(&inclusive)).unwrap();
            let expected = (input * 11 + cached * 3 + written * 17 + (output + reasoning) * 23).div_ceil(1_000_000);
            prop_assert_eq!(projected.estimated_cost.token_nano_units, Some(expected));
            let mut exclusive = inclusive.clone();
            exclusive.cached = TokenPrice::Additional { nano_units_per_million: Some(3) };
            exclusive.cache_write = TokenPrice::Additional { nano_units_per_million: Some(17) };
            exclusive.reasoning = TokenPrice::Additional { nano_units_per_million: Some(23) };
            let mut other = row.clone();
            other.tokens.input = Some(input);
            other.tokens.output = Some(output);
            prop_assert_eq!(other.estimate(Some(&exclusive)).unwrap().estimated_cost.token_nano_units, Some(expected));

            // An admitted call has unknown counters even if a caller supplies numbers.
            let mut admitted = row.clone();
            admitted.completed = false;
            let pending = admitted.estimate(Some(&inclusive)).unwrap();
            let mut total = ModelUsageTotals::new("USD".into()).unwrap();
            total.push(&pending).unwrap();
            prop_assert_eq!(total.input, UsageTotal { known: 0, unknown: 1 });
            total.replace(&pending, &projected).unwrap();
            prop_assert_eq!((total.calls, total.completed), (1, 1));
            prop_assert_eq!(total.token_nano_units, UsageTotal { known: expected, unknown: 0 });
            let unknown = row.estimate(None).unwrap();
            total.replace(&projected, &unknown).unwrap();
            prop_assert_eq!(total.token_nano_units, UsageTotal { known: 0, unknown: 1 });
            total.replace(&unknown, &projected).unwrap();
            let snapshot = total.clone();
            // Replacing an installed projection with itself is a fixed point.
            total.replace(&projected, &projected).unwrap();
            prop_assert_eq!(&total, &snapshot);
            let mut foreign = row.clone();
            foreign.dispatch.step = 1;
            prop_assert_eq!(total.replace(&projected, &foreign.estimate(None).unwrap()), Err(ModelPriceError::CallMismatch));
            prop_assert_eq!(&total, &snapshot);

            let mut missing = row.clone();
            missing.tokens.cached = None;
            prop_assert_eq!(missing.estimate(Some(&inclusive)).unwrap().estimated_cost.token_nano_units, None);
            missing.tokens.cached = Some(input + cached + written + 1);
            prop_assert_eq!(missing.estimate(Some(&inclusive)).unwrap().estimated_cost.token_nano_units, None);
            let mut missing_rate = inclusive.clone();
            missing_rate.cached = TokenPrice::Subset { nano_units_per_million: None };
            prop_assert_eq!(row.estimate(Some(&missing_rate)).unwrap().estimated_cost.token_nano_units, None);
            let mut foreign_price = inclusive.clone();
            foreign_price.model.push('x');
            prop_assert!(matches!(row.estimate(Some(&foreign_price)), Err(ModelPriceError::BindingMismatch)));
            foreign_price = inclusive.clone();
            foreign_price.version = 2;
            prop_assert!(matches!(row.estimate(Some(&foreign_price)), Err(ModelPriceError::InvalidRecord)));

            let mut revised = inclusive.clone();
            revised.revision = "generated-2".into();
            revised.effective_at = 2;
            revised.currency = "EUR".into();
            prop_assert_eq!(total.push(&row.estimate(Some(&revised)).unwrap()), Err(ModelPriceError::CurrencyMismatch));
            prop_assert_eq!(&total, &snapshot);
            revised.currency = "USD".into();
            revised.input_nano_units_per_million = Some(29);
            let updated = row.estimate(Some(&revised)).unwrap();
            total.replace(&projected, &updated).unwrap();
            prop_assert_eq!(total.token_nano_units.known, (input * 29 + cached * 3 + written * 17 + (output + reasoning) * 23).div_ceil(1_000_000));
            prop_assert_eq!(projected.estimated_cost.price_revision.as_deref(), Some("generated-1"));
            prop_assert_eq!(updated.estimated_cost.price_revision.as_deref(), Some("generated-2"));
            let allocations = acyclic_test_support::measure(|| {
                let same = std::hint::black_box(row.estimate(Some(&revised)).unwrap());
                total.replace(&same, &same).unwrap();
            });
            prop_assert_eq!((allocations.count_total, allocations.bytes_total), (0, 0));
            let copied = acyclic_test_support::measure(|| { std::hint::black_box(row.clone()); });
            prop_assert!(copied.count_total > 0);

            let mut no_reasoning = row.clone();
            no_reasoning.tokens.reasoning = None;
            prop_assert_eq!(no_reasoning.estimate(Some(&inclusive)).unwrap().estimated_cost.token_nano_units, Some(expected));
            let mut contradictory = row.clone();
            contradictory.tokens.reasoning = Some(output + reasoning + 1);
            prop_assert_eq!(contradictory.estimate(Some(&inclusive)).unwrap().estimated_cost.token_nano_units, None);
            let mut included_cache = inclusive.clone();
            included_cache.cached = TokenPrice::Included;
            contradictory = row.clone();
            // Each count fits independently; together these disjoint subsets do not.
            contradictory.tokens.cached = Some(input + cached + 1);
            prop_assert_eq!(contradictory.estimate(Some(&included_cache)).unwrap().estimated_cost.token_nano_units, None);
            included_cache.cache_write = TokenPrice::Included;
            prop_assert_eq!(contradictory.estimate(Some(&included_cache)).unwrap().estimated_cost.token_nano_units, None);
            contradictory.tokens.cached = None;
            prop_assert!(contradictory.estimate(Some(&included_cache)).unwrap().estimated_cost.token_nano_units.is_some());
            let zero = record(ModelTokenUsage { input: Some(0), output: Some(0), ..Default::default() });
            let mut zero_price = inclusive.clone();
            zero_price.input_nano_units_per_million = None;
            zero_price.output_nano_units_per_million = None;
            zero_price.cached = TokenPrice::Included;
            zero_price.cache_write = TokenPrice::Included;
            prop_assert_eq!(zero.estimate(Some(&zero_price)).unwrap().estimated_cost.token_nano_units, Some(0));
            prop_assert_eq!(zero.estimate(None).unwrap().estimated_cost.token_nano_units, None);

            // Overflow is unknown for a call, and cannot corrupt an aggregate.
            let huge = record(ModelTokenUsage { input: Some(u64::MAX), output: Some(u64::MAX), ..Default::default() });
            let mut huge_price = inclusive.clone();
            huge_price.input_nano_units_per_million = Some(u64::MAX);
            huge_price.output_nano_units_per_million = Some(u64::MAX);
            huge_price.cached = TokenPrice::Included;
            huge_price.cache_write = TokenPrice::Included;
            prop_assert_eq!(huge.estimate(Some(&huge_price)).unwrap().estimated_cost.token_nano_units, None);
            total.input.known = u64::MAX;
            let snapshot = total.clone();
            prop_assert_eq!(total.push(&projected), Err(ModelPriceError::Overflow));
            prop_assert_eq!(&total, &snapshot);

            // Raw units, absence and selected price identity survive the file projection.
            let value = serde_json::to_value(&projected).unwrap();
            prop_assert_eq!(&value["usage"]["tokens"], &serde_json::to_value(row.tokens).unwrap());
            prop_assert!(value["usage"]["latency_ns"].is_null());
            prop_assert_eq!(&value["estimated_cost"]["price_revision"], "generated-1");
            let decoded: ModelUsageProjection<'static> = serde_json::from_value(value).unwrap();
            prop_assert_eq!(decoded.usage.tokens, row.tokens);
            let mut decoded_total = ModelUsageTotals::new("USD".into()).unwrap();
            decoded_total.push(&decoded).unwrap();
            prop_assert_eq!(decoded_total.token_nano_units.value(), Some(expected));
        }
    }
}
