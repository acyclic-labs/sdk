use acyclic_harness_core::{
    OperationId,
    executor::{
        ModelPriceError, ModelPriceRecord, ModelPrices, ModelPurpose, ModelTokenUsage,
        ModelUsageProjection, ModelUsageRecord, ModelUsageTotals, TokenPrice, UsageTotal,
    },
    model::{ModelDispatch, ModelSampling},
};
use proptest::prelude::*;

fn record(tokens: ModelTokenUsage) -> ModelUsageRecord {
    ModelUsageRecord {
        version: ModelUsageRecord::VERSION,
        completed: true,
        dispatch: ModelDispatch {
            operation_id: OperationId::from_bytes([31; 16]),
            step: 0,
            request_digest: [7; 32],
        },
        purpose: ModelPurpose::Response,
        provider: "generated-provider".into(),
        model: "generated-model".into(),
        agent_path: Some("/selected-agent".into()),
        sampling: None,
        tokens,
        complete_input_tokens: None,
        started_at: None,
        completed_at: None,
        latency_ns: None,
        provider_request_id: None,
        system_fingerprint: None,
        ttft_ns: None,
        cache: None,
        abandoned: Some(Default::default()),
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

#[test]
fn selected_price_files_refuse_ambiguity_and_preserve_unknown_calls()
-> Result<(), Box<dyn std::error::Error>> {
    let rate = price();
    let text = serde_json::to_vec(&rate)?;
    let selected = ModelPrices::from_jsonl(&text)?;
    let mut usage = record(ModelTokenUsage {
        input: Some(1_000_000),
        output: Some(2_000_000),
        cached: Some(0),
        cache_write: Some(0),
        reasoning: Some(0),
    });
    let mut total = selected.totals();
    total.push(&usage.estimate(selected.price(&usage))?)?;
    assert_eq!(total.token_nano_units.value(), Some(57));
    // A different admitted binding never receives this model's price.
    usage.model.push_str("-other");
    usage.dispatch.step += 1;
    total.push(&usage.estimate(selected.price(&usage))?)?;
    assert_eq!(total.token_nano_units.known, 57);
    assert_eq!(total.token_nano_units.unknown, 1);
    assert_eq!(total.token_nano_units.value(), None);
    let mut duplicate = text.clone();
    duplicate.push(b'\n');
    duplicate.extend(&text);
    assert!(ModelPrices::from_jsonl(&duplicate).is_err());
    let mut foreign = rate.clone();
    foreign.model.push_str("-other");
    foreign.currency = "EUR".into();
    assert!(matches!(
        ModelPrices::new([rate.clone(), foreign]),
        Err(ModelPriceError::CurrencyMismatch)
    ));
    let mut unsupported = rate;
    unsupported.version = 2;
    assert!(ModelPrices::from_jsonl(&serde_json::to_vec(&unsupported)?).is_err());
    Ok(())
}

#[test]
fn saved_cost_annotations_require_completion_and_selected_currency()
-> Result<(), Box<dyn std::error::Error>> {
    let usage = record(ModelTokenUsage {
        input: Some(1_000_000),
        output: Some(0),
        cached: Some(0),
        cache_write: Some(0),
        reasoning: Some(0),
    });
    let price = price();
    let complete = usage.estimate(Some(&price))?;
    for completed in [false, true] {
        let mut saved = serde_json::to_value(&complete)?;
        *field(&mut saved, "/usage/completed")? = completed.into();
        *field(&mut saved, "/estimated_cost/token_nano_units")? = 7.into();
        if completed {
            *field(&mut saved, "/estimated_cost/currency")? = serde_json::Value::Null;
        }
        let annotated: ModelUsageProjection<'static> = serde_json::from_value(saved)?;
        let mut totals = ModelUsageTotals::new("USD".into())?;
        totals.push(&annotated)?;
        let unknown = UsageTotal {
            known: 0,
            unknown: 1,
        };
        assert_eq!(
            totals.token_nano_units, unknown,
            "saved annotation invented a known cost"
        );
        totals.validate()?;
        totals.replace(&annotated, &complete)?;
        assert_eq!(
            totals.token_nano_units.value(),
            complete.estimated_cost.token_nano_units
        );
        totals.validate()?;
        totals.replace(&complete, &annotated)?;
        assert_eq!(totals.token_nano_units, unknown);
        totals.validate()?;
    }
    Ok(())
}

use proptest::test_runner::TestCaseError;

fn case_error(error: impl std::fmt::Display) -> TestCaseError {
    TestCaseError::fail(error.to_string())
}

fn field<'a>(
    document: &'a mut serde_json::Value,
    pointer: &str,
) -> Result<&'a mut serde_json::Value, std::io::Error> {
    document.pointer_mut(pointer).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("fixture missing {pointer}"),
        )
    })
}

proptest! {
    #[test]
    fn disjoint_usage_ranges_merge_without_losing_unknowns_or_partial_installation(
        units in proptest::array::uniform3(0u64..1_000_000),
        completed in proptest::array::uniform3(any::<bool>()),
        known in proptest::array::uniform3(any::<bool>()),
    ) {
        let regroup = |[first, second, third]: &[ModelUsageTotals; 3]| {
            let mut left = first.clone();
            left.merge(second)?;
            left.merge(third)?;
            let mut right = second.clone();
            right.merge(third)?;
            let mut regrouped = first.clone();
            regrouped.merge(&right)?;
            Ok::<_, ModelPriceError>((left, regrouped))
        };
        let rows = [0u32, 1, 2].into_iter().zip(units).zip(known).zip(completed)
            .map(|(((step, units), known), completed)| {
                let mut row = record(ModelTokenUsage {
                    input: known.then_some(units),
                    output: Some(units),
                    ..Default::default()
                });
                row.dispatch.step = step;
                row.completed = completed;
                row
            });
        let mut parts = std::array::from_fn::<_, 3, _>(|_| ModelUsageTotals::unpriced());
        let mut reference = ModelUsageTotals::unpriced();
        for (part, row) in parts.iter_mut().zip(rows) {
            let projected = row.estimate(None).map_err(case_error)?;
            part.push(&projected).map_err(case_error)?;
            reference.push(&projected).map_err(case_error)?;
        }
        let (left, regrouped) = regroup(&parts).map_err(case_error)?;
        prop_assert_eq!(&left, &reference);
        prop_assert_eq!(&regrouped, &reference);
        prop_assert_eq!(reference.input.value(), if completed.iter().all(|v| *v) && known.iter().all(|v| *v) {
            Some(units.iter().sum())
        } else { None });

        // Generated arithmetic operands force a known positive charge and
        // an unknown charge in every selection, not merely by chance.
        let priced_rows = [0u32, 1, 2].into_iter().zip(units).map(|(step, units)| {
            let mut row = record(ModelTokenUsage {
                input: Some(units + 1),
                output: Some(units),
                cached: Some(0),
                cache_write: Some(0),
                ..Default::default()
            });
            row.dispatch.step = step;
            row
        });
        let selected_price = price();
        let priced_empty = ModelUsageTotals::new("USD".into()).map_err(case_error)?;
        let mut priced_parts = [priced_empty.clone(), priced_empty.clone(), priced_empty];
        let mut priced_reference = ModelUsageTotals::new("USD".into()).map_err(case_error)?;
        let [.., last_known] = known;
        for (index, (part, row)) in priced_parts.iter_mut().zip(priced_rows).enumerate() {
            let price = (index == 0 || (index == 2 && last_known)).then_some(&selected_price);
            let projected = row.estimate(price).map_err(case_error)?;
            part.push(&projected).map_err(case_error)?;
            priced_reference.push(&projected).map_err(case_error)?;
        }
        let (priced_left, priced_regrouped) = regroup(&priced_parts).map_err(case_error)?;
        prop_assert_eq!(&priced_left, &priced_reference);
        prop_assert_eq!(&priced_regrouped, &priced_reference);
        prop_assert!(priced_reference.token_nano_units.known > 0);
        prop_assert!(priced_reference.token_nano_units.unknown > 0);
        prop_assert_eq!(priced_reference.token_nano_units.value(), None);

        let unchanged = reference.clone();
        let mut foreign_version = reference.clone();
        foreign_version.version ^= 1;
        let mut foreign_count = reference.clone();
        foreign_count.completed = foreign_count.calls + 1;
        let mut foreign_unknown = reference.clone();
        foreign_unknown.input.unknown = foreign_unknown.calls + 1;
        for invalid in [foreign_version, foreign_count, foreign_unknown] {
            // Every refusal preserves the original selected contribution.
            prop_assert_eq!(reference.merge(&invalid), Err(ModelPriceError::InvalidRecord));
            prop_assert_eq!(&reference, &unchanged);
        }
        let priced = ModelUsageTotals::new("USD".into()).map_err(case_error)?;
        prop_assert_eq!(reference.merge(&priced), Err(ModelPriceError::CurrencyMismatch));
        prop_assert_eq!(&reference, &unchanged);
        let [first, ..] = &parts;
        let mut forged = first.clone();
        forged.token_nano_units.known = 1;
        prop_assert_eq!(reference.merge(&forged), Err(ModelPriceError::InvalidRecord));
        prop_assert_eq!(&reference, &unchanged);
        // A late counter overflow must not install earlier field additions.
        let mut maximum = ModelUsageTotals::new("USD".into()).map_err(case_error)?;
        maximum.calls = 1;
        maximum.completed = 1;
        maximum.token_nano_units.known = u64::MAX;
        let mut one = maximum.clone();
        one.token_nano_units.known = 1;
        let snapshot = maximum.clone();
        prop_assert_eq!(maximum.merge(&one), Err(ModelPriceError::Overflow));
        prop_assert_eq!(&maximum, &snapshot);
        for calls in [1, 100, 100_000] {
            let mut target = ModelUsageTotals::unpriced();
            target.calls = calls;
            target.completed = calls;
            target.token_nano_units.unknown = calls;
            let allocations = acyclic_test_support::measure(|| target.merge(first));
            allocations.output.map_err(case_error)?;
            prop_assert_eq!((allocations.count_total, allocations.bytes_total), (0, 0));
        }
        let negative = acyclic_test_support::measure(|| {
            std::hint::black_box(priced.clone());
        });
        prop_assert!(negative.count_total > 0);
    }

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
        let projected = row.estimate(Some(&inclusive)).map_err(case_error)?;
        let savings = (i128::from(cached) * 8 - i128::from(written) * 6) / 1_000_000;
        prop_assert_eq!(row.cache_savings(&inclusive).map_err(case_error)?, Some(savings));
        let expected = (input * 11 + cached * 3 + written * 17 + (output + reasoning) * 23).div_ceil(1_000_000);
        prop_assert_eq!(projected.estimated_cost.token_nano_units, Some(expected));
        let mut exclusive = inclusive.clone();
        exclusive.cached = TokenPrice::Additional { nano_units_per_million: Some(3) };
        exclusive.cache_write = TokenPrice::Additional { nano_units_per_million: Some(17) };
        exclusive.reasoning = TokenPrice::Additional { nano_units_per_million: Some(23) };
        let mut other = row.clone();
        other.tokens.input = Some(input);
        other.tokens.output = Some(output);
        prop_assert_eq!(other.estimate(Some(&exclusive)).map_err(case_error)?.estimated_cost.token_nano_units, Some(expected));
        prop_assert_eq!(other.cache_savings(&exclusive).map_err(case_error)?, Some(savings));

        // An admitted call has unknown counters even if a caller supplies numbers.
        let mut admitted = row.clone();
        admitted.completed = false;
        let pending = admitted.estimate(Some(&inclusive)).map_err(case_error)?;
        prop_assert_eq!(admitted.cache_savings(&inclusive).map_err(case_error)?, None);
        // Saved annotations cannot supply completion or price authority.
        // Exercise the same persisted projection entrance used on replay.
        let mut saved = serde_json::to_value(&pending).map_err(case_error)?;
        *field(&mut saved, "/estimated_cost/token_nano_units").map_err(case_error)? = serde_json::json!(expected);
        let annotated: ModelUsageProjection<'static> = serde_json::from_value(saved).map_err(case_error)?;
        prop_assert_eq!(annotated.estimated_cost.token_nano_units, Some(expected));
        let mut retained = ModelUsageTotals::new("USD".into()).map_err(case_error)?;
        retained.push(&annotated).map_err(case_error)?;
        prop_assert_eq!(retained.token_nano_units, UsageTotal { known: 0, unknown: 1 });
        retained.validate().map_err(case_error)?;
        retained.replace(&annotated, &projected).map_err(case_error)?;
        prop_assert_eq!(retained.token_nano_units.value(), Some(expected));
        let mut unselected = projected.clone();
        unselected.estimated_cost.currency = None;
        retained.replace(&projected, &unselected).map_err(case_error)?;
        prop_assert_eq!(retained.token_nano_units, UsageTotal { known: 0, unknown: 1 });
        retained.replace(&unselected, &projected).map_err(case_error)?;
        prop_assert_eq!(retained.token_nano_units.value(), Some(expected));
        let mut unpriced = ModelUsageTotals::unpriced();
        unpriced.push(&pending).map_err(case_error)?;
        unpriced.replace(&pending, &projected).map_err(case_error)?;
        prop_assert_eq!(unpriced.currency.as_deref(), None);
        prop_assert_eq!(unpriced.input.value(), row.tokens.input);
        prop_assert_eq!(unpriced.output.value(), row.tokens.output);
        prop_assert_eq!(unpriced.token_nano_units, UsageTotal { known: 0, unknown: 1 });
        let mut total = ModelUsageTotals::new("USD".into()).map_err(case_error)?;
        total.push(&pending).map_err(case_error)?;
        prop_assert_eq!(total.input, UsageTotal { known: 0, unknown: 1 });
        total.replace(&pending, &projected).map_err(case_error)?;
        prop_assert_eq!((total.calls, total.completed), (1, 1));
        prop_assert_eq!(total.token_nano_units, UsageTotal { known: expected, unknown: 0 });
        let unknown = row.estimate(None).map_err(case_error)?;
        total.replace(&projected, &unknown).map_err(case_error)?;
        prop_assert_eq!(total.token_nano_units, UsageTotal { known: 0, unknown: 1 });
        total.replace(&unknown, &projected).map_err(case_error)?;
        let snapshot = total.clone();
        // Replacing an installed projection with itself is a fixed point.
        total.replace(&projected, &projected).map_err(case_error)?;
        prop_assert_eq!(&total, &snapshot);
        let mut foreign = row.clone();
        foreign.dispatch.step = 1;
        prop_assert_eq!(total.replace(&projected, &foreign.estimate(None).map_err(case_error)?), Err(ModelPriceError::CallMismatch));
        prop_assert_eq!(&total, &snapshot);

        // The old dispatch/provider-only comparison accepted substituted
        // agent or pinned request observations. Each must refuse before
        // replacing any installed contribution, without retaining a row.
        for field in 0..2 {
            let mut foreign = row.clone();
            match field {
                0 => foreign.agent_path = None,
                _ => foreign.sampling = Some(ModelSampling::default()),
            }
            prop_assert!(row.dispatch == foreign.dispatch
                && row.purpose == foreign.purpose
                && row.provider == foreign.provider
                && row.model == foreign.model);
            prop_assert_eq!(total.replace(&projected, &foreign.estimate(None).map_err(case_error)?), Err(ModelPriceError::CallMismatch));
            prop_assert_eq!(&total, &snapshot);
        }

        let mut missing = row.clone();
        missing.tokens.cached = None;
        prop_assert_eq!(missing.cache_savings(&inclusive).map_err(case_error)?, None);
        prop_assert_eq!(missing.estimate(Some(&inclusive)).map_err(case_error)?.estimated_cost.token_nano_units, None);
        missing.tokens.cached = Some(input + cached + written + 1);
        prop_assert_eq!(missing.estimate(Some(&inclusive)).map_err(case_error)?.estimated_cost.token_nano_units, None);
        let mut missing_rate = inclusive.clone();
        missing_rate.cached = TokenPrice::Subset { nano_units_per_million: None };
        prop_assert_eq!(row.estimate(Some(&missing_rate)).map_err(case_error)?.estimated_cost.token_nano_units, None);
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
        prop_assert_eq!(total.push(&row.estimate(Some(&revised)).map_err(case_error)?), Err(ModelPriceError::CurrencyMismatch));
        prop_assert_eq!(&total, &snapshot);
        let mut unsupported = row.clone();
        unsupported.version = ModelUsageRecord::VERSION ^ 1;
        let unsupported = unsupported.estimate(Some(&inclusive)).map_err(case_error)?;
        prop_assert_eq!(total.push(&unsupported), Err(ModelPriceError::InvalidRecord));
        prop_assert_eq!(total.replace(&unsupported, &projected), Err(ModelPriceError::InvalidRecord));
        prop_assert_eq!(&total, &snapshot);
        let euro = row.estimate(Some(&revised)).map_err(case_error)?;
        unpriced.replace(&projected, &euro).map_err(case_error)?;
        prop_assert_eq!(unpriced.currency.as_deref(), None);
        prop_assert_eq!(unpriced.token_nano_units.value(), None);
        let unpriced_wire = serde_json::to_value(&unpriced).map_err(case_error)?;
        prop_assert!(unpriced_wire.get("currency").is_some_and(serde_json::Value::is_null));
        let round_trip: ModelUsageTotals = serde_json::from_value(unpriced_wire).map_err(case_error)?;
        prop_assert_eq!(&round_trip, &unpriced);
        revised.currency = "USD".into();
        revised.input_nano_units_per_million = Some(29);
        let updated = row.estimate(Some(&revised)).map_err(case_error)?;
        total.replace(&projected, &updated).map_err(case_error)?;
        prop_assert_eq!(total.token_nano_units.known, (input * 29 + cached * 3 + written * 17 + (output + reasoning) * 23).div_ceil(1_000_000));
        prop_assert_eq!(projected.estimated_cost.price_revision.as_deref(), Some("generated-1"));
        prop_assert_eq!(updated.estimated_cost.price_revision.as_deref(), Some("generated-2"));
        let allocations = acyclic_test_support::measure(|| {
            let same = std::hint::black_box(row.estimate(Some(&revised)).map_err(case_error)?);
            total.replace(&same, &same).map_err(case_error)?;
            let mut token_only = ModelUsageTotals::unpriced();
            token_only.push(&same).map_err(case_error)?;
            token_only.replace(&same, &same).map_err(case_error)
        });
        allocations.output?;
        prop_assert_eq!((allocations.count_total, allocations.bytes_total), (0, 0));
        let copied = acyclic_test_support::measure(|| { std::hint::black_box(row.clone()); });
        prop_assert!(copied.count_total > 0);

        let mut no_reasoning = row.clone();
        no_reasoning.tokens.reasoning = None;
        prop_assert_eq!(no_reasoning.estimate(Some(&inclusive)).map_err(case_error)?.estimated_cost.token_nano_units, Some(expected));
        let mut contradictory = row.clone();
        contradictory.tokens.reasoning = Some(output + reasoning + 1);
        prop_assert_eq!(contradictory.estimate(Some(&inclusive)).map_err(case_error)?.estimated_cost.token_nano_units, None);
        let mut included_cache = inclusive.clone();
        included_cache.cached = TokenPrice::Included;
        contradictory = row.clone();
        // Each count fits independently; together these disjoint subsets do not.
        contradictory.tokens.cached = Some(input + cached + 1);
        prop_assert_eq!(contradictory.estimate(Some(&included_cache)).map_err(case_error)?.estimated_cost.token_nano_units, None);
        included_cache.cache_write = TokenPrice::Included;
        prop_assert_eq!(contradictory.estimate(Some(&included_cache)).map_err(case_error)?.estimated_cost.token_nano_units, None);
        contradictory.tokens.cached = None;
        prop_assert!(contradictory.estimate(Some(&included_cache)).map_err(case_error)?.estimated_cost.token_nano_units.is_some());
        let zero = record(ModelTokenUsage { input: Some(0), output: Some(0), ..Default::default() });
        let mut zero_price = inclusive.clone();
        zero_price.input_nano_units_per_million = None;
        zero_price.output_nano_units_per_million = None;
        zero_price.cached = TokenPrice::Included;
        zero_price.cache_write = TokenPrice::Included;
        prop_assert_eq!(zero.estimate(Some(&zero_price)).map_err(case_error)?.estimated_cost.token_nano_units, Some(0));
        prop_assert_eq!(zero.estimate(None).map_err(case_error)?.estimated_cost.token_nano_units, None);

        // Overflow is unknown for a call, and cannot corrupt an aggregate.
        let huge = record(ModelTokenUsage { input: Some(u64::MAX), output: Some(u64::MAX), ..Default::default() });
        let mut huge_price = inclusive.clone();
        huge_price.input_nano_units_per_million = Some(u64::MAX);
        huge_price.output_nano_units_per_million = Some(u64::MAX);
        huge_price.cached = TokenPrice::Included;
        huge_price.cache_write = TokenPrice::Included;
        prop_assert_eq!(huge.estimate(Some(&huge_price)).map_err(case_error)?.estimated_cost.token_nano_units, None);
        total.input.known = u64::MAX;
        let snapshot = total.clone();
        prop_assert_eq!(total.push(&projected), Err(ModelPriceError::Overflow));
        prop_assert_eq!(&total, &snapshot);

        // Raw units, absence and selected price identity survive the file projection.
        let value = serde_json::to_value(&projected).map_err(case_error)?;
        prop_assert_eq!(value.pointer("/usage/tokens").ok_or_else(|| TestCaseError::fail("missing token counters"))?, &serde_json::to_value(row.tokens).map_err(case_error)?);
        prop_assert!(value.pointer("/usage/latency_ns").is_some_and(serde_json::Value::is_null));
        prop_assert_eq!(value.pointer("/estimated_cost/price_revision").and_then(serde_json::Value::as_str), Some("generated-1"));
        let decoded: ModelUsageProjection<'static> = serde_json::from_value(value).map_err(case_error)?;
        prop_assert_eq!(decoded.usage.tokens, row.tokens);
        let mut decoded_total = ModelUsageTotals::new("USD".into()).map_err(case_error)?;
        decoded_total.push(&decoded).map_err(case_error)?;
        prop_assert_eq!(decoded_total.token_nano_units.value(), Some(expected));
    }
}
