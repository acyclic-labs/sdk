use super::*;
use proptest::prelude::*;
use serde_json::json;

proptest! {
    // Property: simultaneously live capture quotas fit one aggregate byte
    // budget for every command count, without a synchronization primitive.
    #[test]
    fn capture_partitions_bound_aggregate_storage(maximum in 256u64..=65_536, count in 2usize..512) {
        let quota = capture_quota(maximum, count);
        prop_assert!((quota as u64) * count as u64 <= maximum);
        // The former independent per-command limit violates the same detector.
        let broken = maximum.min(65_536);
        prop_assert!(broken * count as u64 > maximum);
    }

    // Property: every admitted canonical delta is charged exactly, including
    // escaping, replacement and first-wins decisions. Exhaustion keeps a bounded
    // guard fold, and its retained representation always fits the same limit.
    #[test]
    fn merging_accounts_exact_encoded_bytes(
        steps in prop::collection::vec((".{0,48}", any::<bool>(), any::<bool>(), 0u8..4), 1..12),
        maximum in 256u64..4096,
    ) {
        let mut output = HookOutput::default();
        let mut budget = Budget::new(maximum)?;
        for (text, deny, replace, mode) in steps {
            let value = json!({"decision":if deny { "block" } else { "approve" }, "reason":text,
                "hookSpecificOutput": {"additionalContext":text,
                    "permissionDecision":if mode == 0 { "ask" } else { "allow" },
                    "permissionDecisionReason":text}});
            let mut value = value;
            if replace { value["hookSpecificOutput"]["updatedInput"] = json!({"text":text}); }
            let result = match mode {
                1 => Err(Error::Process(crate::ProcessError::Timeout { exit_code:None, message:text })),
                2 => Ok((Some(3), vec![], text.into_bytes())),
                _ => Ok((Some(0), serde_json::to_vec(&value)?, vec![])),
            };
            budget.apply(&mut output, HookEvent::PreToolUse, HookFailure::Block, result);
            let exact = serde_json::to_vec(&output)?.len() as u64;
            prop_assert!(exact <= maximum);
            if let State::Available { used, .. } = budget.state { prop_assert_eq!(used, exact); }
        }
        budget.finish(&mut output);
        prop_assert!(serde_json::to_vec(&output)?.len() as u64 <= maximum);
    }
}

#[test]
fn accounting_detector_rejects_a_budget_reset_between_hooks() -> Result<()> {
    fn observed(reset: bool) -> Result<bool> {
        let mut output = HookOutput::default();
        let mut budget = Budget::new(512)?;
        for _ in 0..2 {
            if reset {
                budget = Budget::new(512)?;
            }
            budget.apply(
                &mut output,
                HookEvent::SessionStart,
                HookFailure::Report,
                Ok((Some(0), b"retained context".to_vec(), vec![])),
            );
        }
        Ok(
            matches!(budget.state, State::Available { used, .. } if used == serde_json::to_vec(&output).unwrap().len() as u64),
        )
    }
    assert!(observed(false)?);
    assert!(
        !observed(true)?,
        "reset budget escaped the same exact-size detector"
    );
    Ok(())
}

#[test]
fn aggregate_failure_preserves_an_already_decoded_denial() -> Result<()> {
    futures::executor::block_on(async {
        let mut hooks = Hooks::default();
        hooks.add_rules([HookRule {
            on: HookEvent::SessionStart,
            matcher: String::new(),
            hooks: vec![
                HookSpec {
                    run: HookRun::Executable {
                        executable: "deny".into(),
                        argv: vec![],
                    },
                    timeout_ms: 1000,
                    failure: HookFailure::Report,
                },
                HookSpec {
                    run: HookRun::Executable {
                        executable: "expand".into(),
                        argv: vec![],
                    },
                    timeout_ms: 1000,
                    failure: HookFailure::Report,
                },
            ],
        }])?;
        let output = hooks
            .run_commands(
                HookEvent::SessionStart,
                &json!({}),
                &ToolRegistry::default(),
                512,
                |hook, quota| {
                    let stdout = match &hook.run {
                        HookRun::Executable { executable, .. } if executable == "deny" => {
                            br#"{"continue":false,"stopReason":"deny"}"#.to_vec()
                        }
                        _ => vec![0; 100],
                    };
                    assert!(stdout.len() <= quota, "fixture exceeds capture quota");
                    std::future::ready(Ok((Some(0), stdout, vec![])))
                },
            )
            .await?;
        assert!(
            output.blocks(),
            "bounded failure discarded an explicit guard decision"
        );
        assert!(matches!(
            output.diagnostics[0].cause,
            crate::ProcessError::OutputLimit { .. }
        ));
        // The former failure-policy-only fallback loses the decoded denial.
        assert!(!HookOutput::output_limit(false).blocks());
        Ok(())
    })
}

proptest! {
    // Property: optional-output overflow never skips a later explicit denial.
    // The broken variant stops folding at the same cut as the former runner.
    #[test]
    fn exhausted_fold_keeps_later_denials(
        length in 1000usize..=1800, before in any::<bool>(), reason in ".{0,48}",
    ) {
        let detect = |skip: bool| -> Result<bool> {
            let mut output = HookOutput::default();
            let mut budget = Budget::new(4096)?;
            let deny = serde_json::to_vec(&json!({"continue":false,"stopReason":reason})).unwrap();
            let overflow = vec![0;length];
            let replies = if before { [deny,overflow] } else { [overflow,deny] };
            for reply in replies {
                if skip && matches!(budget.state, State::Exhausted { .. }) { break; }
                budget.apply(&mut output, HookEvent::SessionStart, HookFailure::Report,
                    Ok((Some(0),reply,vec![])));
                prop_assert_output_bound(&output, 4096)?;
            }
            budget.finish(&mut output);
            prop_assert_output_bound(&output, 4096)?;
            Ok(output.blocks())
        };
        prop_assert!(detect(false)?);
        prop_assert_eq!(detect(true)?, before, "same denial detector must catch abandoned folding");
    }

    // Property: expanding raw control bytes never materializes a reason-sized
    // string before its reservation. The negative control allocates that same
    // string before charging, exactly the former order.
    #[test]
    fn reservations_precede_owned_output(length in 1000usize..=1800) {
        for allocate_first in [false, true] {
            let mut output = HookOutput::default();
            let mut budget = Budget::new(4096)?;
            let bytes = vec![0; length];
            let result = Ok((Some(0),bytes.clone(),vec![]));
            let mut applied = None;
            let measured = acyclic_test_support::measure(|| {
                if allocate_first {
                    output.additional_context.push(std::str::from_utf8(&bytes).unwrap().to_owned());
                }
                applied = Some(budget.apply(&mut output, HookEvent::SessionStart, HookFailure::Report, result));
            });
            applied.unwrap();
            prop_assert!(matches!(budget.state, State::Exhausted { .. }), "expected exhausted ledger");
            prop_assert!(output.additional_context.is_empty());
            prop_assert_eq!(measured.bytes_total < length as u64, !allocate_first,
                "pre-reservation allocation detector failed: {:?}", measured);
            budget.finish(&mut output);
            prop_assert_output_bound(&output, 4096)?;
        }
    }
}

fn prop_assert_output_bound(output: &HookOutput, maximum: usize) -> Result<()> {
    assert!(serde_json::to_vec(output).unwrap().len() <= maximum);
    Ok(())
}

#[test]
fn runner_folds_deny_after_encoded_overflow_in_config_order() -> Result<()> {
    futures::executor::block_on(async {
        let mut hooks = Hooks::default();
        hooks.add_rules([HookRule {
            on: HookEvent::SessionStart,
            matcher: String::new(),
            hooks: ["expand", "deny"]
                .into_iter()
                .map(|command| HookSpec {
                    run: HookRun::Executable {
                        executable: command.into(),
                        argv: vec![],
                    },
                    timeout_ms: 1000,
                    failure: HookFailure::Report,
                })
                .collect(),
        }])?;
        let mut calls = 0;
        let output = hooks.run_commands(HookEvent::SessionStart, &json!({}), &ToolRegistry::default(), 4096,
            |hook, quota| {
                calls += 1;
                let stdout = if matches!(&hook.run, HookRun::Executable { executable, .. } if executable == "expand") {
                    vec![0;1000]
                } else { br#"{"continue":false,"stopReason":"deny"}"#.to_vec() };
                assert!(stdout.len() <= quota);
                std::future::ready(Ok((Some(0),stdout,vec![])))
            }).await?;
        assert_eq!(calls, 2);
        assert!(output.blocks(), "encoded overflow bypassed a later guard");
        assert_eq!(output.blocked.as_deref(), Some("Hook denied"));
        assert!(matches!(
            output.diagnostics[0].cause,
            crate::ProcessError::OutputLimit { .. }
        ));
        prop_assert_output_bound(&output, 4096)?;
        Ok(())
    })
}

#[test]
fn malformed_unicode_is_reported_before_decision_or_argument_materialization() -> Result<()> {
    for bytes in [
        br#"{"continue":false,"stopReason":"\ud800"}"#.as_slice(),
        br#"{"hookSpecificOutput":{"updatedInput":{"text":"\udc00"}}}"#.as_slice(),
    ] {
        let mut output = HookOutput::default();
        let mut budget = Budget::new(4096)?;
        budget.apply(
            &mut output,
            HookEvent::PreToolUse,
            HookFailure::Block,
            Ok((Some(0), bytes.to_vec(), vec![])),
        );
        budget.finish(&mut output);
        assert!(output.blocks());
        assert!(output.blocked.is_none());
        assert!(output.updated_input.is_none());
        assert!(matches!(
            output.diagnostics[0].cause,
            crate::ProcessError::InvalidOutput { .. }
        ));
    }
    Ok(())
}

#[test]
fn exhaustion_applies_only_the_policy_of_failed_or_unretainable_commands() -> Result<()> {
    for event in [HookEvent::SessionStart, HookEvent::Stop] {
        for policy in [HookFailure::Report, HookFailure::Block] {
            let mut output = HookOutput::default();
            let mut budget = Budget::new(4096)?;
            // Plain context expands past the outcome bound only at SessionStart.
            // Stop's explicit exit-2 context is instead a denial regardless of policy.
            budget.apply(
                &mut output,
                event,
                policy,
                Ok((
                    Some(if event == HookEvent::Stop { 2 } else { 0 }),
                    vec![0; 1000],
                    vec![0; 1000],
                )),
            );
            // This successful empty block-policy hook must not change the policy
            // of an earlier report-only overflow.
            budget.apply(
                &mut output,
                event,
                HookFailure::Block,
                Ok((Some(0), vec![], vec![])),
            );
            budget.finish(&mut output);
            assert_eq!(
                output.blocks(),
                event == HookEvent::Stop || policy == HookFailure::Block
            );
            assert!(matches!(
                output.diagnostics[0].cause,
                crate::ProcessError::OutputLimit { .. }
            ));
            prop_assert_output_bound(&output, 4096)?;
        }
    }
    Ok(())
}

proptest! {
    // Property: reservations use canonical encoded size, not wire spelling.
    // The broken variant restores the raw-byte charge; the SAME outcome-bound
    // detector rejects it for every generated expanding-number collection.
    #[test]
    fn replacement_reservations_cover_numeric_expansion(count in 40usize..100, digit in 1u8..10) {
        let numbers = vec![format!("{digit}e6");count].join(",");
        let input = format!(r#"{{"numbers":[{numbers}]}}"#);
        let bytes = format!(r#"{{"hookSpecificOutput":{{"updatedInput":{input}}}}}"#).into_bytes();
        let maximum = serde_json::to_vec(&HookOutput::default())?.len() as u64 - 4 + input.len() as u64;
        for raw_charge in [false,true] {
            let mut output = HookOutput::default();
            let mut budget = Budget::new(maximum)?;
            let mut plan = Plan::inspect(&output, HookEvent::PreToolUse, Some(0), &bytes,b"").unwrap();
            if raw_charge { plan.input.as_mut().unwrap().1 = input.len() as u64; }
            budget.fold(&mut output,plan,false);
            let fits_during_merge = serde_json::to_vec(&output)?.len() as u64 <= maximum;
            budget.finish(&mut output);
            prop_assert_eq!(fits_during_merge && serde_json::to_vec(&output)?.len() as u64 <= maximum, !raw_charge,
                "canonical-size detector must reject the raw-byte reservation");
        }
    }
}
