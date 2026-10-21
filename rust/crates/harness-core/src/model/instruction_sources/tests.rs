use super::*;
use crate::context::DiscoveredContext;
use acyclic_test_support::{differential, measure};
use proptest::prelude::*;
type Trace = (&'static str, u64);

fn identity(file: &ProjectInstruction) -> InstructionIdentity {
    match file {
        ProjectInstruction::Direct(file) => {
            InstructionIdentity::new(file.volume().clone(), file.path().into()).unwrap()
        }
        ProjectInstruction::Captured { source, .. } => source.clone(),
    }
}
fn fixture(n: usize) -> DiscoveredContext {
    let mut instructions: Vec<ProjectInstruction> = (0..n)
        .map(|i| {
            let name = if i % 2 == 0 { "AGENTS.md" } else { "CLAUDE.md" };
            super::super::instruction_tests::file(&format!("d{i:04}/{name}"), "rules", "rules")
                .into()
        })
        .collect();
    let last = &instructions[n - 1];
    instructions.push(super::super::instruction_tests::file(last.path(), "rules", "alias").into());
    DiscoveredContext {
        instructions,
        skills: vec![],
    }
}

// Independent pre-index behavior: no candidate lookup, decoder or projection.
fn reference(context: &DiscoveredContext) -> Result<Vec<ModelMessage>> {
    let mut seen: Vec<(InstructionIdentity, usize)> = Vec::new();
    let mut output = Vec::new();
    for (position, file) in context.instructions.iter().enumerate() {
        file.validate_source(&mut None)?;
        trace_operation("instruction-source-visit", 1);
        let source = identity(file);
        if let Some((_, prior)) = seen.iter().find(|(prior, _)| prior == &source) {
            let prior = &context.instructions[*prior];
            if prior.descriptor().sha256() != file.descriptor().sha256()
                || prior.descriptor().byte_length() != file.descriptor().byte_length()
            {
                return Err(Error::Conflict(
                    "instruction source has conflicting contents".into(),
                ));
            }
            continue;
        }
        seen.push((source.clone(), position));
        output.push(ModelMessage {
            role: ModelRole::User,
            content: ModelContent::Part(ModelContentPart::File {
                file: (**file).clone(),
                policy: FileProjectionPolicy::ProjectInstructions(Box::new(source)),
            }),
        });
    }
    Ok(output)
}

// A flat, uncompressed binary partition oracle, separate from the radix nodes.
// It counts the branching decisions on each lookup, including skipped prefix
// bits, and the insertion path above the first differing bit.
fn index_work(context: &DiscoveredContext, observed: bool) -> u64 {
    let mut keys: Vec<[u8; 32]> = Vec::new();
    let mut work = 0;
    let bit = |key: &[u8; 32], at: usize| (key[at / 8] >> (7 - at % 8)) & 1;
    for file in &context.instructions {
        let source = identity(file);
        let tuple = (source.volume(), source.path());
        let bytes = if observed {
            crate::contract::canonical_json_bytes(&tuple).unwrap()
        } else {
            serde_json::to_vec(&tuple).unwrap()
        };
        let key = *blake3::hash(&bytes).as_bytes();
        work += 2; // one source visit and one lookup entry
        if keys.is_empty() {
            work += 1;
            keys.push(key);
            continue;
        }
        let mut candidates: Vec<_> = keys.iter().collect();
        let mut branches = Vec::new();
        for at in 0..256 {
            if candidates.len() == 1 {
                break;
            }
            if candidates.iter().any(|key| bit(key, at) == 0)
                && candidates.iter().any(|key| bit(key, at) == 1)
            {
                branches.push(at);
                candidates.retain(|candidate| bit(candidate, at) == bit(&key, at));
            }
        }
        let leaf = candidates[0];
        work += branches.len() as u64 + 33; // leaf visit and full key equality
        if *leaf == key {
            work += 1;
        } else {
            let difference = (0..256).find(|&at| bit(leaf, at) != bit(&key, at)).unwrap();
            work += 34
                + branches.len() as u64
                + branches.iter().filter(|&&at| at < difference).count() as u64;
            keys.push(key);
        }
    }
    work
}
fn total(trace: &[Trace], name: &str) -> u64 {
    trace
        .iter()
        .filter(|(kind, _)| *kind == name)
        .map(|(_, n)| n)
        .sum()
}
fn pipeline(context: DiscoveredContext, mode: u8) -> (Vec<ModelMessage>, u64, u64) {
    if mode == 0 {
        context.validate(&mut None).unwrap();
        return (context.messages(Limits::default(), None).unwrap(), 0, 0);
    }
    let result =
        crate::test_support::accounted::accounted(mode == 2, 32 * 1024 * 1024, false, |io| {
            with_history_io(Some(io), |work| context.validate(work)).unwrap();
            context.messages(Limits::default(), Some(io)).unwrap()
        })
        .unwrap();
    (result.0, result.1.items_examined, result.5)
}

#[test]
fn discovery_and_projection_have_exact_source_work_at_n_and_2n() {
    for n in [64, 128] {
        for mode in 0..3 {
            let expected = 2 * index_work(&fixture(n), mode != 0) + 2 * n as u64;
            let [old, candidate] = differential::pair::<_, _, Trace>(
                || fixture(n),
                |context, old| {
                    super::super::instruction_index::traced(|| {
                        if old {
                            reference(&context).unwrap();
                            (reference(&context).unwrap(), 0, 0)
                        } else {
                            pipeline(context, mode)
                        }
                    })
                },
            );
            assert_eq!(
                serde_json::to_vec(&old.0.0).unwrap(),
                serde_json::to_vec(&candidate.0.0).unwrap()
            );
            let visits = |trace: &Vec<Trace>| {
                trace
                    .iter()
                    .filter(|(name, _)| *name == "instruction-source-visit")
                    .copied()
                    .collect::<Vec<_>>()
            };
            assert_eq!(
                visits(&old.1),
                visits(&candidate.1),
                "validation observer trace changed"
            );
            assert_eq!(total(&candidate.1, "instruction-step"), expected);
            assert_eq!(
                total(&candidate.1, "instruction-source-visit"),
                2 * (n + 1) as u64
            );
            assert_eq!(total(&candidate.1, "instruction-source-compare"), 2);
            assert_eq!(
                total(&candidate.1, "instruction-source-projection"),
                2 * n as u64
            );
            assert_eq!(
                candidate.0.2,
                if mode == 2 {
                    acyclic_fs::NativeMaterializationWork::accounting_state_bytes()
                } else {
                    0
                }
            );
            if mode != 0 {
                assert!(candidate.0.1 >= expected);
            }
            let (broken, trace) = differential::observe::<_, _, Trace>(
                || fixture(n),
                |context| {
                    super::super::instruction_index::traced(|| {
                        with_prefix_scan(true, || pipeline(context, mode))
                    })
                },
            );
            assert_eq!(
                serde_json::to_vec(&broken.0).unwrap(),
                serde_json::to_vec(&candidate.0.0).unwrap()
            );
            let extra = (n * (n + 1)) as u64; // two actual validation/projection entrypoints
            assert_eq!(total(&trace, "instruction-step"), expected + extra);
            assert_eq!(total(&trace, "instruction-source-compare"), 2 + extra);
            assert_ne!(
                total(&trace, "instruction-step"),
                expected,
                "quadratic prefix scan escaped exact work pin"
            );
        }
    }
}

#[test]
fn source_index_allocates_two_funded_vectors_with_copy_negative() {
    for n in [64, 128] {
        for redundant in [false, true] {
            let context = fixture(n);
            let mut output = None;
            let allocations = measure(|| {
                let mut work = None;
                let mut quota = crate::wire_codec::CodecQuota::new(&mut work, 0).unwrap();
                let source = InstructionSources::new(&context.instructions, &mut quota).unwrap();
                let extra = redundant.then(|| source.positions.clone());
                output = Some(source.positions);
                std::hint::black_box(extra);
            });
            assert_eq!(output.unwrap(), (0..n).collect::<Vec<_>>());
            assert_eq!(allocations.count_total, 2 + u64::from(redundant));
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    #[test]
    fn shared_sources_preserve_bytes_errors_scope_and_first_occurrence(
        entries in prop::collection::vec((0u8..6,any::<bool>(),any::<bool>()),0..16),
    ) {
        let instructions=entries.iter().enumerate().map(|(slot,(id,changed,captured))| {
            let path=format!("nested/d{id}/{}",if id%2==0 {"AGENTS.md"} else {"CLAUDE.md"});
            let body=if *changed {"other"} else {"rules"};
            let original=super::super::instruction_tests::file(&path,body,"arbitrary");
            if *captured {
                ProjectInstruction::Captured {
                    source:InstructionIdentity::new(original.volume().clone(),path).unwrap(),
                    file:super::super::instruction_tests::file(&format!("archive/{slot}"),body,"archived"),
                }
            } else {original.into()}
        }).collect();
        let context=DiscoveredContext {instructions,skills:vec![]};
        let encode=|result:Result<Vec<ModelMessage>>|result.map(|messages|serde_json::to_vec(&messages).unwrap()).map_err(|error|error.to_string());
        let expected=encode(reference(&context));
        let validity=context.validate(&mut None).map_err(|error|error.to_string());
        prop_assert_eq!(validity,expected.as_ref().map(|_|()).map_err(Clone::clone));
        prop_assert_eq!(encode(context.messages(Limits::default(),None)),expected);
        let bytes=serde_json::to_vec(&context).unwrap();
        let decoded:DiscoveredContext=serde_json::from_slice(&bytes).unwrap();
        prop_assert_eq!(encode(decoded.messages(Limits::default(),None)),encode(reference(&context)));
    }
}
