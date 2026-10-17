use super::*;
use acyclic_fs::{CancellationToken, WorkCounters, performance::BOUNDARY_BUDGET};
use acyclic_test_support::differential;
use proptest::prelude::*;

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Observation {
    Hash(u64),
    Storage(String),
}

pub(crate) fn verifier() -> AuthorityVerifier {
    AuthorityIssuer::new(
        "effect-root-proof",
        [91; 32],
        Authority {
            kind: AggregateKind::Task,
            id: "effect-root-proof".into(),
        },
    )
    .verifier(&mut None)
    .unwrap()
}

// Buffered baseline: initialized independently, with no candidate codec or state.
pub(crate) fn reference(
    v: &AuthorityVerifier,
    revision: u64,
    canonical: [u8; 32],
    root: Option<[u8; 32]>,
) -> [u8; 32] {
    let mut bytes = b"harness/v2/effect-history-root\0".to_vec();
    bytes.push(5);
    for text in [v.id.as_bytes(), v.audience.id.as_bytes()] {
        bytes.extend((text.len() as u64).to_le_bytes());
        bytes.extend(text);
    }
    bytes.extend(revision.to_le_bytes());
    bytes.extend(canonical);
    bytes.push(u8::from(root.is_some()));
    bytes.extend(root.unwrap_or([0; 32]));
    differential::record(Observation::Hash(bytes.len() as u64));
    *blake3::keyed_hash(&v.key, &bytes).as_bytes()
}

fn measured(
    v: &AuthorityVerifier,
    revision: u64,
    canonical: [u8; 32],
    root: Option<[u8; 32]>,
    decimal: bool,
) -> ([u8; 32], WorkCounters) {
    let io =
        crate::filesystem::PreparationIo::from_budget(BOUNDARY_BUDGET, &CancellationToken::new())
            .unwrap();
    let mut observer = &io;
    let proof = if decimal {
        old_proof(v, revision, canonical, root, &mut Some(&mut observer))
    } else {
        v.effect_history_root_proof_with_work(revision, canonical, root, &mut Some(&mut observer))
    }
    .unwrap();
    assert_eq!(io.owned_bytes().unwrap(), 0);
    (proof, io.receipt())
}

fn assert_exact_work(v: &AuthorityVerifier, actual: WorkCounters) {
    let expected = WorkCounters {
        bytes_hashed: (b"harness/v2/effect-history-root\0".len()
            + 1
            + 16
            + v.id.len()
            + v.audience.id.len()
            + 8
            + 32
            + 1
            + 32) as u64,
        ..WorkCounters::default()
    };
    assert_eq!(actual, expected);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn proof_bytes_trace_and_exact_work_are_independent_of_cut_digits(
        revision in any::<u64>(), canonical in any::<[u8;32]>(), root in proptest::option::of(any::<[u8;32]>())
    ) {
        let [(baseline, baseline_trace), (candidate, candidate_trace)] = differential::pair::<_, _, Observation>(
            verifier,
            |v, baseline| if baseline {
                (reference(&v, revision, canonical, root), None)
            } else {
                let (proof, work) = measured(&v, revision, canonical, root, false);
                assert_exact_work(&v, work);
                (proof, Some(work))
            },
        );
        prop_assert_eq!(baseline.0, candidate.0);
        prop_assert_eq!(baseline_trace, candidate_trace);
        let (_, other) = measured(&verifier(), revision.wrapping_add(1), [255;32], root, false);
        prop_assert_eq!(candidate.1.unwrap(), other);
    }
}

#[test]
fn exact_work_oracle_detects_restored_decimal_serialization() {
    let v = verifier();
    for digest in [[0; 32], [255; 32]] {
        let (_, work) = measured(&v, 64, digest, Some([0; 32]), true);
        assert!(std::panic::catch_unwind(|| assert_exact_work(&v, work)).is_err());
    }
    let (_, sparse) = measured(&v, 64, [0; 32], Some([0; 32]), true);
    let (_, dense) = measured(&v, 64, [255; 32], Some([0; 32]), true);
    assert!(std::panic::catch_unwind(|| assert_eq!(sparse, dense)).is_err());
}

// Frozen v1 MAC path; no v2 framing or certificate decoder is used here.
pub(crate) fn old_proof(
    v: &AuthorityVerifier,
    revision: u64,
    canonical: [u8; 32],
    root: Option<[u8; 32]>,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<[u8; 32]> {
    let mut quota = CodecQuota::new(work, 0)?;
    let path = v.audience.stream_path(quota.work())?;
    quota.adopt(path.capacity() as u64)?;
    v.canonical_proof(
        b"harness/v1/effect-history-root\0",
        &(&v.id, &v.audience, &path, revision, canonical, root),
        quota.work(),
    )
}

#[test]
fn old_decimal_preimage_exposes_content_dependent_work() {
    let v = verifier();
    let path = v.audience.stream_path(&mut None).unwrap();
    let observe = |digest| {
        let io = crate::filesystem::PreparationIo::from_budget(
            BOUNDARY_BUDGET,
            &CancellationToken::new(),
        )
        .unwrap();
        let mut observer = &io;
        v.canonical_proof(
            b"harness/v1/effect-history-root\0",
            &(&v.id, &v.audience, &path, 64u64, digest, Some([0u8; 32])),
            &mut Some(&mut observer),
        )
        .unwrap();
        io.receipt()
    };
    let zero = observe([0u8; 32]);
    let dense = observe([255u8; 32]);
    assert_eq!(dense.bytes_hashed - zero.bytes_hashed, 64);
    assert!(dense.bytes_copied > zero.bytes_copied);
    assert!(dense.bytes_encoded > zero.bytes_encoded);
}

#[test]
fn invalid_authority_keeps_the_original_error_and_empty_trace() {
    let [(baseline, before), (candidate, after)] = differential::pair::<_, _, Observation>(
        || {
            let mut v = verifier();
            v.audience.id = "../invalid".into();
            v
        },
        |v, old| {
            if old {
                v.audience.stream_path(&mut None).unwrap_err().to_string()
            } else {
                v.effect_history_root_proof_with_work(0, [0; 32], None, &mut None)
                    .unwrap_err()
                    .to_string()
            }
        },
    );
    assert_eq!(baseline, candidate);
    assert_eq!(before, after);
    assert!(after.is_empty());
}
