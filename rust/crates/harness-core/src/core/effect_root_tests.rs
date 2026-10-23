use super::*;
use acyclic_fs::{CancellationToken, WorkCounters, performance::BOUNDARY_BUDGET};
use acyclic_test_support::differential;
use proptest::prelude::*;

pub(crate) use crate::test_support::EffectRootObservation as Observation;

pub(crate) fn verifier() -> AuthorityVerifier {
    static RUNTIME: std::sync::OnceLock<AuthorityIssuer> = std::sync::OnceLock::new();
    RUNTIME
        .get_or_init(|| {
            AuthorityIssuer::new(
                "effect-root-proof",
                [91; 32],
                Authority {
                    kind: AggregateKind::Task,
                    id: "effect-root-proof".into(),
                },
            )
        })
        .verifier(&mut None)
        .unwrap()
}

// Buffered baseline: initialized independently, with no candidate codec or state.
pub(crate) fn reference(
    v: &AuthorityVerifier,
    revision: u64,
    canonical: [u8; 32],
    root: Option<[u8; 32]>,
) -> [u8; 64] {
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
    use ed25519_dalek::Signer;
    let signer = ed25519_dalek::SigningKey::from_bytes(&blake3::derive_key(
        "acyclic.harness.metadata.v1",
        &[91; 32],
    ));
    signer.sign(blake3::hash(&bytes).as_bytes()).to_bytes()
}

fn measured(
    v: &AuthorityVerifier,
    revision: u64,
    canonical: [u8; 32],
    root: Option<[u8; 32]>,
    decimal: bool,
) -> (Vec<u8>, WorkCounters) {
    let io =
        crate::filesystem::PreparationIo::from_budget(BOUNDARY_BUDGET, &CancellationToken::new())
            .unwrap();
    let mut observer = &io;
    let proof = if decimal {
        old_proof(v, revision, canonical, root, &mut Some(&mut observer))
            .unwrap()
            .to_vec()
    } else {
        v.effect_history_root_proof_with_work(revision, canonical, root, &mut Some(&mut observer))
            .unwrap()
            .to_vec()
    };
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
                (reference(&v, revision, canonical, root).to_vec(), None)
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
    let domain = b"harness/v1/effect-history-root\0";
    let bytes = crate::store::history::canonical_bytes_with_work(
        &(&v.id, &v.audience, &path, revision, canonical, root),
        crate::store::default_projection_read_limits().maximum_bytes,
        quota.work(),
    )?;
    quota.adopt(bytes.capacity() as u64)?;
    if let Some(io) = quota.work().as_deref_mut() {
        io.hashed_bytes(domain.len() as u64 + 8 + bytes.len() as u64)?;
    }
    let mut hasher = blake3::Hasher::new_keyed(&[91; 32]);
    hasher.update(domain);
    hasher.update(&(bytes.len() as u64).to_le_bytes());
    hasher.update(&bytes);
    Ok(*hasher.finalize().as_bytes())
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

// The history adapter returns a canonical encoding with exactly its buffer
// capacity owned, and the caller's release of `capacity()` balances the ledger
// to zero, whatever slack the growing encoder left. Restoring the former
// second slack charge in `canonical_bytes_for_bounds_with_work` leaves
// `capacity + slack` owned and fails the first assertion on any slack case.
// Returns whether this value left encoder slack.
fn history_encoding_owns_exactly_its_capacity(value: &str) -> bool {
    let io =
        crate::filesystem::PreparationIo::from_budget(BOUNDARY_BUDGET, &CancellationToken::new())
            .unwrap();
    let mut observer = &io;
    let mut work: Option<&mut dyn crate::store::history::HistoryIo> = Some(&mut observer);
    let bytes =
        crate::store::history::canonical_bytes_with_work(value, 1 << 20, &mut work).unwrap();
    let capacity = bytes.capacity() as u64;
    assert_eq!(io.owned_bytes().unwrap(), capacity, "value={value:?}");
    let slack = bytes.capacity() > bytes.len();
    drop(bytes);
    work.as_deref_mut()
        .unwrap()
        .release_owned(capacity)
        .unwrap();
    assert_eq!(io.owned_bytes().unwrap(), 0, "value={value:?}");
    slack
}

// Every encoded length up to past several growth steps, so every slack residue
// of the geometric growth is exercised; the sweep must see slack.
#[test]
fn history_encoding_owns_exactly_its_capacity_at_every_length() {
    let mut slack_seen = false;
    for length in 0..=1100 {
        slack_seen |= history_encoding_owns_exactly_its_capacity(&"x".repeat(length));
    }
    assert!(slack_seen, "no length exercised encoder slack");
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    // Escapes and multi-byte text change the encoded length independently of
    // the input length; shrinking reports the smallest unbalanced value.
    #[test]
    fn history_encoding_owns_exactly_its_capacity_for_any_text(value in ".{0,600}") {
        history_encoding_owns_exactly_its_capacity(&value);
    }
}
