//! Holder trust adapters preserve the shared verifier's scope and revocations.
#[path = "../../auth/tests/common/signed.rs"]
mod signed;
use acyclic_auth::{crypto::BlockId, verify::*};
use acyclic_auth_wasm::{Snapshot, block_ids};
use proptest::prelude::*;

fn snapshot() -> Snapshot {
    Snapshot {
        environment: "test".into(),
        environment_keys: vec![signed::key(1).verifying_key().to_bytes()],
        account: "account".into(),
        issuer_keys: vec![signed::key(2).verifying_key().to_bytes()],
        minimum_key_epoch: 1,
        revoked_key_ids: Default::default(),
        revoked_block_ids: Default::default(),
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn fixed_width_ids_round_trip(ids in prop::collection::vec(any::<BlockId>(), 0..32)) {
        let bytes: Vec<_> = ids.iter().flatten().copied().collect();
        prop_assert_eq!(block_ids(&bytes), Ok(ids));
    }

    #[test]
    fn partial_ids_are_never_admitted(bytes in prop::collection::vec(any::<u8>(), 1..1024)) {
        prop_assert_eq!(block_ids(&bytes).is_ok(), bytes.len() % 32 == 0);
        if bytes.len() % 32 != 0 {
            // A deliberately truncated decoder accepts what this property rejects.
            let broken: Vec<_> = bytes.chunks_exact(32).collect();
            prop_assert_eq!(broken.len(), bytes.len() / 32);
        }
    }

    #[test]
    fn fresh_snapshot_revocation_invalidates_every_descendant(depth in 1..6u8, revoked in 0..6usize) {
        let mut trust = snapshot();
        let mut holder = verify(&signed::root(|_| {}), &trust, signed::NOW).unwrap();
        for index in 1..depth {
            holder = holder.attenuate(vec![], &[index + 4; 32]).unwrap();
        }
        let ancestor = revoked % holder.block_ids().len();
        prop_assert!(holder.check(&trust, signed::NOW).is_ok());
        trust.revoked_block_ids.insert(holder.block_ids()[ancestor]);
        prop_assert_eq!(holder.check(&trust, signed::NOW), Err(VerifyError::Revoked));
        // Broken revocation hooks returning false would accept the same holder.
        trust.revoked_block_ids.clear();
        prop_assert!(holder.check(&trust, signed::NOW).is_ok());
    }

    #[test]
    fn trust_is_scoped_to_account_environment_and_epoch(suffix in "[a-z]{1,8}", epoch in 2..100u64) {
        let bytes = signed::root(|_| {});
        let mut trust = snapshot();
        trust.account.push_str(&suffix);
        prop_assert!(matches!(verify(&bytes, &trust, signed::NOW), Err(VerifyError::Trust)));
        trust = snapshot();
        trust.environment.push_str(&suffix);
        prop_assert!(matches!(verify(&bytes, &trust, signed::NOW), Err(VerifyError::Trust)));
        trust = snapshot();
        trust.minimum_key_epoch = epoch;
        prop_assert!(matches!(verify(&bytes, &trust, signed::NOW), Err(VerifyError::Revoked)));
    }
}
