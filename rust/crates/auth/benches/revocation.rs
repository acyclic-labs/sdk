//! Synthetic publication cost and byte accounting, without credential output.
use acyclic_auth::{contract::*, revocation::*};
use ed25519_dalek::{Signer, SigningKey};
use prost::Message;
use std::time::Instant;

fn signed(publication: RevocationPublication, key: &SigningKey) -> SignedRevocation {
    let signature = key.sign(&transcript(&publication)).to_bytes().to_vec();
    SignedRevocation {
        publication,
        signature,
    }
}

fn bytes(proof: &SignedRevocation) -> usize {
    SignedRevocationProto::from(proof.clone()).encoded_len()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let signer = SigningKey::from_bytes(&[19; 32]);
    let issuer = signer.verifying_key().to_bytes();
    for retained in [1, 4096, 16384] {
        let mut keys = (0..retained)
            .map(|index| KeyRevocation {
                key_id: format!("synthetic-{index}"),
                expires_at_unix_seconds: 100,
            })
            .collect::<Vec<_>>();
        let checkpoint = |sequence, retired, keys| {
            signed(
                RevocationPublication::Checkpoint(RevocationCheckpoint {
                    account_id: "synthetic".into(),
                    sequence,
                    retired_before_unix_seconds: retired,
                    minimum_key_epoch: 0,
                    keys,
                    blocks: vec![],
                }),
                &signer,
            )
        };
        let initial = checkpoint(1, 1, keys.clone());
        let checkpoint_bytes = bytes(&initial);
        let mut state = RevocationState::from_checkpoint(&initial, "synthetic", &issuer, 1)?;
        let mut delta_bytes = 0;
        let mut elapsed = std::time::Duration::ZERO;
        for index in 0..128 {
            let target = KeyRevocation {
                key_id: format!("changed-{index}"),
                expires_at_unix_seconds: 100,
            };
            let (sequence, digest) = state.frontier();
            let proof = signed(
                RevocationPublication::Delta(RevocationDelta {
                    account_id: "synthetic".into(),
                    sequence: sequence + 1,
                    previous_digest: digest.to_vec(),
                    issued_at_unix_seconds: 1,
                    keys: vec![target.clone()],
                    blocks: vec![],
                    revoke_all_before_epoch: None,
                }),
                &signer,
            );
            delta_bytes += bytes(&proof);
            let start = Instant::now();
            state.apply(&proof, &issuer, 1)?;
            elapsed += start.elapsed();
            keys.push(target);
        }
        let compact = checkpoint(state.frontier().0 + 1, 2, keys);
        let start = Instant::now();
        state.apply(&compact, &issuer, 2)?;
        let checkpoint_us = start.elapsed().as_secs_f64() * 1e6;
        let retired = checkpoint(state.frontier().0 + 1, 100, vec![]);
        state.apply(&retired, &issuer, 100)?;
        println!(
            "{}",
            serde_json::json!({
                "retained_before": retained, "targeted_updates": 128, "affected_per_delta": 1,
                "delta_mean_bytes": delta_bytes as f64 / 128., "initial_checkpoint_bytes": checkpoint_bytes,
                "full_checkpoint_bytes": bytes(&compact), "retired_checkpoint_bytes": bytes(&retired),
                "apply_mean_us": elapsed.as_secs_f64() * 1e6 / 128., "checkpoint_us": checkpoint_us,
            })
        );
    }
    Ok(())
}
