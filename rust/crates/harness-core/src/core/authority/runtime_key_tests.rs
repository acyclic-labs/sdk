//! Key preparation belongs to the runtime lifetime, not each signed message.
use super::*;

#[test]
fn issuance_receipts_and_metadata_share_runtime_prepared_keys() -> Result<()> {
    let (result, preparations) = acyclic_test_support::differential::observe(
        || (),
        |()| -> Result<()> {
            let issuer = AuthorityIssuer::new(
                "prepared-runtime",
                [89; 32],
                Authority {
                    kind: AggregateKind::Task,
                    id: "prepared-task".into(),
                },
            );
            let copy = issuer.copy(&mut None)?;
            let verifier = copy.verifier(&mut None)?;
            for byte in 0..8 {
                let scope = copy.issue(
                    "actor",
                    &Capabilities::new(["event:append"]),
                    None,
                    None,
                    &mut None,
                )?;
                verifier.verify(&scope, &mut None)?;
                let payload = [byte; 32];
                let receipt = verifier.admission.record(
                    "prepared-runtime",
                    &issuer.audience,
                    &scope,
                    payload,
                    &mut None,
                )?;
                workload::verify_record(&verifier, &receipt, payload, None, &[], &mut None)?;
                let proof = verifier.canonical_proof(b"prepared-metadata", &payload, &mut None)?;
                verifier.verify_canonical_proof(
                    b"prepared-metadata",
                    &payload,
                    &proof,
                    &mut None,
                )?;
                assert!(
                    verifier
                        .verify_canonical_proof(b"foreign-metadata", &payload, &proof, &mut None,)
                        .is_err()
                );
            }
            Ok(())
        },
    );
    result?;
    // Four domain-separated runtime keys, regardless of messages, receipts,
    // verifier copies or metadata signatures. All signatures above are real.
    let preparations: Vec<KeyPreparation> = preparations;
    assert_eq!(preparations.len(), 4, "runtime keys were prepared again");
    Ok(())
}
