//! Verification and cached admission costs for short capability chains.
#[path = "../tests/common/signed.rs"]
mod signed;
use acyclic_auth::{authority::PolicyDecision, contract::*, verify::verify};
use std::{hint::black_box, time::Instant};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let trust = signed::Snapshot::default();
    let attributes = RecordAttributes::default();
    let usage = signed::limits(None);
    let request = signed::request("fs:/public", &attributes, &usage);
    for length in [1u8, 3, 8] {
        let mut capability = verify(&signed::root(|_| {}), &trust, signed::NOW)?;
        for index in 1..length {
            capability = capability.attenuate(
                vec![
                    Caveat {
                        kind: CaveatKind::ExpiresAtUnixSeconds(900)
                    };
                    4
                ],
                &[4 + index; 32],
            )?;
        }
        let encoded = capability.encode();
        let start = Instant::now();
        for _ in 0..128 {
            black_box(verify(black_box(&encoded), &trust, signed::NOW)?);
        }
        let verify_micros = start.elapsed().as_secs_f64() * 1e6 / 128.;
        let start = Instant::now();
        for _ in 0..16384 {
            capability.authorize(
                &trust,
                black_box(&request),
                PolicyDecision::Allow,
                acyclic_auth::authority::Scope::Exact,
            )?;
            black_box(());
        }
        let admission_micros = start.elapsed().as_secs_f64() * 1e6 / 16384.;
        println!(
            "{length} blocks: verify {verify_micros:.2} us/chain; cached admission {admission_micros:.2} us/request"
        );
    }
    Ok(())
}
