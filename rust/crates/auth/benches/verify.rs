//! Verification and cached admission costs for short capability chains.
#[path = "../tests/common/signed.rs"]
mod signed;
use acyclic_auth::{authority::PolicyDecision, contract::*, verify::verify};
use std::{hint::black_box, time::Instant};

fn main() {
    let trust = signed::Snapshot::default();
    let attributes = RecordAttributes::try_new(vec![]).unwrap();
    let usage = signed::limits(None);
    let request = signed::request("fs:/public", &attributes, &usage);
    for length in [1, 3, 8] {
        let mut capability = verify(&signed::root(|_| {}), &trust, signed::NOW).unwrap();
        for index in 1..length {
            capability = capability
                .attenuate(
                    vec![
                        Caveat {
                            kind: CaveatKind::ExpiresAtUnixSeconds(900)
                        };
                        4
                    ],
                    &[4 + index as u8; 32],
                )
                .unwrap();
        }
        let encoded = capability.encode();
        let start = Instant::now();
        for _ in 0..128 {
            black_box(verify(black_box(&encoded), &trust, signed::NOW).unwrap());
        }
        let verify_micros = start.elapsed().as_secs_f64() * 1e6 / 128.;
        let start = Instant::now();
        for _ in 0..16384 {
            black_box(
                capability
                    .authorize(&trust, black_box(&request), PolicyDecision::Allow)
                    .unwrap(),
            );
        }
        let admission_micros = start.elapsed().as_secs_f64() * 1e6 / 16384.;
        println!(
            "{length} blocks: verify {verify_micros:.2} us/chain; cached admission {admission_micros:.2} us/request"
        );
    }
}
