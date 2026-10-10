//! Compare demand-scoped reads with and without hypotheses using existing Divan.
#![allow(
    clippy::unwrap_used,
    reason = "benchmark setup failure stops measurement"
)]
#[path = "../tests/support/mod.rs"]
#[allow(
    dead_code,
    reason = "benchmark shares the domain fixture, not every adversarial evidence constructor"
)]
mod support;
use acyclic_client::*;
use divan::Bencher;
use support::*;

#[global_allocator]
static ALLOC: divan::AllocProfiler = divan::AllocProfiler::system();

fn main() {
    divan::main();
}

#[divan::bench(args = [1, 8, 16])]
fn canonical_view(bencher: Bencher, records: u64) {
    let mut client = Client::new(Numbers::default(), 1, 0, limits()).unwrap();
    for key in 0..records {
        client.observe(key, &evidence(key, 0, 10, None)).unwrap();
    }
    bencher.bench_local(|| client.view(&0, &[]).unwrap());
}

#[divan::bench(args = [1, 8, 16])]
fn predicted_view(bencher: Bencher, records: u64) {
    let mut client = Client::new(Numbers::default(), 1, 0, limits()).unwrap();
    for key in 0..records {
        client.observe(key, &evidence(key, 0, 10, None)).unwrap();
    }
    let id = client.begin(request(0, 0, 10, 11, None, vec![])).unwrap();
    bencher.bench_local(|| client.view(&0, &[id]).unwrap());
}

#[divan::bench(args = [1, 8, 16])]
fn selective_reconciliation(bencher: Bencher, records: u64) {
    bencher
        .with_inputs(|| {
            let mut client = Client::new(Numbers::default(), 1, 0, limits()).unwrap();
            for key in 0..records {
                client.observe(key, &evidence(key, 0, 10, None)).unwrap();
                client.begin(request(key, 0, 10, 11, None, vec![])).unwrap();
            }
            client
        })
        .bench_local_values(|mut client| client.observe(0, &evidence(0, 1, 12, None)).unwrap());
}
