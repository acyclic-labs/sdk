//! Operation context overhead with and without an observing subscriber.

use acyclic_native_runtime::obs::OperationSpan;
use divan::{AllocProfiler, Bencher, black_box};

#[global_allocator]
static ALLOC: AllocProfiler = AllocProfiler::system();

fn main() {
    let _subscriber = std::env::var_os("NATIVE_BENCH_TRACING")
        .map(|_| tracing::subscriber::set_default(tracing_subscriber::registry()));
    divan::main();
}

#[divan::bench(args = [false, true])]
fn clone_context(bencher: Bencher, parent: bool) {
    let parent = if parent {
        tracing::info_span!("benchmark.parent")
    } else {
        tracing::Span::none()
    };
    parent.in_scope(|| {
        let context = OperationSpan::new(tracing::Span::none());
        bencher.bench_local(|| drop(black_box(black_box(&context).clone())));
    });
}

#[divan::bench(args = [false, true])]
fn create_context(bencher: Bencher, parent: bool) {
    let parent = if parent {
        tracing::info_span!("benchmark.parent")
    } else {
        tracing::Span::none()
    };
    parent.in_scope(|| {
        bencher.bench_local(|| {
            drop(black_box(OperationSpan::new(tracing::Span::none())));
        });
    });
}
