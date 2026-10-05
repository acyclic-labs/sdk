#[path = "../runtime_consumer.rs"]
mod runtime_consumer;

fn main() {
    if let Err(error) = runtime_consumer::run_from_args(&std::env::args().skip(1).collect::<Vec<_>>()) {
        eprintln!("sdk-runtime-consumer: {error}");
        std::process::exit(2);
    }
}
