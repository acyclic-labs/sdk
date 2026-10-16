fn main() {
    if let Err(error) = sdk_generation::run(std::env::args().skip(1)) {
        eprintln!("sdk-generation: {error}");
        std::process::exit(1);
    }
}
