#![allow(missing_docs)]

fn main() {
    match acyclic_inference_wasm::run_terminal_metadata_native() {
        Ok(metadata) => println!("{metadata}"),
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(1);
        }
    }
}
