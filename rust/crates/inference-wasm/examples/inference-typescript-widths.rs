fn main() {
    match acyclic_inference_wasm::client_widths_native() {
        Ok(widths) => println!("{widths}"),
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(1);
        }
    }
}
