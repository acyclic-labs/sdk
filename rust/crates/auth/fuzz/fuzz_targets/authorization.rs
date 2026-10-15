#![no_main]
#[path = "../../../contract-derive/src/authorization.rs"]
mod authorization;
use libfuzzer_sys::fuzz_target;
use syn::parse::Parser;
fuzz_target!(|text: &str| {
    let mut result = None;
    let parser = syn::meta::parser(|meta| {
        result = Some(authorization::fields(meta)?);
        Ok(())
    });
    if parser.parse_str(text).is_ok() {
        if let Some(declaration) = result {
            assert!(!declaration.is_empty());
        }
    }
});
