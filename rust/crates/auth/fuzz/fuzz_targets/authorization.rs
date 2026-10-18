#![no_main]
#[path = "../../../contract-derive/src/authorization.rs"]
mod authorization;
pub use acyclic_fuzz_support as fuzz_support;
use syn::parse::Parser;
fuzz_support::fuzz_target!(|text: &str, fuzz_input| {
    let _ = fuzz_support::FuzzInput::parse_text(fuzz_input, |text| {
        let mut result = None;
        let parser = syn::meta::parser(|meta| {
            result = Some(authorization::fields(&meta)?);
            Ok(())
        });
        parser.parse_str(text)?;
        if let Some(declaration) = result {
            assert!(!declaration.is_empty());
        }
        Ok::<_, syn::Error>(())
    });
});
