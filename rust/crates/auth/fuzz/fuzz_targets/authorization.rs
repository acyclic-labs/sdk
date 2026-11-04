#![no_main]
#[path = "../../../contract-derive/src/authorization.rs"]
mod authorization;
use libfuzzer_sys::fuzz_target;
use syn::parse::Parser;
fuzz_target!(|text: &str| {
    for unknown in ["streams/raed", "execution/actor-turn", "new-product/read"] {
        let declaration = format!("auth(action = {unknown:?}, resource = \"stream_path\")");
        assert!(
            syn::meta::parser(|meta| authorization::fields(&meta).map(|_| ()))
                .parse_str(&declaration)
                .is_err(),
            "undeclared authorization action must refuse"
        );
    }
    if let Ok(action) = text.parse::<acyclic_auth_action::Action>() {
        assert_eq!(
            action.as_str(),
            text,
            "action parser must preserve canonical spelling"
        );
    }
    let mut result = None;
    let parser = syn::meta::parser(|meta| {
        result = Some(authorization::fields(&meta)?);
        Ok(())
    });
    if parser.parse_str(text).is_ok() {
        if let Some(declaration) = result {
            assert!(!declaration.is_empty());
        }
    }
});
