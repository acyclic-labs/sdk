use super::project;
use proptest::prelude::*;

#[path = "roundtrip.rs"]
mod roundtrip;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]

    #[test]
    fn includes_preserve_every_generated_token(methods in 1usize..25, fields in 1usize..65) {
        let members = (0..fields).map(|index| format!("/// Field {index}.\npub field{index}: u64,\n")).collect::<String>();
        let arms = (0..methods).map(|index| {
            let statements = (0..fields).map(|field| format!("let value{field} = {field};\n")).collect::<String>();
            format!("{index} => {{ {statements} {index} }},\n")
        }).collect::<String>();
        let source = format!("pub mod generated {{ pub struct Message {{ {members} }} impl Message {{ pub fn call(&self, input: u64) -> u64 {{ match input {{ {arms} _ => 0 }} }} }} }}");
        let files = project("acyclic/machines/v1/test.tonic.rs", &source).map_err(|error| TestCaseError::fail(error.to_string()))?;
        prop_assert!(roundtrip::assert_roundtrip(&source, files).is_ok());
    }
}

#[test]
fn large_nested_dispatch_spills_and_preserves_every_token() -> Result<(), Box<dyn std::error::Error>>
{
    let source = include_str!("../../../../fuzz/corpus/sdk_rust_projection/machines");
    let files = project("acyclic/machines/v1/test.tonic.rs", source)?;
    assert!(files.len() > 10);
    roundtrip::assert_roundtrip(source, files)
}

#[test]
fn messages_keep_the_platform_and_feature_gates() -> Result<(), Box<dyn std::error::Error>> {
    for (family, expected) in [
        (
            "objects",
            "all (feature = \"grpc\" , not (target_arch = \"wasm32\"))",
        ),
        ("machines", "feature = \"grpc\""),
    ] {
        let files = project(
            &format!("acyclic/{family}/v1/test.rs"),
            "include!(\"test.tonic.rs\");",
        )?;
        let file = syn::parse_file(&files.last().ok_or("missing output")?.1)?;
        let Some(syn::Item::Macro(item)) = file.items.first() else {
            return Err("include missing".into());
        };
        use quote::ToTokens;
        assert_eq!(
            item.attrs
                .first()
                .ok_or("gate missing")?
                .meta
                .to_token_stream()
                .to_string(),
            format!("cfg ({expected})")
        );
    }
    Ok(())
}
