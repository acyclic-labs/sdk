//! The derive parser is compiled from its actual source, with no model copy.
#[path = "../../contract-derive/src/authorization.rs"]
mod authorization;
use proptest::prelude::*;
use prost_reflect::DescriptorPool;
use std::{fs, path::Path, process::Command};
use syn::parse::Parser;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn authorization_strings_round_trip(value in ".{1,40}") {
        let input=format!("auth(action = {value:?}, action = \"fs/read\", extractor = \"fs.paths\")");
        let mut declaration=String::new();
        syn::meta::parser(|meta| { declaration=authorization::fields(&meta)?; Ok(()) }).parse_str(&input).unwrap();
        let root=tempfile::tempdir().unwrap();
        let proto=format!("syntax = \"proto3\"; package test; import \"validation/v1/options.proto\"; message Empty {{}} service Test {{ rpc Call(Empty) returns (Empty) {{ option (acyclic.validation.v1.auth) = {{ {declaration} }}; }} }}");
        fs::write(root.path().join("test.proto"),proto).unwrap();
        let source=Path::new(env!("CARGO_MANIFEST_DIR")).join("../contract-derive/proto");
        let output=root.path().join("test.bin");
        let status=Command::new(protoc_bin_vendored::protoc_bin_path().unwrap())
            .arg(format!("--proto_path={}",root.path().display()))
            .arg(format!("--proto_path={}",source.display()))
            .arg(format!("--proto_path={}",protoc_bin_vendored::include_path().unwrap().display()))
            .arg(format!("--descriptor_set_out={}",output.display())).arg("--include_imports").arg("test.proto").status().unwrap();
        prop_assert!(status.success());
        let pool=DescriptorPool::decode(fs::read(output).unwrap().as_slice()).unwrap();
        let ext=pool.get_extension_by_name("acyclic.validation.v1.auth").unwrap();
        let options=pool.get_service_by_name("test.Test").unwrap().methods().next().unwrap().options();
        let rules=options.get_extension(&ext);
        let rule=rules.as_list().unwrap().first().unwrap().as_message().unwrap().get_field_by_name("action").unwrap();
        let actions=rule.as_list().unwrap().iter().map(|action| action.as_str()).collect::<Vec<_>>();
        prop_assert_eq!(actions,vec![Some(value.as_str()),Some("fs/read")]);
    }
    #[test]
    fn arbitrary_authorization_input_never_panics(text in ".{0,1024}") {
        let mut result=None;
        let parsed=syn::meta::parser(|meta| {result=Some(authorization::fields(&meta)?);Ok(())}).parse_str(&text);
        if parsed.is_ok() && let Some(declaration)=result { prop_assert!(!declaration.is_empty()); }
    }
}
