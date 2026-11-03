use super::*;
use proptest::prelude::*;

proptest! {
    #[test]
    fn public_projection_preserves_names_and_rejects_nonpublic_receipts(
        suffix in "[a-z0-9_]{0,16}",
        docs in prop::option::of(".{0,96}"),
        version in any::<u16>(),
        private in any::<bool>(),
        legacy in any::<bool>(),
    ) {
        let mut value: serde_json::Value = serde_json::from_slice(include_bytes!(
            "../../fuzz/corpus/rustdoc_adapter/valid.json"
        )).unwrap();
        let name = format!("witness_{suffix}");
        value["index"]["1"]["name"] = serde_json::json!(name);
        value["paths"]["1"]["path"][1] = serde_json::json!(name);
        value["index"]["1"]["docs"] = serde_json::json!(docs);
        value["crate_version"] = serde_json::json!(format!("{version}.0.0"));
        value["includes_private"] = serde_json::json!(private);
        value["format_version"] = serde_json::json!(if legacy { 59 } else { 60 });
        let bytes: std::rc::Rc<[u8]> = serde_json::to_vec(&value).unwrap().into();
        let decoded = crate::public_api::decode::from_slice(&bytes).unwrap();
        let result = crate::public_api::extract(Path::new("fixture.json"), &bytes, &decoded);
        if private || legacy {
            prop_assert!(result.is_err());
        } else {
            let items = result.unwrap();
            let witness = items.iter().find(|item| item.id == rustdoc_types::Id(1));
            prop_assert!(witness.is_some(), "generated public item must survive extraction");
            prop_assert_eq!(&witness.unwrap().path, &vec!["fuzz_doc".to_owned(), name],
                "generated public item must keep its exported identity");
        }
    }
}
