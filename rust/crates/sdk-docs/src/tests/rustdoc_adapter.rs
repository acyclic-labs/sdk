use super::*;
use proptest::prelude::*;

proptest! {
    #[test]
    fn shared_rustdoc_decoding_matches_owned_decoding_and_round_trips(
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
        let owned: Crate = serde_json::from_value(value.clone()).unwrap();
        let original_bytes = serde_json::to_vec_pretty(&value).unwrap();
        let decoded = crate::public_api::decode::from_slice(&original_bytes).unwrap();
        prop_assert_eq!(serde_json::to_value(&owned).unwrap(), serde_json::to_value(&decoded).unwrap());
        let bytes = serde_json::to_vec(&decoded).unwrap();
        let round_trip: Crate = serde_json::from_slice(&bytes).unwrap();
        prop_assert_eq!(serde_json::to_value(&round_trip).unwrap(), serde_json::to_value(&owned).unwrap());
        let original = crate::public_api::extract(Path::new("fixture.json"), &original_bytes.into(), &decoded);
        let replay = crate::public_api::extract(Path::new("fixture.json"), &bytes.into(), &round_trip);
        if private || legacy {
            prop_assert!(original.is_err());
            prop_assert!(replay.is_err());
        } else {
            prop_assert_eq!(original.unwrap(), replay.unwrap());
        }
    }
}
