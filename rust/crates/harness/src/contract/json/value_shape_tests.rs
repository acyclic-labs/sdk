use super::*;

const RAW: &str = "$serde_json::private::RawValue";
const LIMITS: JsonBounds = JsonBounds {
    maximum_bytes: 16 * 1024 * 1024,
    maximum_items: 2 * 1024 * 1024,
    maximum_depth: 256,
};

fn raw_document(payload: &str) -> String {
    format!("{{\"{RAW}\":{}}}", serde_json::to_string(payload).unwrap())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
mod native {
    use super::*;

    fn decode<F: FnMut(JsonCodecCharge) -> Result<()>>(
        bytes: &[u8], bounds: JsonBounds, charge: &mut F,
    ) -> Result<Value> {
        json_from_slice_bounded(std::marker::PhantomData, bytes, bounds, 0, Some(crate::wire::ErrorCode::Storage), Some(JsonValueSchema::Root), charge)
    }

    #[test]
    fn hidden_million_item_payload_is_denied_before_original_value_growth() {
        let payload = format!("[{}0]", "0,".repeat(999_999));
        let document = raw_document(&payload);
        let mut admitted = false;
        let mut inspected = 0;
        let failure = decode(document.as_bytes(), JsonBounds { maximum_items: 64, ..LIMITS }, &mut |event| {
            admitted |= matches!(event, JsonCodecCharge::Admission { .. });
            if let JsonCodecCharge::Inspect { items } = event { inspected += items; }
            Ok(())
        }).unwrap_err();
        assert!(!admitted, "hidden semantic items must be denied before Value Deserialize");
        assert!(inspected <= payload.len() as u64);
        assert!(matches!(failure, Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, .. }));
    }

    #[test]
    fn hidden_semantic_depth_and_raw_parser_frames_are_admitted_before_growth() {
        let deep = raw_document(&format!("{}0{}", "[".repeat(1024), "]".repeat(1024)));
        let frames = (0..6).fold("0".to_owned(), |payload, _| raw_document(&payload));
        for document in [deep, frames] {
            let mut admitted = false;
            let failure = decode(document.as_bytes(), JsonBounds { maximum_depth: 3, ..LIMITS }, &mut |event| {
                admitted |= matches!(event, JsonCodecCharge::Admission { .. });
                Ok(())
            }).unwrap_err();
            assert!(!admitted);
            assert!(matches!(failure, Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, .. }));
        }
        let document = raw_document("[0]");
        let original: Value = serde_json::from_slice(document.as_bytes()).unwrap();
        assert_eq!(original, serde_json::json!([0]));
        let mut admitted = false;
        let failure = decode(document.as_bytes(), JsonBounds { maximum_depth: 1, ..LIMITS }, &mut |event| {
            admitted |= matches!(event, JsonCodecCharge::Admission { .. });
            Ok(())
        }).unwrap_err();
        assert!(!admitted, "local parser counter resets must not bypass the global boundary");
        assert!(matches!(failure, Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, .. }));
        let decoded = decode(document.as_bytes(), JsonBounds { maximum_depth: 2, ..LIMITS }, &mut |_| Ok(())).unwrap();
        assert_eq!(decoded, original, "at the cumulative boundary original Value semantics are unchanged");
    }

}
