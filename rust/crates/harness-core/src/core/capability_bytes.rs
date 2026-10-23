//! Canonical opaque admission bytes in JSON; the protobuf carrier stays bytes.
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::{Deserialize, Deserializer, Serializer};

pub(super) fn serialize<S: Serializer>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
    serializer.collect_str(&base64::display::Base64Display::new(bytes, &STANDARD))
}

pub(super) fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
    let encoded = zeroize::Zeroizing::new(String::deserialize(deserializer)?);
    let mut decoded = zeroize::Zeroizing::new(Vec::new());
    STANDARD
        .decode_vec(encoded.as_bytes(), &mut decoded)
        .map_err(serde::de::Error::custom)?;
    Ok(std::mem::take(&mut *decoded))
}

#[cfg(test)]
mod tests {
    #[derive(serde::Serialize, serde::Deserialize)]
    struct Receipt(#[serde(with = "super")] Vec<u8>);

    #[test]
    fn opaque_receipts_have_one_canonical_json_spelling() -> Result<(), serde_json::Error> {
        let receipt = Receipt(vec![0, 255]);
        let encoded = serde_json::to_string(&receipt)?;
        assert_eq!(encoded, "\"AP8=\"");
        assert_eq!(serde_json::from_str::<Receipt>(&encoded)?.0, receipt.0);
        for rejected in ["\"AP8\"", "\"AP9=\"", "[0,255]", "\"AP8=\\n\""] {
            assert!(
                serde_json::from_str::<Receipt>(rejected).is_err(),
                "{rejected}"
            );
        }
        Ok(())
    }
}
