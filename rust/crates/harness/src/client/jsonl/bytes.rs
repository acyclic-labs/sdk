//! The one byte encoding on the JSON wire. Bytes that are valid UTF-8 are
//! `{"text": "..."}`; any other bytes are `{"base64": "..."}` (standard
//! alphabet, padded). Exactly one form is canonical for each byte string, so
//! the decoder rejects base64 of valid UTF-8 and non-canonical base64.
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};

#[derive(Serialize, Deserialize)]
enum Tagged<T> {
    #[serde(rename = "text")]
    Text(T),
    #[serde(rename = "base64")]
    Base64(T),
}

/// Encodes bytes in their single canonical wire form.
pub(super) fn serialize<S: Serializer, T: AsRef<[u8]> + ?Sized>(
    bytes: &T,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    let bytes = bytes.as_ref();
    match std::str::from_utf8(bytes) {
        Ok(text) => Tagged::Text(text).serialize(serializer),
        Err(_) => Tagged::Base64(STANDARD.encode(bytes)).serialize(serializer),
    }
}

/// Decodes the canonical wire form, rejecting every other spelling.
pub(super) fn deserialize<'de, D: Deserializer<'de>, T: From<Vec<u8>>>(
    deserializer: D,
) -> Result<T, D::Error> {
    decode(deserializer).map(T::from)
}

fn decode<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
    match Tagged::<String>::deserialize(deserializer)? {
        Tagged::Text(text) => Ok(text.into_bytes()),
        Tagged::Base64(encoded) => {
            let bytes = STANDARD.decode(&encoded).map_err(D::Error::custom)?;
            if std::str::from_utf8(&bytes).is_ok() {
                return Err(D::Error::custom("UTF-8 bytes must use the text form"));
            }
            Ok(bytes)
        }
    }
}

/// A fixed-size digest in the same wire form.
pub(super) mod digest {
    use serde::{Deserializer, Serializer, de::Error as _};

    pub(in super::super) fn serialize<S: Serializer>(
        bytes: &[u8; 32],
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        super::serialize(bytes, serializer)
    }

    pub(in super::super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<[u8; 32], D::Error> {
        super::decode(deserializer)?
            .try_into()
            .map_err(|_| D::Error::custom("a digest is exactly 32 bytes"))
    }
}
