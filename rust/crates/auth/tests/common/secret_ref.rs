use crate::secrets::{SecretRef, SecretRefProto, decode_request};
use prost::Message;
use serde_json::Value;

pub fn check(bytes: &[u8]) {
    if let Ok(reference) = serde_json::from_slice::<SecretRef>(bytes) {
        let json = serde_json::to_vec(&reference).unwrap();
        assert_eq!(
            serde_json::from_slice::<SecretRef>(&json).unwrap(),
            reference
        );
        let mut fields: Value = serde_json::from_slice(&json).unwrap();
        fields["version"] = Value::from(reference.version());
        assert_eq!(
            serde_json::from_value::<SecretRef>(fields.clone()).unwrap(),
            reference
        );
        fields["unexpected"] = Value::Null;
        assert!(
            serde_json::from_value::<SecretRef>(fields).is_err(),
            "secret reference must refuse unknown fields"
        );
        let mut fields: Value = serde_json::from_slice(&json).unwrap();
        fields["version"] = Value::from(0);
        assert!(serde_json::from_value::<SecretRef>(fields.clone()).is_err());
        fields["version"] = Value::String(format!("{}00000000000000000000", reference.version()));
        assert!(serde_json::from_value::<SecretRef>(fields).is_err());
    }
    if let Ok(reference) = decode_request::<SecretRefProto, SecretRef>(bytes) {
        assert_eq!(
            SecretRefProto::from(reference.clone()).encode_to_vec(),
            bytes
        );
        let mut repeated = SecretRefProto {
            version: reference.version(),
            ..Default::default()
        }
        .encode_to_vec();
        repeated.extend_from_slice(bytes);
        assert!(
            decode_request::<SecretRefProto, SecretRef>(&repeated).is_err(),
            "secret reference must refuse duplicate wire fields"
        );
    }
}
