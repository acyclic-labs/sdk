//! Regenerate digest/preimage fields in the single checked-in vector file.
use acyclic_auth::addressing::{ResourceKind, ResourceName};
use std::{error::Error, path::PathBuf};

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn unhex(value: &str) -> Result<Vec<u8>, Box<dyn Error>> {
    if !value.len().is_multiple_of(2) {
        return Err("odd-length hex".into());
    }
    value
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| Ok(u8::from_str_radix(std::str::from_utf8(pair)?, 16)?))
        .collect()
}

fn main() -> Result<(), Box<dyn Error>> {
    let path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/resource-address-v1.json");
    let mut file: serde_json::Value = serde_json::from_slice(&std::fs::read(&path)?)?;
    let rows = file
        .get_mut("addresses")
        .and_then(serde_json::Value::as_array_mut)
        .ok_or("missing addresses")?;
    for row in rows {
        let account = unhex(
            row.get("account_hex")
                .and_then(serde_json::Value::as_str)
                .ok_or("missing account")?,
        )?;
        let name = unhex(
            row.get("name_hex")
                .and_then(serde_json::Value::as_str)
                .ok_or("missing name")?,
        )?;
        let kind = ResourceKind::try_from(u8::try_from(
            row.get("kind")
                .and_then(serde_json::Value::as_u64)
                .ok_or("missing kind")?,
        )?)?;
        let resource = ResourceName::new(&account, kind, &name)?;
        let row = row.as_object_mut().ok_or("vector must be an object")?;
        row.insert("preimage_hex".into(), hex(&resource.encode()).into());
        row.insert("address_hex".into(), resource.address().to_string().into());
    }
    std::fs::write(path, format!("{}\n", serde_json::to_string_pretty(&file)?))?;
    Ok(())
}
