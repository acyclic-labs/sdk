//! Resource and semantic action grammar. Canonicalization belongs to the owner.

/// Filesystem actions shared with the filesystem access decision model.
pub const FS_ACTIONS: &[&str] = &[
    "list",
    "read",
    "create",
    "write",
    "append",
    "delete",
    "rename",
    "fork",
    "merge-into",
    "read-history",
    "lock",
    "admin",
];

/// Resource components are lossless bytes: unreserved ASCII is literal and all
/// other bytes use uppercase %HH. Owners canonicalize hierarchical names first;
/// Objects encodes its entire opaque key as ONE component without normalization.
/// Literal '/' separates components; only a final empty prefix is permitted.
pub fn valid_resource(value: &str) -> bool {
    let Some((product, path)) = value.split_once(":/") else {
        return false;
    };
    valid_product(product)
        && (path.is_empty()
            || path.split('/').enumerate().all(|(i, part)| {
                (!part.is_empty() || i + 1 == path.split('/').count())
                    && decode_component(part).is_ok()
            }))
}

fn valid_product(value: &str) -> bool {
    !value.is_empty() && value.bytes().all(|b| b.is_ascii_lowercase() || b == b'-')
}

fn unreserved(b: u8) -> bool {
    b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'~')
}

/// Injective byte encoding. In particular dot, slash, percent and Unicode bytes
/// are escaped; no Unicode normalization or case folding occurs here.
pub fn encode_component(value: &[u8]) -> String {
    let mut result = String::new();
    const HEX: &[u8] = b"0123456789ABCDEF";
    for &b in value {
        if unreserved(b) {
            result.push(char::from(b));
        } else {
            result.push('%');
            result.push(char::from(HEX[usize::from(b >> 4)]));
            result.push(char::from(HEX[usize::from(b & 15)]));
        }
    }
    result
}

/// Decode only the unique canonical spelling; reject alternate escapes.
pub fn decode_component(value: &str) -> Result<Vec<u8>, crate::Error> {
    let mut bytes = value.bytes();
    let mut result = Vec::new();
    while let Some(b) = bytes.next() {
        if unreserved(b) {
            result.push(b);
        } else if b == b'%' {
            let hex = |b| match b {
                b'0'..=b'9' => Ok(b - b'0'),
                b'A'..=b'F' => Ok(b - b'A' + 10),
                _ => Err(crate::Error::Invalid),
            };
            let value = hex(bytes.next().ok_or(crate::Error::Invalid)?)? * 16
                + hex(bytes.next().ok_or(crate::Error::Invalid)?)?;
            if unreserved(value) {
                return Err(crate::Error::Invalid);
            }
            result.push(value);
        } else {
            return Err(crate::Error::Invalid);
        }
    }
    Ok(result)
}

/// Bucket and opaque key are separate components; slashes IN a key are %2F.
/// An encoded key prefix remains a byte prefix of all encoded matching keys.
pub fn objects_resource(bucket: &[u8], key: &[u8]) -> String {
    format!(
        "objects:/{}/{}",
        encode_component(bucket),
        encode_component(key)
    )
}

/// Product catalogs in descriptors define actions; new products need no verifier
/// deployment. Undeclared actions cannot authorize a declared operation.
pub fn valid_action(value: &str) -> bool {
    value.split_once('/').is_some_and(|(product, action)| {
        valid_product(product)
            && !action.is_empty()
            && action.bytes().all(|b| b.is_ascii_lowercase() || b == b'-')
    })
}
