#![allow(missing_docs)]

use acyclic_machines::{
    MANAGED_OCI_DIGEST_HEX_LENGTH, MANAGED_OCI_HEX_DIGITS, MANAGED_OCI_REFERENCE_SEPARATOR,
    MANAGED_OCI_ZERO_DIGIT, MAX_EVENT_PAGE_SIZE, MAX_PAGE_SIZE,
};

fn main() {
    println!(
        "{}",
        serde_json::json!({
            "separator": MANAGED_OCI_REFERENCE_SEPARATOR,
            "digestHexLength": MANAGED_OCI_DIGEST_HEX_LENGTH,
            "hexDigits": MANAGED_OCI_HEX_DIGITS,
            "zeroDigit": MANAGED_OCI_ZERO_DIGIT.to_string(),
            "maxPageSize": MAX_PAGE_SIZE,
            "maxEventPageSize": MAX_EVENT_PAGE_SIZE,
        })
    );
}
