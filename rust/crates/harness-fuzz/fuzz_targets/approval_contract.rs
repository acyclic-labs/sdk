#![no_main]

use acyclic_harness::{
    executor::json::{JsonBounds, JsonValueSchema, json_from_slice_bounded},
    interaction::{ApprovalDecisionFile, Interaction},
};
use libfuzzer_sys::fuzz_target;

// Both approval file bodies use their production parsers. Accepted request
// arguments survive the native schema and serialization without substitution.
fuzz_target!(|bytes: &[u8]| {
    if bytes.len() > 4096 {
        return;
    }
    if let Ok(request @ Interaction::Approval { .. }) = request(bytes)
        && request.validate(&mut None).is_ok()
    {
        let encoded = serde_json::to_vec(&request).unwrap();
        assert_eq!(self::request(&encoded).unwrap(), request);
    }
    let Ok(decision) = ApprovalDecisionFile::decode(bytes, 4096) else {
        return;
    };
    assert!(bytes.len() <= 4096);
    decision.validate().unwrap();
    let encoded = serde_json::to_vec(&decision).unwrap();
    let decoded = ApprovalDecisionFile::decode(&encoded, encoded.len() as u64).unwrap();
    assert_eq!(decoded, decision);
    assert!(ApprovalDecisionFile::decode(&encoded, encoded.len() as u64 - 1).is_err());
    let mut sha = [0_u8; 32];
    for (index, byte) in sha.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&decision.request_sha256[index * 2..index * 2 + 2], 16).unwrap();
    }
    assert!(decision.binds(&sha, decision.expected_version));
    assert!(!decision.binds(&sha, decision.expected_version.wrapping_add(1)));
    let file = ApprovalDecisionFile::principal_file(&decision.principal);
    assert_eq!(file.len(), 32);
    assert!(
        file.bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    );
});

fn request(bytes: &[u8]) -> acyclic_harness::Result<Interaction> {
    json_from_slice_bounded(
        std::marker::PhantomData::<Interaction>,
        bytes,
        JsonBounds {
            maximum_bytes: bytes.len() as u64,
            maximum_items: bytes.len() as u64,
            maximum_depth: 256,
        },
        bytes.len() as u64,
        Some(acyclic_harness::wire::ErrorCode::Invalid),
        Some(JsonValueSchema::Interaction),
        &mut |_| Ok(()),
    )
}
