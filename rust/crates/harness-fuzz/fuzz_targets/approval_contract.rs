#![no_main]

use acyclic_harness::interaction::ApprovalDecisionFile;
use libfuzzer_sys::fuzz_target;

// The approver-written decision body is the only parser on the approval file
// surface. Accepted bodies are canonical, bounded and bind exactly one cut.
fuzz_target!(|bytes: &[u8]| {
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
