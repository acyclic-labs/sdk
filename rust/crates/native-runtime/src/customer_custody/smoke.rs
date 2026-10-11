use super::*;
use ed25519_dalek::{Signer, Verifier};

struct Cleanup(CustomerCustodyNamespace);
impl Drop for Cleanup {
    fn drop(&mut self) {
        if let Ok(_lock) = lock::NamespaceLock::acquire(&self.0.name) {
            let _ = platform::delete(&self.0.name);
        }
    }
}

/// Real OS-vault qualification, not an issuer fixture, mock store, or Cloud bearer proof.
/// Canonical usable-bearer qualification additionally requires the live Cloud issuer to
/// certify this fresh public key; never substitute a test Root for that prerequisite.
#[test]
#[ignore = "requires the real logged-in OS user's unlocked credential vault"]
fn real_os_custody_create_reopen_sign_delete_and_failures() {
    let mut nonce = [0u8; 16];
    getrandom::fill(&mut nonce).expect("OS random source");
    let mut suffix = String::with_capacity(32);
    use std::fmt::Write;
    for byte in nonce {
        write!(suffix, "{byte:02x}").expect("namespace hex");
    }
    let account = format!("custody-smoke-{suffix}");
    let namespace = CustomerCustodyNamespace::new("https://custody-smoke.invalid", "production", &account, "leaf-smoke").expect("isolated namespace");
    let _cleanup = Cleanup(namespace.clone());
    assert!(matches!(RestoredCustomerLeaf::open(namespace.clone()), Err(CustodyError::NotFound)));

    let pending = PendingCustomerLeaf::generate().expect("OS random own leaf");
    let public_key = pending.public_key();
    // Invoke the same private pair writer used after commit's public certificate binding.
    // No certificate is fabricated and no authorization or usable bearer is claimed here.
    let stored = pending.store_pair(namespace.clone(), Zeroizing::new("isolated-smoke-sql-session".to_owned())).expect("actual OS store");
    drop(stored);
    let restored = RestoredCustomerLeaf::open(namespace.clone()).expect("actual OS reopen");
    assert!(restored.public_key() == public_key);
    restored.with_sql_session(|session| assert!(session == "isolated-smoke-sql-session")).expect("distinct SQL session custody");

    // Real Ed25519 signing from the actual vault-restored key, not an echo comparison.
    let record = {
        let _lock = lock::NamespaceLock::acquire(&namespace.name).expect("namespace lock");
        restored.current().expect("actual restored key")
    };
    let challenge = b"acyclic-real-native-customer-custody-signature-v1";
    let signature = record.key.sign(challenge);
    let public = VerifyingKey::from_bytes(&public_key).expect("exportable public key");
    public.verify_strict(challenge, &signature).expect("actual restored leaf signature");
    assert!(public.verify(b"different-message", &signature).is_err());
    drop(record);

    let bad_binding = PendingCustomerLeaf::generate().expect("replacement leaf");
    assert!(matches!(bad_binding.commit(namespace.clone(), "not-a-birth", "not-a-certificate", Zeroizing::new("not-stored".into())), Err(CustodyError::Holder(_))));
    let too_large = PendingCustomerLeaf::generate().expect("replacement leaf");
    assert!(matches!(too_large.store_pair(namespace.clone(), Zeroizing::new("x".repeat(MAX_RECORD_BYTES))), Err(CustodyError::TooLarge)));
    let empty = PendingCustomerLeaf::generate().expect("replacement leaf");
    assert!(matches!(empty.store_pair(namespace.clone(), Zeroizing::new(String::new())), Err(CustodyError::InvalidSession)));
    restored.with_sql_session(|session| assert!(session == "isolated-smoke-sql-session")).expect("original survives failed replacements");
    assert!(RestoredCustomerLeaf::open(namespace.clone()).expect("preserved original leaf").public_key() == public_key);

    // Origin/environment/account/credential each independently selects a different item.
    for isolated in [
        CustomerCustodyNamespace::new("https://other-custody-smoke.invalid", "production", &account, "leaf-smoke"),
        CustomerCustodyNamespace::new("https://custody-smoke.invalid", "staging", &account, "leaf-smoke"),
        CustomerCustodyNamespace::new("https://custody-smoke.invalid", "production", &format!("{account}-other"), "leaf-smoke"),
        CustomerCustodyNamespace::new("https://custody-smoke.invalid", "production", &account, "leaf-smoke-other"),
    ] {
        assert!(matches!(RestoredCustomerLeaf::open(isolated.expect("isolated namespace")), Err(CustodyError::NotFound)));
    }

    let replacement = PendingCustomerLeaf::generate().expect("new own leaf");
    let replacement_public = replacement.public_key();
    let new_handle = replacement.store_pair(namespace.clone(), Zeroizing::new("isolated-new-sql-session".into())).expect("atomic actual OS replacement");
    assert!(matches!(restored.with_sql_session(|_| ()), Err(CustodyError::Stale)));
    assert!(matches!(restored.delete(), Err(CustodyError::Stale)));
    let reopened = RestoredCustomerLeaf::open(namespace.clone()).expect("new login survives stale deletion");
    assert!(reopened.public_key() == replacement_public);
    reopened.with_sql_session(|session| assert!(session == "isolated-new-sql-session")).expect("consistent replacement pair");
    let first = PendingCustomerLeaf::generate().expect("concurrent leaf one");
    let first_public = first.public_key();
    let second = PendingCustomerLeaf::generate().expect("concurrent leaf two");
    let second_public = second.public_key();
    let barrier = std::sync::Barrier::new(2);
    let (first_handle, second_handle) = std::thread::scope(|scope| {
        let first_writer = scope.spawn(|| {
            barrier.wait();
            first.store_pair(namespace.clone(), Zeroizing::new("isolated-concurrent-one".into()))
        });
        let second_writer = scope.spawn(|| {
            barrier.wait();
            second.store_pair(namespace.clone(), Zeroizing::new("isolated-concurrent-two".into()))
        });
        (first_writer.join().expect("first custody writer").expect("first OS write"),
         second_writer.join().expect("second custody writer").expect("second OS write"))
    });
    let current = RestoredCustomerLeaf::open(namespace.clone()).expect("concurrent consistent pair");
    let expected_session = if current.public_key() == first_public {
        assert!(matches!(second_handle.with_sql_session(|_| ()), Err(CustodyError::Stale)));
        first_handle.with_sql_session(|_| ()).expect("first handle remains current");
        "isolated-concurrent-one"
    } else {
        assert!(current.public_key() == second_public);
        assert!(matches!(first_handle.with_sql_session(|_| ()), Err(CustodyError::Stale)));
        second_handle.with_sql_session(|_| ()).expect("second handle remains current");
        "isolated-concurrent-two"
    };
    current.with_sql_session(|session| assert!(session == expected_session)).expect("no concurrent key/session mismatch");
    assert!(matches!(new_handle.delete(), Err(CustodyError::Stale)));
    current.delete().expect("actual OS deletion");
    assert!(matches!(reopened.with_sql_session(|_| ()), Err(CustodyError::NotFound)));
    assert!(matches!(RestoredCustomerLeaf::open(namespace), Err(CustodyError::NotFound)));
}
