use super::*;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};
use std::fmt::Write as _;
use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

// Exact NDJSON protocol of Cloud's existing customer-holder-fixture example.
// This SDK test neither signs certificates nor owns a Root issuer implementation.
#[derive(Serialize)]
#[serde(tag = "operation", rename_all = "snake_case", rename_all_fields = "camelCase")]
enum Request<'a> {
    Issue {
        account_id: &'a str, public_key: &'a str, certificate_lifetime_seconds: u64,
        key_id: &'a str, credential_id: &'a str,
    },
    Verify {
        bearer: &'a str, expected_public_key: &'a str, expected_account_id: &'a str,
        revoked: bool, foreign_root: bool, now_epoch_milliseconds: Option<u64>,
    },
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", rename_all_fields = "camelCase")]
enum Reply {
    Issued {
        mode: String, account_id: String, public_key: String, key_id: String,
        credential_id: String, birth: String, certificate: String,
        certificate_expires_at_epoch_milliseconds: u64,
    },
    Verified { mode: String, account_id: String, key_id: String, credential_id: String },
    Refused { mode: String, reason: String },
}

struct PublicFixture {
    birth: String,
    certificate: String,
    expires_ms: u64,
}

struct ServerFixture {
    child: Child,
    input: ChildStdin,
    output: BufReader<ChildStdout>,
}

impl ServerFixture {
    fn start() -> Self {
        // The explicit executable path is public qualification configuration, not a
        // secret, provider CLI, ambient login cache, downloaded helper, or vault fallback.
        let executable = std::env::var_os("ACYCLIC_CUSTOMER_HOLDER_FIXTURE")
            .expect("set the path of the actual source-bound Cloud customer-holder-fixture executable");
        let mut child = Command::new(executable).stdin(Stdio::piped()).stdout(Stdio::piped())
            .stderr(Stdio::inherit()).spawn().expect("launch actual canonical server fixture");
        let input = child.stdin.take().expect("fixture stdin");
        let output = BufReader::new(child.stdout.take().expect("fixture stdout"));
        Self { child, input, output }
    }

    fn request(&mut self, request: Request<'_>) -> Reply {
        // Serialize borrowed bearer bytes directly to the pipe, never to logs or JS.
        serde_json::to_writer(&mut self.input, &request).expect("fixture request");
        self.input.write_all(b"\n").expect("fixture request boundary");
        self.input.flush().expect("fixture request flush");
        let mut line = String::new();
        assert!(self.output.read_line(&mut line).expect("fixture response") > 0);
        serde_json::from_str(&line).expect("actual fixture response schema")
    }

    fn issue(&mut self, account: &str, public: &str, key_id: &str, jti: &str, lifetime: u64) -> PublicFixture {
        match self.request(Request::Issue {
            account_id: account, public_key: public, certificate_lifetime_seconds: lifetime,
            key_id, credential_id: jti,
        }) {
            Reply::Issued { mode, account_id, public_key, key_id: returned_key, credential_id, birth,
                certificate, certificate_expires_at_epoch_milliseconds } => {
                assert!(mode == "local_conformance_only" && account_id == account && public_key == public
                    && returned_key == key_id && credential_id == jti);
                PublicFixture { birth, certificate, expires_ms: certificate_expires_at_epoch_milliseconds }
            }
            _ => panic!("actual server fixture did not issue the requested own-leaf certificate"),
        }
    }

    fn verify(&mut self, issued: &IssuedCredential, account: &str, public: &str, key_id: &str,
        jti: &str, revoked: bool, foreign_root: bool, accepted: bool) {
        match self.request(Request::Verify {
            bearer: issued.bearer(), expected_public_key: public, expected_account_id: account,
            revoked, foreign_root, now_epoch_milliseconds: None,
        }) {
            Reply::Verified { mode, account_id, key_id: actual_key, credential_id } => {
                assert!(accepted && mode == "local_conformance_only" && account_id == account
                    && actual_key == key_id && credential_id == jti);
            }
            Reply::Refused { mode, reason } => {
                assert!(!accepted && mode == "local_conformance_only" && !reason.is_empty());
            }
            _ => panic!("unexpected actual server verification response"),
        }
    }
}

impl Drop for ServerFixture {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

struct ActualClock;
impl Clock for ActualClock {
    fn now_ms(&self) -> Result<u64, AccountHolderError> {
        let millis = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| AccountHolderError::Expired)?.as_millis();
        millis.try_into().map_err(|_| AccountHolderError::Expired)
    }
}

struct Cleanup(CustomerCustodyNamespace);
impl Drop for Cleanup {
    fn drop(&mut self) {
        if let Ok(_lock) = lock::NamespaceLock::acquire(&self.0.name) {
            let _ = platform::delete(&self.0.name);
        }
    }
}

/// Actual vault and canonical Rust server crypto, using a clearly local test-only Root.
/// Not live Identity, a production account, an installed-public-artifact, or other-OS proof.
#[test]
#[ignore = "requires a real OS vault and source-bound Cloud customer-holder-fixture executable"]
fn real_os_restored_holder_and_canonical_server_conformance() {
    let mut nonce = [0u8; 16];
    getrandom::fill(&mut nonce).expect("actual OS random source");
    nonce[6] = (nonce[6] & 0x0f) | 0x40;
    nonce[8] = (nonce[8] & 0x3f) | 0x80;
    let mut hex = String::with_capacity(32);
    for byte in nonce { write!(hex, "{byte:02x}").expect("public namespace hex"); }
    let account = format!("{}-{}-{}-{}-{}", &hex[..8], &hex[8..12], &hex[12..16], &hex[16..20], &hex[20..]);
    let key_id = format!("native-leaf-{hex}");
    let jti = "native-custody-conformance";
    let namespace = CustomerCustodyNamespace::new("https://custody-conformance.invalid", "staging", &account, &key_id).expect("own isolated namespace");
    let _cleanup = Cleanup(namespace.clone());
    let mut server = ServerFixture::start();
    let pending = PendingCustomerLeaf::generate().expect("actual own leaf");
    let public = URL_SAFE_NO_PAD.encode(pending.public_key());
    let fixture = server.issue(&account, &public, &key_id, jti, 30);
    let stored = pending.commit(namespace.clone(), &fixture.birth, &fixture.certificate,
        Zeroizing::new("explicit-local-conformance-sql-session".into())).expect("actual bound OS commit");
    drop(stored);
    let restored = RestoredCustomerLeaf::open(namespace.clone()).expect("actual OS reopen");
    assert!(URL_SAFE_NO_PAD.encode(restored.public_key()) == public);
    let issued = restored.mint(&fixture.birth, &fixture.certificate, jti, &ActualClock, 60)
        .expect("actual canonical bearer from vault-restored own leaf");
    assert!(issued.expires_at_unix_millis() == fixture.expires_ms
        && issued.certificate_expires_at_unix_millis() == fixture.expires_ms);
    server.verify(&issued, &account, &public, &key_id, jti, false, false, true);
    server.verify(&issued, &account, &public, &key_id, jti, true, false, false);
    server.verify(&issued, &account, &public, &key_id, jti, false, true, false);

    let foreign = PendingCustomerLeaf::generate().expect("foreign own test leaf");
    let foreign_public = URL_SAFE_NO_PAD.encode(foreign.public_key());
    let foreign_key_id = format!("foreign-leaf-{hex}");
    let foreign_fixture = server.issue(&account, &foreign_public, &foreign_key_id, jti, 30);
    assert!(matches!(restored.mint(&foreign_fixture.birth, &foreign_fixture.certificate, jti, &ActualClock, 60),
        Err(CustodyError::Holder(AccountHolderError::Scope))));
    assert!(matches!(foreign.commit(namespace.clone(), &fixture.birth, &fixture.certificate,
        Zeroizing::new("not-stored".into())), Err(CustodyError::Holder(AccountHolderError::Scope))));
    restored.with_sql_session(|session| assert!(session == "explicit-local-conformance-sql-session"))
        .expect("original survives foreign-certificate replacement");
    let still_issued = restored.mint(&fixture.birth, &fixture.certificate, jti, &ActualClock, 60)
        .expect("preserved original still signs a usable local conformance bearer");
    server.verify(&still_issued, &account, &public, &key_id, jti, false, false, true);

    let renewed_key_id = format!("renewed-leaf-{hex}");
    let renewed_namespace = CustomerCustodyNamespace::new("https://custody-conformance.invalid", "staging", &account, &renewed_key_id).expect("renewed namespace");
    let _renewed_cleanup = Cleanup(renewed_namespace.clone());
    let renewed_fixture = server.issue(&account, &public, &renewed_key_id, jti, 30);
    for changed_scope in [
        CustomerCustodyNamespace::new("https://other-conformance.invalid", "staging", &account, &renewed_key_id),
        CustomerCustodyNamespace::new("https://custody-conformance.invalid", "production", &account, &renewed_key_id),
        CustomerCustodyNamespace::new("https://custody-conformance.invalid", "staging", &format!("{account}-other"), &renewed_key_id),
    ] {
        assert!(matches!(restored.recertify(changed_scope.expect("changed namespace"),
            &renewed_fixture.birth, &renewed_fixture.certificate), Err(CustodyError::Binding)));
    }
    assert!(matches!(restored.recertify(renewed_namespace.clone(), &foreign_fixture.birth,
        &foreign_fixture.certificate), Err(CustodyError::Holder(AccountHolderError::Scope))));
    let renewed = restored.recertify(renewed_namespace.clone(), &renewed_fixture.birth,
        &renewed_fixture.certificate).expect("same private leaf and SQL pair under actual renewed certificate key ID");
    assert!(renewed.public_key() == restored.public_key());
    renewed.with_sql_session(|session| assert!(session == "explicit-local-conformance-sql-session"))
        .expect("renewal never exposes or loses the original SQL session");
    restored.with_sql_session(|_| ()).expect("original namespace remains until explicit deletion");
    let renewed_bearer = renewed.mint(&renewed_fixture.birth, &renewed_fixture.certificate, jti, &ActualClock, 60)
        .expect("actual bearer after key-ID renewal");
    server.verify(&renewed_bearer, &account, &public, &renewed_key_id, jti, false, false, true);
    let current_renewed = renewed.recertify(renewed_namespace.clone(), &renewed_fixture.birth,
        &renewed_fixture.certificate).expect("same-namespace renewal does not deadlock");
    assert!(renewed.generation == current_renewed.generation);
    renewed.with_sql_session(|_| ()).expect("same-namespace retry preserves the actual existing generation");
    restored.delete().expect("explicit original deletion after new tuple is available");
    assert!(matches!(restored.with_sql_session(|_| ()), Err(CustodyError::NotFound)));
    assert!(matches!(restored.recertify(renewed_namespace.clone(), &renewed_fixture.birth,
        &renewed_fixture.certificate), Err(CustodyError::NotFound)));
    current_renewed.with_sql_session(|session| assert!(session == "explicit-local-conformance-sql-session"))
        .expect("failed stale source renewal preserves the current new pair");

    let short = PendingCustomerLeaf::generate().expect("short-window own test leaf");
    let short_public = URL_SAFE_NO_PAD.encode(short.public_key());
    let short_key_id = format!("short-leaf-{hex}");
    let short_namespace = CustomerCustodyNamespace::new("https://custody-conformance.invalid", "staging", &account, &short_key_id).expect("short-window namespace");
    let _short_cleanup = Cleanup(short_namespace.clone());
    let short_fixture = server.issue(&account, &short_public, &short_key_id, jti, 3);
    let short_handle = short.commit(short_namespace.clone(), &short_fixture.birth, &short_fixture.certificate,
        Zeroizing::new("explicit-local-conformance-renewal-session".into())).expect("short-window actual OS commit");
    let short_bearer = short_handle.mint(&short_fixture.birth, &short_fixture.certificate, jti, &ActualClock, 60)
        .expect("actual certificate-bounded bearer");
    server.verify(&short_bearer, &account, &short_public, &short_key_id, jti, false, false, true);
    drop(short_handle);
    let remaining = short_fixture.expires_ms.saturating_sub(ActualClock.now_ms().expect("actual current clock"));
    std::thread::sleep(std::time::Duration::from_millis(remaining + 20));
    let expired = RestoredCustomerLeaf::open(short_namespace.clone()).expect("expired certificate does not destroy SQL renewal custody");
    expired.with_sql_session(|session| assert!(session == "explicit-local-conformance-renewal-session"))
        .expect("expired own leaf retains distinct opaque SQL renewal session");
    assert!(matches!(expired.mint(&short_fixture.birth, &short_fixture.certificate, jti, &ActualClock, 60),
        Err(CustodyError::Holder(AccountHolderError::Expired))));
    server.verify(&short_bearer, &account, &short_public, &short_key_id, jti, false, false, false);
    expired.delete().expect("delete expired own custody");
    current_renewed.delete().expect("delete locally verified renewed own custody");
    assert!(matches!(RestoredCustomerLeaf::open(namespace), Err(CustodyError::NotFound)));
    assert!(matches!(RestoredCustomerLeaf::open(renewed_namespace), Err(CustodyError::NotFound)));
    assert!(matches!(RestoredCustomerLeaf::open(short_namespace), Err(CustodyError::NotFound)));
}

/// Lost acknowledgements use actual OS entries and actual server certificates, never
/// a mock store, cached private snapshot, or fabricated missing-entry result.
#[test]
#[ignore = "requires a real OS vault and source-bound Cloud customer-holder-fixture executable"]
fn real_os_recertification_lost_acknowledgement_and_foreign_replacement() {
    let mut nonce = [0u8; 16];
    getrandom::fill(&mut nonce).expect("actual OS random source");
    nonce[6] = (nonce[6] & 0x0f) | 0x40;
    nonce[8] = (nonce[8] & 0x3f) | 0x80;
    let mut hex = String::with_capacity(32);
    for byte in nonce { write!(hex, "{byte:02x}").expect("public namespace hex"); }
    let account = format!("{}-{}-{}-{}-{}", &hex[..8], &hex[8..12], &hex[12..16], &hex[16..20], &hex[20..]);
    let key_id = format!("lost-ack-old-{hex}");
    let new_key_id = format!("lost-ack-new-{hex}");
    let jti = "native-custody-lost-ack";
    let origin = "https://custody-lost-ack.invalid";
    let original_namespace = CustomerCustodyNamespace::new(origin, "staging", &account, &key_id).expect("original namespace");
    let destination = CustomerCustodyNamespace::new(origin, "staging", &account, &new_key_id).expect("renewed namespace");
    let _original_cleanup = Cleanup(original_namespace.clone());
    let _destination_cleanup = Cleanup(destination.clone());
    let mut server = ServerFixture::start();
    let pending = PendingCustomerLeaf::generate().expect("own leaf");
    let public = URL_SAFE_NO_PAD.encode(pending.public_key());
    let original_fixture = server.issue(&account, &public, &key_id, jti, 30);
    let renewed_fixture = server.issue(&account, &public, &new_key_id, jti, 30);
    let original = pending.commit(original_namespace.clone(), &original_fixture.birth,
        &original_fixture.certificate, Zeroizing::new("lost-ack-original-sql-session".into()))
        .expect("actual original pair commit");
    let original_receipt = original.reference().encode();
    let saved_original = CustomerCustodyReference::decode(&original_receipt).expect("canonical original public receipt");
    assert!(saved_original.encode() == original_receipt);
    let fenced_original = RestoredCustomerLeaf::open_at(original_namespace.clone(), &saved_original)
        .expect("actual original generation reopened with persisted nonsecret receipt");
    assert!(fenced_original.public_key() == original.public_key());
    for malformed in [
        String::new(),
        original_receipt.replacen("v1:", "v2:", 1),
        format!("{original_receipt}="),
        format!("{original_receipt}{}", "A".repeat(4096)),
    ] {
        assert!(matches!(CustomerCustodyReference::decode(&malformed), Err(CustodyError::InvalidReference)));
    }
    let mut bad_alphabet = original_receipt.clone();
    bad_alphabet.replace_range(REFERENCE_PREFIX.len()..REFERENCE_PREFIX.len() + 1, "+");
    assert!(matches!(CustomerCustodyReference::decode(&bad_alphabet), Err(CustodyError::InvalidReference)));
    assert!(matches!(RestoredCustomerLeaf::open_at(destination.clone(), &saved_original), Err(CustodyError::Binding)));
    let mut wrong_public = CustomerCustodyReference::decode(&original_receipt).expect("decode public receipt");
    wrong_public.public_key[0] ^= 1;
    assert!(matches!(RestoredCustomerLeaf::open_at(original_namespace.clone(), &wrong_public), Err(CustodyError::Stale)));
    let mut wrong_generation = CustomerCustodyReference::decode(&original_receipt).expect("decode generation receipt");
    wrong_generation.generation[0] ^= 1;
    assert!(matches!(RestoredCustomerLeaf::open_at(original_namespace.clone(), &wrong_generation), Err(CustodyError::Stale)));
    let acknowledged = original.recertify(destination.clone(), &renewed_fixture.birth,
        &renewed_fixture.certificate).expect("first real recertification");
    let actual_generation = acknowledged.generation;
    let destination_receipt = acknowledged.reference().encode();
    let saved_destination = CustomerCustodyReference::decode(&destination_receipt).expect("persist actual acknowledged generation");
    drop(acknowledged); // The durable write succeeded, but the caller lost its reply.
    let reopened = RestoredCustomerLeaf::open_at(destination.clone(), &saved_destination)
        .expect("fenced reopen after lost acknowledgement");
    let retry = original.recertify(destination.clone(), &renewed_fixture.birth,
        &renewed_fixture.certificate).expect("retry from the same original handle");
    assert!(retry.generation == actual_generation && reopened.generation == actual_generation);
    reopened.with_sql_session(|session| assert!(session == "lost-ack-original-sql-session"))
        .expect("retry did not rewrite the entry or invalidate an existing destination handle");
    let still_acknowledged = RestoredCustomerLeaf::open_at(destination.clone(), &saved_destination)
        .expect("lost-ack retry retained the persisted destination receipt");
    assert!(still_acknowledged.generation == actual_generation);
    let bearer = retry.mint(&renewed_fixture.birth, &renewed_fixture.certificate, jti, &ActualClock, 60)
        .expect("real restored holder after retry");
    server.verify(&bearer, &account, &public, &new_key_id, jti, false, false, true);
    let same_namespace = retry.recertify(destination.clone(), &renewed_fixture.birth,
        &renewed_fixture.certificate).expect("same-namespace acknowledgement retry");
    assert!(same_namespace.generation == actual_generation);
    retry.with_sql_session(|_| ()).expect("same namespace retained its actual generation");

    // A different SQL session is a different pair even when the private leaf is identical.
    let key = {
        let _lock = lock::NamespaceLock::acquire(&original_namespace.name).expect("original namespace lock");
        original.current().expect("actual original restored key").key
    };
    let changed_session = PendingCustomerLeaf { key }.commit(destination.clone(),
        &renewed_fixture.birth, &renewed_fixture.certificate,
        Zeroizing::new("lost-ack-replacement-sql-session".into())).expect("real same-leaf session replacement");
    assert!(matches!(original.recertify(destination.clone(), &renewed_fixture.birth,
        &renewed_fixture.certificate), Err(CustodyError::Stale)));
    changed_session.with_sql_session(|session| assert!(session == "lost-ack-replacement-sql-session"))
        .expect("retry did not overwrite a different destination pair");
    assert!(matches!(retry.delete(), Err(CustodyError::Stale)));
    assert!(matches!(RestoredCustomerLeaf::open_at(destination.clone(), &saved_destination), Err(CustodyError::Stale)));
    assert!(matches!(reopened.with_sql_session(|_| ()), Err(CustodyError::Stale)));

    // Simulate a foreign writer at the actual vault boundary using the same private
    // pair writer as production. This claims no certificate authorization for that key.
    let foreign = PendingCustomerLeaf::generate().expect("foreign leaf");
    let foreign_public = foreign.public_key();
    let foreign_handle = foreign.store_pair(destination.clone(),
        Zeroizing::new("lost-ack-foreign-sql-session".into())).expect("actual foreign OS replacement");
    assert!(matches!(original.recertify(destination.clone(), &renewed_fixture.birth,
        &renewed_fixture.certificate), Err(CustodyError::Stale)));
    assert!(matches!(changed_session.delete(), Err(CustodyError::Stale)));
    assert!(matches!(RestoredCustomerLeaf::open_at(destination.clone(), &saved_destination), Err(CustodyError::Stale)));
    let observed_foreign = RestoredCustomerLeaf::open(destination.clone()).expect("foreign entry remains");
    assert!(observed_foreign.public_key() == foreign_public);
    observed_foreign.with_sql_session(|session| assert!(session == "lost-ack-foreign-sql-session"))
        .expect("stale destination deletion did not remove the foreign pair");
    assert!(matches!(observed_foreign.mint(&renewed_fixture.birth, &renewed_fixture.certificate,
        jti, &ActualClock, 60), Err(CustodyError::Holder(AccountHolderError::Scope))));
    foreign_handle.delete().expect("actual foreign entry deletion");
    foreign_handle.delete().expect("lost delete acknowledgement is idempotent only after physical absence");
    assert!(matches!(RestoredCustomerLeaf::open(destination.clone()), Err(CustodyError::NotFound)));
    assert!(matches!(RestoredCustomerLeaf::open_at(destination.clone(), &saved_destination), Err(CustodyError::NotFound)));
    let recreated = original.recertify(destination.clone(), &renewed_fixture.birth,
        &renewed_fixture.certificate).expect("only true OS absence permits recreation");
    assert!(matches!(RestoredCustomerLeaf::open_at(destination.clone(), &saved_destination), Err(CustodyError::Stale)));
    let recreated_receipt = recreated.reference().encode();
    let saved_recreated = CustomerCustodyReference::decode(&recreated_receipt).expect("actual recreated reference");
    RestoredCustomerLeaf::open_at(destination.clone(), &saved_recreated).expect("new acknowledged reference reopens actual recreated generation");
    let recreated_bearer = recreated.mint(&renewed_fixture.birth, &renewed_fixture.certificate, jti, &ActualClock, 60)
        .expect("actual recreated holder");
    server.verify(&recreated_bearer, &account, &public, &new_key_id, jti, false, false, true);

    // A corrupt existing entry is not absence and must not be overwritten by retry.
    {
        let _lock = lock::NamespaceLock::acquire(&destination.name).expect("destination lock");
        let mut damaged = platform::read(&destination.name).expect("actual destination bytes");
        damaged[0] ^= 1;
        platform::write(&destination.name, &damaged).expect("actual isolated corrupted entry");
    }
    assert!(matches!(original.recertify(destination.clone(), &renewed_fixture.birth,
        &renewed_fixture.certificate), Err(CustodyError::Corrupt)));
    assert!(matches!(recreated.delete(), Err(CustodyError::Corrupt)));
    assert!(matches!(RestoredCustomerLeaf::open(destination.clone()), Err(CustodyError::Corrupt)));
    assert!(matches!(RestoredCustomerLeaf::open_at(destination.clone(), &saved_recreated), Err(CustodyError::Corrupt)));
    {
        let _lock = lock::NamespaceLock::acquire(&destination.name).expect("destination lock");
        platform::delete(&destination.name).expect("remove only the isolated corrupt test entry");
    }
    let current = original.recertify(destination.clone(), &renewed_fixture.birth,
        &renewed_fixture.certificate).expect("recreate after explicit physical corrupt-entry removal");

    // Check the original generation before considering even an identical destination.
    let source_replacement = PendingCustomerLeaf::generate().expect("foreign original replacement")
        .store_pair(original_namespace.clone(), Zeroizing::new("lost-ack-foreign-original-session".into()))
        .expect("actual original namespace replacement");
    assert!(matches!(original.recertify(destination.clone(), &renewed_fixture.birth,
        &renewed_fixture.certificate), Err(CustodyError::Stale)));
    assert!(matches!(RestoredCustomerLeaf::open_at(original_namespace.clone(), &saved_original), Err(CustodyError::Stale)));
    assert!(matches!(original.delete(), Err(CustodyError::Stale)));
    assert!(matches!(fenced_original.delete(), Err(CustodyError::Stale)));
    source_replacement.with_sql_session(|session| assert!(session == "lost-ack-foreign-original-session"))
        .expect("stale original delete did not remove the replacement");
    source_replacement.delete().expect("remove actual original replacement");
    original.delete().expect("physically absent original deletion is idempotent");
    assert!(matches!(RestoredCustomerLeaf::open_at(original_namespace.clone(), &saved_original), Err(CustodyError::NotFound)));
    assert!(matches!(original.recertify(destination.clone(), &renewed_fixture.birth,
        &renewed_fixture.certificate), Err(CustodyError::NotFound)));
    current.with_sql_session(|session| assert!(session == "lost-ack-original-sql-session"))
        .expect("absent original never triggers a cached-snapshot overwrite");
    let current_bearer = current.mint(&renewed_fixture.birth, &renewed_fixture.certificate, jti, &ActualClock, 60)
        .expect("destination remains usable after refused stale retries");
    server.verify(&current_bearer, &account, &public, &new_key_id, jti, false, false, true);
    current.delete().expect("actual destination deletion");
    current.delete().expect("lost destination delete acknowledgement is idempotent");
    assert!(matches!(RestoredCustomerLeaf::open(original_namespace), Err(CustodyError::NotFound)));
    assert!(matches!(RestoredCustomerLeaf::open(destination), Err(CustodyError::NotFound)));
}
