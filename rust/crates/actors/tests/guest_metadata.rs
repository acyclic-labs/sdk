//! Downstream metadata construction, immutable fields and validating deserialization.
#[cfg(not(target_arch = "wasm32"))]
acyclic_test_support::counting_allocator!();
use acyclic_actors::guest::*;
use proptest::prelude::*;
use serde_json::json;
#[path = "support/metadata.rs"]
mod oracle;

fn caller(value: &str, field: usize, expiry: u64) -> Result<AdmittedCaller, GuestMetadataError> {
    let mut fields = ["a", "o", "p", "c", "i"];
    *fields
        .get_mut(field)
        .ok_or(GuestMetadataError("caller field"))? = value;
    let [account, org, principal, credential, actor] = fields;
    AdmittedCaller::new(
        account.into(),
        org.into(),
        principal.into(),
        credential.into(),
        actor.into(),
        expiry,
    )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    /// Owned metadata decoding allocates nothing and retains the original expiry.
    #[test]
    fn caller_admission_reuses_wire_strings(expiry in 1u64..=u64::MAX, principal in "[a-z]{1,64}", positional in any::<bool>(), capability in any::<bool>()) {
        let cap = if capability { json!([1, "cap"]) } else { serde_json::Value::Null };
        let raw = if positional {
            json!(["a", "o", principal, "c", "i", expiry.to_string(), cap])
        } else {
            json!({"accountId":"a","orgId":"o","principal":principal,"credentialId":"c","actorId":"i","expiresAtMs":expiry.to_string(),"capability":cap})
        };
        let key = if positional { raw.get(5) } else { raw.get("expiresAtMs") }.ok_or_else(|| TestCaseError::fail("expiry field"))?;
        let original = key.as_str().ok_or_else(|| TestCaseError::fail("expiry string"))?.as_ptr();
        let bytes = serde_json::to_vec(&raw)?;
        let mut decoded = None;
        let allocation = acyclic_test_support::measure(|| {
            decoded = Some(serde_json::from_value::<AdmittedCaller>(raw));
        });
        prop_assert_eq!((allocation.count_total, allocation.bytes_total), (0, 0));
        let decoded = decoded.ok_or_else(|| TestCaseError::fail("decode ran"))??;
        prop_assert_eq!(decoded.expires_at_ms().as_ptr(), original);
        prop_assert_eq!(decoded.expires_at_ms(), expiry.to_string());
        let mut from_bytes = None;
        let allocation = acyclic_test_support::measure(|| {
            from_bytes = Some(serde_json::from_slice::<AdmittedCaller>(&bytes));
        });
        prop_assert_eq!(allocation.count_total, 6 + u64::from(capability), "only the six wire strings and optional capability handle are allocated");
        prop_assert_eq!(from_bytes.ok_or_else(|| TestCaseError::fail("byte decode ran"))??, decoded);

        let fields = ["a".into(), "o".into(), principal, "c".into(), "i".into()];
        let [account, org, principal, credential, actor] = fields;
        let mut constructed = None;
        let allocation = acyclic_test_support::measure(|| {
            constructed = Some(AdmittedCaller::new(account, org, principal, credential, actor, expiry));
        });
        prop_assert_eq!(allocation.count_total, 1, "only the canonical expiry string is allocated");
        let constructed = constructed.ok_or_else(|| TestCaseError::fail("construction ran"))??;
        prop_assert_eq!(constructed.expires_at_ms(), expiry.to_string());
    }

    /// UTF-8 bytes, not character count, bound every constructor and serde route.
    #[test]
    fn metadata_boundaries_apply_to_native_and_wire(count in 0usize..=1100, multibyte in any::<bool>(), version in any::<u32>(), field in 0usize..5) {
        let text = if multibyte { "λ" } else { "a" }.repeat(count);
        let valid = oracle::text(&text);
        prop_assert_eq!(CallerCapability::new(version, text.clone()).is_ok(), valid && version != 0);
        prop_assert_eq!(RuntimeHandle::new(version, text.clone()).is_ok(), valid && version != 0);
        let handle = json!({"version":version,"handle":text,"future":true});
        prop_assert_eq!(serde_json::from_value::<CallerCapability>(handle.clone()).is_ok(), valid && version != 0);
        prop_assert_eq!(serde_json::from_value::<RuntimeHandle>(handle.clone()).is_ok(), valid && version != 0);
        oracle::check(&serde_json::to_vec(&handle)?);
        let positional = json!([version, text]);
        prop_assert_eq!(serde_json::from_value::<CallerCapability>(positional.clone()).is_ok(), valid && version != 0);
        prop_assert_eq!(serde_json::from_value::<RuntimeHandle>(positional.clone()).is_ok(), valid && version != 0);
        oracle::check(&serde_json::to_vec(&positional)?);
        let names = ["accountId","orgId","principal","credentialId","actorId"];
        let mut raw = json!({"accountId":"a","orgId":"o","principal":"p","credentialId":"c","actorId":"i","expiresAtMs":"1","capability":null,"future":true});
        let name = names.get(field).ok_or_else(|| TestCaseError::fail("caller field"))?;
        *raw.get_mut(*name).ok_or_else(|| TestCaseError::fail("metadata field"))? = text.clone().into();
        prop_assert_eq!(caller(&text, field, 1).is_ok(), valid);
        let decoded = serde_json::from_value::<AdmittedCaller>(raw.clone());
        prop_assert_eq!(decoded.is_ok(), valid);
        if let Ok(decoded) = decoded {
            prop_assert_eq!(serde_json::from_slice::<AdmittedCaller>(&serde_json::to_vec(&decoded)?)?, decoded);
        }
        oracle::check(&serde_json::to_vec(&raw)?);
        let fields = ["accountId", "orgId", "principal", "credentialId", "actorId"].map(|name| raw.get(name));
        let [Some(account), Some(org), Some(principal), Some(credential), Some(actor)] = fields else { return Err(TestCaseError::fail("metadata fields")); };
        let positional = json!([account, org, principal, credential, actor, "1", null]);
        prop_assert_eq!(serde_json::from_value::<AdmittedCaller>(positional.clone()).is_ok(), valid);
        oracle::check(&serde_json::to_vec(&positional)?);
    }

    /// Malformed expiry spellings cannot bypass canonical positive u64 admission.
    #[test]
    fn serde_expiry_is_canonical_positive_u64(expiry in ".{0,40}") {
        let expected = expiry.len() <= 20 && expiry.parse::<u64>().is_ok_and(|n| n > 0 && n.to_string() == expiry);
        let raw = json!({"accountId":"a","orgId":"o","principal":"p","credentialId":"c","actorId":"i","expiresAtMs":expiry});
        prop_assert_eq!(serde_json::from_value::<AdmittedCaller>(raw.clone()).is_ok(), expected);
        oracle::check(&serde_json::to_vec(&raw)?);
    }
}

#[test]
fn positional_metadata_uses_the_same_admission() {
    for bytes in [
        br#"[1,"cap"]"#.as_slice(),
        br#"["a","o","p","c","i","1",[1,"cap"]]"#.as_slice(),
        br#"["a","o","p","c","i","1",null]"#.as_slice(),
    ] {
        oracle::check(bytes);
        let is_handle = serde_json::from_slice::<CallerCapability>(bytes).is_ok();
        let is_caller = serde_json::from_slice::<AdmittedCaller>(bytes).is_ok();
        assert!(is_handle || is_caller);
    }
    for bytes in [
        br#"[0,"cap"]"#.as_slice(),
        br#"["a","o","p","c","i","01",null]"#.as_slice(),
        br#"["a","o","p","c","i","1",[0,"cap"]]"#.as_slice(),
    ] {
        oracle::check(bytes);
        assert!(serde_json::from_slice::<CallerCapability>(bytes).is_err());
        assert!(serde_json::from_slice::<AdmittedCaller>(bytes).is_err());
    }
}

#[test]
fn rejected_metadata_never_enters_context() -> Result<(), Box<dyn std::error::Error>> {
    for invalid in ["", " ", " leading", "trailing ", "line\nfeed"] {
        let raw = json!({"version":1,"handle":invalid});
        assert!(serde_json::from_value::<CallerCapability>(raw.clone()).is_err());
        assert!(serde_json::from_value::<RuntimeHandle>(raw).is_err());
    }
    for invalid in ["0", "01", "+1", "-1", "1.0", " 1", "18446744073709551616"] {
        let raw = json!({"accountId":"a","orgId":"o","principal":"p","credentialId":"c","actorId":"i","expiresAtMs":invalid});
        assert!(serde_json::from_value::<AdmittedCaller>(raw).is_err());
    }
    for text in ["a".repeat(1024), "λ".repeat(512)] {
        let handle = CallerCapability::new(1, text.clone())?;
        assert_eq!(handle.handle(), text);
        assert!(CallerCapability::new(1, format!("{text}a")).is_err());
    }
    let huge = "a".repeat(1024 * 1024);
    assert!(CallerCapability::new(1, huge.clone()).is_err());
    assert!(RuntimeHandle::new(1, huge.clone()).is_err());
    assert!(caller(&huge, 0, 1).is_err());
    let caller = AdmittedCaller::new(
        "a".into(),
        "o".into(),
        "p".into(),
        "c".into(),
        "i".into(),
        1,
    )?
    .with_capability(CallerCapability::new(1, "cap".into())?)?;
    assert_eq!(
        (
            caller.account_id(),
            caller.org_id(),
            caller.principal(),
            caller.credential_id(),
            caller.actor_id(),
            caller.expires_at_ms()
        ),
        ("a", "o", "p", "c", "i", "1")
    );
    assert_eq!(
        caller.capability().map(CallerCapability::handle),
        Some("cap")
    );
    let wire = serde_json::to_string(&caller)?;
    let context = GuestContext::builder((), Box::new(|| 1))
        .admitted_identity(serde_json::from_str(&wire)?)
        .build();
    assert_eq!(context.admitted_identity.as_ref(), Some(&caller));
    let mut raw = serde_json::to_value(&caller)?;
    *raw.pointer_mut("/capability/version")
        .ok_or("capability version")? = 0.into();
    assert!(serde_json::from_value::<AdmittedCaller>(raw).is_err());
    Ok(())
}
