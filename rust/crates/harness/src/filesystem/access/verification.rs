use super::*;

fn roundtrip<T: PreparationRecord + serde::Serialize + std::fmt::Debug + PartialEq>(
    io: &PreparationIo,
    value_io: &PreparationIo,
    bytes: &[u8],
) -> Result<()> {
    let expected = serde_json::from_slice::<T>(bytes);
    let decoded = record::<T>(io, bytes, crate::wire::ErrorCode::Storage);
    let (expected, decoded) = match (expected, decoded) {
        (Err(_), Err(_)) => return Ok(()),
        (Ok(expected), Ok(decoded)) => (expected, decoded),
        (Ok(_), Err(error)) => return Err(error),
        (Err(error), Ok(_)) => panic!("bounded decoder accepted an invalid record: {error}"),
    };
    assert_eq!(expected, decoded.0);
    let encoded = io.encode(
        &decoded.0,
        acyclic_fs::access::MAXIMUM_POLICY_RECORD_BYTES,
        false,
    )?;
    let hold = io.guard_bytes(&encoded)?;
    let encoded = (encoded, hold);
    let repeated = record::<T>(io, &encoded.0, crate::wire::ErrorCode::Storage)?;
    assert_eq!(decoded.0, repeated.0);
    drop(repeated);
    drop(encoded);
    drop(decoded);
    let value: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|error| Error::Storage(error.to_string()))?;
    let repeated =
        value_io.value_decode_preparation::<T>(&value, crate::wire::ErrorCode::Storage)?;
    assert_eq!(expected, repeated.0);
    Ok(())
}

/// Shared bounded-decoder oracle for cargo-fuzz, seeds and shrinking properties.
pub fn check_access_record(bytes: &[u8]) -> Result<()> {
    if bytes.len() > 4096 {
        return Ok(());
    }
    let cancel = acyclic_fs::CancellationToken::new();
    #[cfg(not(target_arch = "wasm32"))]
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::performance::BOUNDARY_BUDGET,
        &cancel,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    #[cfg(not(target_arch = "wasm32"))]
    let io = PreparationIo::from_native_work(&work)?;
    #[cfg(target_arch = "wasm32")]
    let io = PreparationIo::from_budget(acyclic_fs::performance::BOUNDARY_BUDGET, &cancel)?;
    let standalone = PreparationIo::from_budget(acyclic_fs::performance::BOUNDARY_BUDGET, &cancel)?;
    for byte_io in [&standalone, &io] {
        roundtrip::<Policy>(byte_io, &io, bytes)?;
        roundtrip::<Capability>(byte_io, &io, bytes)?;
        roundtrip::<FileRef>(byte_io, &io, bytes)?;
        roundtrip::<GenerationRef>(byte_io, &io, bytes)?;
        roundtrip::<(FileRef, Option<GenerationRef>)>(byte_io, &io, bytes)?;
        roundtrip::<crate::Capabilities>(byte_io, &io, bytes)?;
    }
    assert_eq!(standalone.owned_bytes()?, 0);
    drop(standalone);
    if let Ok(capabilities) =
        record::<crate::Capabilities>(&io, bytes, crate::wire::ErrorCode::Storage)
    {
        let ceilings = crate::Capabilities::new(
            capabilities
                .0
                .iter()
                .filter(|label| label.starts_with(acyclic_fs::access::SCOPE_PREFIX)),
        );
        let legacy = crate::Capabilities::new(
            capabilities
                .0
                .iter()
                .filter(|label| !label.starts_with(acyclic_fs::access::SCOPE_PREFIX)),
        );
        if ceilings.iter().next().is_some()
            && legacy
                .iter()
                .any(acyclic_fs::access::is_filesystem_capability)
        {
            assert!(!legacy.is_subset_of(&capabilities.0));
            assert!(
                capabilities
                    .0
                    .without(&ceilings)
                    .iter()
                    .all(|label| !acyclic_fs::access::is_filesystem_capability(label))
            );
            assert!(
                capabilities
                    .0
                    .intersect(&legacy)
                    .iter()
                    .all(|label| !acyclic_fs::access::is_filesystem_capability(label))
            );
        }
    }
    assert_eq!(io.owned_bytes()?, 0);
    drop(io);
    #[cfg(not(target_arch = "wasm32"))]
    assert_eq!(
        work.live_allocation_bytes(),
        acyclic_fs::NativeMaterializationWork::accounting_state_bytes()
    );
    Ok(())
}

#[cfg(test)]
mod facts {
    use super::*;
    use proptest::prelude::*;
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]
        #[test]
        fn unknown_facts_are_rejected_regardless_of_value(value in "[a-z]{1,16}", selector in 0u8..6) {
            let io = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new()).unwrap();
            let mut policy: Policy = serde_json::from_slice(INITIAL_POLICY).unwrap();
            prop_assert!(require_authenticated_facts(&policy, &io).is_ok());
            let rule = &mut policy.layers[0][0];
            rule.effect = acyclic_fs::access::Effect::Deny;
            match selector {
                0 => rule.principal.group = Some(value),
                1 => { rule.principal.attributes.insert(value.clone(), value); },
                2 => rule.conditions.role = Some(value),
                3 => rule.conditions.tag = Some(value),
                4 => rule.conditions.relation = Some(acyclic_fs::access::Relation::SelfTask),
                _ => rule.resource.lineage_root = Some(value),
            }
            let error = require_authenticated_facts(&policy, &io).unwrap_err();
            prop_assert!(matches!(error, Error::FilesystemAccessConfiguration(_)));
            prop_assert_eq!(error.code(), crate::wire::ErrorCode::Unsupported);
            prop_assert!(!error.is_native_failure());
        }
    }
}
