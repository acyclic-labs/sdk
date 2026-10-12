use super::*;

#[tokio::test]
async fn operation_control_is_correlated_and_deterministic() {
    let provider = Arc::new(SimulatedMachines::default());
    let key = IdempotencyKey::parse("00000000-0000-0000-0000-00000000000c")
        .unwrap_or_else(|_| unreachable!());
    provider
        .create(request(key))
        .await
        .unwrap_or_else(|_| unreachable!());
    let operation = provider
        .recover_operation(key)
        .await
        .unwrap_or_else(|_| unreachable!());
    let expected = OperationObservation {
        id: operation,
        phase: OperationPhase::Succeeded,
    };

    assert_eq!(provider.inspect_operation(operation).await, Ok(expected));
    assert_eq!(provider.cancel(operation).await, Ok(expected));
    let mut observations = provider
        .watch_operation(operation)
        .await
        .unwrap_or_else(|_| unreachable!());
    assert_eq!(observations.next().await, Some(Ok(expected)));
    assert_eq!(observations.next().await, None);

    let unknown = OperationId::parse("00000000-0000-0000-0000-00000000000d")
        .unwrap_or_else(|_| unreachable!());
    assert!(matches!(
        provider.inspect_operation(unknown).await,
        Err(ProviderError::NotFound(_))
    ));
    assert!(matches!(
        provider.cancel(unknown).await,
        Err(ProviderError::NotFound(_))
    ));
    assert!(matches!(
        provider.watch_operation(unknown).await,
        Err(ProviderError::NotFound(_))
    ));
    let unknown_key = IdempotencyKey::parse("00000000-0000-0000-0000-00000000000e")
        .unwrap_or_else(|_| unreachable!());
    assert!(matches!(
        provider.recover_operation(unknown_key).await,
        Err(ProviderError::NotFound(_))
    ));
}

#[test]
fn derived_identities_are_version_five_and_never_nil() {
    assert_eq!(
        OperationId::from_digest([0; 32]).to_string(),
        "00000000-0000-5000-8000-000000000000"
    );
    assert_eq!(
        MachineId::from_digest([0xff; 32]).to_string(),
        "ffffffff-ffff-5fff-bfff-ffffffffffff"
    );
}

#[test]
fn managed_oci_requires_an_immutable_digest() {
    assert!(Image::oci("ghcr.io/acme/agent:latest").is_err());
    assert!(Image::oci(format!("ghcr.io/acme/agent@sha256:{}", "0".repeat(64))).is_err());
    assert!(Image::oci(format!("ghcr.io/acme/agent@sha256:{}", "a".repeat(64))).is_ok());
}

#[test]
#[allow(
    clippy::indexing_slicing,
    reason = "indexes a repo-local `compatibility/manifest.json` fixture bundled via include_str!; a missing key means the fixture itself is broken and the test should panic loudly"
)]
fn public_descriptor_is_pinned() {
    let digest: [u8; 32] = Sha256::digest(FILE_DESCRIPTOR_SET).into();
    let manifest: serde_json::Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../../compatibility/manifest.json"
    )))
    .unwrap_or_else(|_| unreachable!());
    let expected = manifest["families"]["machines"]["descriptorDigest"]
        .as_str()
        .unwrap_or_else(|| unreachable!());
    assert_eq!(expected, format!("sha256:{}", hex::encode(digest)));
}
