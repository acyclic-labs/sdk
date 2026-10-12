use super::*;

#[test]
fn wire_decoders_reject_nil_and_duplicate_capabilities() {
    assert!(decode_machine(Some(&wire::MachineId { value: vec![0; 16] })).is_err());
    let capabilities = BTreeSet::from([Capability::ElasticCpu]);
    assert!(
        decode_compatibility(
            Some(&wire::CompatibilityPolicy {
                mode: wire::CompatibilityMode::Require as i32,
                required: vec![
                    wire::Capability::ElasticCpu as i32,
                    wire::Capability::ElasticCpu as i32,
                ],
            }),
            &capabilities,
        )
        .is_err()
    );
}

#[test]
fn all_capability_intents_round_trip_through_the_wire() {
    let capabilities = [
        Capability::ElasticCpu,
        Capability::ElasticMemory,
        Capability::LiveCheckpoint,
        Capability::LiveFork,
        Capability::SuspendResume,
        Capability::LiveMovement,
        Capability::DiskFork,
    ];
    for capability in capabilities {
        assert_eq!(
            decode_capability(encode_capability(capability)),
            Ok(capability)
        );
    }
}

#[test]
fn managed_usage_rejects_missing_lineage_authority() {
    let machine =
        MachineId::parse("00000000-0000-0000-0000-000000000001").unwrap_or_else(|_| unreachable!());
    let receipt = |lineage_receipt_sha256: Vec<u8>| wire::UsageReceipt {
        machine: Some(encode_machine(machine)),
        start_unix_ms: 1,
        end_unix_ms: 2,
        elastic_cpu_ns: 0,
        dedicated_cpu_ns: 0,
        private_resident_byte_seconds: 0,
        durable_private_bytes: 0,
        lineage_receipt_sha256,
        egress_bytes: 0,
        receipt: vec![1],
    };
    assert!(decode_usage_receipt(receipt(vec![7; 32]), machine, 1, 2).is_ok());
    assert!(decode_usage_receipt(receipt(Vec::new()), machine, 1, 2).is_err());
    assert!(decode_usage_receipt(receipt(vec![0; 32]), machine, 1, 2).is_err());
}

#[test]
fn mutation_errors_preserve_unsupported_capabilities() {
    let key = IdempotencyKey::parse("00000000-0000-0000-0000-000000000001")
        .unwrap_or_else(|_| unreachable!());
    assert_eq!(
        mutation_error(key, &tonic::Status::unimplemented("capability unavailable"),),
        ProviderError::Unsupported("capability unavailable".into())
    );
    assert_eq!(
        watch_error(key, &tonic::Status::unimplemented("watch unavailable")),
        ProviderError::Indeterminate(key)
    );
    assert_eq!(
        watch_error(
            key,
            &tonic::Status::resource_exhausted("watch capacity unavailable"),
        ),
        ProviderError::Indeterminate(key)
    );
}

fn wire_contract(capabilities: &[wire::Capability]) -> wire::MachineContract {
    wire::MachineContract {
        image: Some(wire::Image {
            kind: wire::ImageKind::Custom as i32,
            immutable_reference: Some(wire::image::ImmutableReference::CustomDigest(vec![7; 32])),
        }),
        capabilities: capabilities.iter().map(|value| *value as i32).collect(),
        compatibility: Some(wire::CompatibilityPolicy {
            mode: wire::CompatibilityMode::BestEffort as i32,
            required: Vec::new(),
        }),
        compatibility_revision: vec![1; 32],
        suspension: Some(wire::SuspensionPolicy {
            policy: Some(wire::suspension_policy::Policy::Manual(true)),
        }),
        expiration: Some(wire::ExpirationPolicy {
            kind: wire::ExpirationKind::Never as i32,
            value_ms: 0,
        }),
        network_policy_digest: vec![8; 32],
        budgets: Some(wire::Budgets::default()),
    }
}

#[test]
fn fork_machine_admissions_are_checked_against_the_source_contract()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let source = MachineId::parse("00000000-0000-0000-0000-000000000051")?;
    let child = MachineId::parse("00000000-0000-0000-0000-000000000052")?;
    let other = MachineId::parse("00000000-0000-0000-0000-000000000053")?;
    let operation = OperationId::parse("00000000-0000-0000-0000-000000000054")?;
    let admission = |capabilities: &[wire::Capability],
                     fidelity: wire::ForkFidelity,
                     children: Vec<MachineId>| wire::ForkMachineAdmission {
        source: Some(encode_machine(source)),
        children: children.into_iter().map(encode_machine).collect(),
        operation: Some(wire::OperationId {
            value: operation.as_bytes().to_vec(),
        }),
        contract: Some(wire_contract(capabilities)),
        fidelity: fidelity as i32,
    };
    let live = [wire::Capability::LiveFork, wire::Capability::DiskFork];
    let disk = [wire::Capability::DiskFork];

    let (_, fidelity, children, _) = decode_fork_machine_admission(&admission(
        &live,
        wire::ForkFidelity::MemoryAndDisk,
        vec![child, other],
    ))?;
    assert_eq!(fidelity, ForkFidelity::MemoryAndDisk);
    assert_eq!(children, vec![child, other]);
    assert_eq!(
        decode_fork_machine_admission(&admission(
            &disk,
            wire::ForkFidelity::DiskOnly,
            vec![child]
        ))?
        .1,
        ForkFidelity::DiskOnly
    );
    for rejected in [
        admission(&disk, wire::ForkFidelity::MemoryAndDisk, vec![child]),
        admission(&live, wire::ForkFidelity::DiskOnly, vec![child]),
        admission(&[], wire::ForkFidelity::DiskOnly, vec![child]),
        admission(&live, wire::ForkFidelity::Unspecified, vec![child]),
        admission(&live, wire::ForkFidelity::MemoryAndDisk, Vec::new()),
        admission(&live, wire::ForkFidelity::MemoryAndDisk, vec![child, child]),
        admission(&live, wire::ForkFidelity::MemoryAndDisk, vec![source]),
    ] {
        assert!(decode_fork_machine_admission(&rejected).is_err());
    }
    Ok(())
}
