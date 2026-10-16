fn validate_handshake(
    response: wire::HandshakeResponse,
) -> Result<wire::Capabilities, HostedFsError> {
    let handshake = response.protocol.ok_or(HostedFsError::InvalidResponse(
        "handshake response is absent",
    ))?;
    let protocol = handshake.protocol.ok_or(HostedFsError::InvalidResponse(
        "handshake protocol is absent",
    ))?;
    if protocol.version != FILESYSTEM_PROTOCOL_VERSION {
        return Err(HostedFsError::InvalidResponse(
            "filesystem protocol version is unsupported",
        ));
    }
    if protocol.descriptor_digest != crate::descriptor_digest() {
        return Err(HostedFsError::InvalidResponse(
            "filesystem descriptor digest does not match",
        ));
    }
    let supported = handshake.supported.ok_or(HostedFsError::InvalidResponse(
        "supported capabilities are absent",
    ))?;
    if !supported.capabilities.iter().any(|capability| {
        capability.name == "filesystem" && capability.version == FILESYSTEM_PROTOCOL_VERSION
    }) {
        return Err(HostedFsError::InvalidResponse(
            "filesystem capability version is unsupported",
        ));
    }
    let capabilities = response.capabilities.ok_or(HostedFsError::InvalidResponse(
        "filesystem capabilities are absent",
    ))?;
    if capabilities.contract_version != FILESYSTEM_PROTOCOL_VERSION {
        return Err(HostedFsError::InvalidResponse(
            "filesystem contract version is unsupported",
        ));
    }
    if capabilities.maximum_request_bytes == 0
        || capabilities.maximum_response_bytes == 0
        || capabilities.maximum_transaction_mutations == 0
        || capabilities.maximum_page_items == 0
    {
        return Err(HostedFsError::InvalidResponse(
            "filesystem capabilities contain an unbounded limit",
        ));
    }
    Ok(capabilities)
}

fn operation(idempotency_key: IdempotencyKey) -> wire::OperationOptions {
    wire::OperationOptions {
        idempotency_key: idempotency_key.into_bytes().to_vec(),
    }
}

fn profile_to_wire(profile: EmbeddedProfile) -> wire::FilesystemProfile {
    match profile {
        EmbeddedProfile::Portable => wire::FilesystemProfile::Portable,
        EmbeddedProfile::Posix => wire::FilesystemProfile::Posix,
        EmbeddedProfile::Windows => wire::FilesystemProfile::Windows,
        EmbeddedProfile::Browser => wire::FilesystemProfile::Browser,
    }
}

fn validate_generation(
    generation: &wire::GenerationRef,
    workspace: &wire::WorkspaceRef,
) -> Result<(), HostedFsError> {
    exact_len(
        &generation.generation_id,
        32,
        "generation identity has the wrong length",
    )?;
    let owner = generation
        .workspace
        .as_ref()
        .ok_or(HostedFsError::InvalidResponse(
            "generation workspace is absent",
        ))?;
    if owner != workspace {
        return Err(HostedFsError::InvalidResponse(
            "generation belongs to another workspace",
        ));
    }
    Ok(())
}

fn exact_len(value: &[u8], expected: usize, message: &'static str) -> Result<(), HostedFsError> {
    if value.len() == expected {
        Ok(())
    } else {
        Err(HostedFsError::InvalidResponse(message))
    }
}
