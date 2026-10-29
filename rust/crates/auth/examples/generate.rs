//! Generate the shared auth protobuf and semantic TypeScript projections.
use acyclic_auth::contract::*;
use std::{fs, path::PathBuf};
use ts_rs::TS;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = PathBuf::from(std::env::args_os().nth(1).ok_or("expected output root")?);
    render(&root.join("rust/crates/auth/proto"))?;
    let mut typescript =
        String::from("// Generated from acyclic-auth contract-derive declarations.\n");
    for declaration in [
        acyclic_auth::guest::GuestAdmissionKind::decl(&ts_rs::Config::default()),
        PrincipalKind::decl(&ts_rs::Config::default()),
        ResourcePattern::decl(&ts_rs::Config::default()),
        ResourceGrant::decl(&ts_rs::Config::default()),
        ExecutionGrant::decl(&ts_rs::Config::default()),
        BillingGrant::decl(&ts_rs::Config::default()),
        SocketPortRange::decl(&ts_rs::Config::default()),
        SocketHostRule::decl(&ts_rs::Config::default()),
        WebsocketConnectGrant::decl(&ts_rs::Config::default()),
        Grant::decl(&ts_rs::Config::default()),
        GrantSet::decl(&ts_rs::Config::default()),
        Attribute::decl(&ts_rs::Config::default()),
        Principal::decl(&ts_rs::Config::default()),
        KeyRange::decl(&ts_rs::Config::default()),
        AttributeMatch::decl(&ts_rs::Config::default()),
        Filter::decl(&ts_rs::Config::default()),
        Pin::decl(&ts_rs::Config::default()),
        Limits::decl(&ts_rs::Config::default()),
        Holder::decl(&ts_rs::Config::default()),
        Caveat::decl(&ts_rs::Config::default()),
        Block::decl(&ts_rs::Config::default()),
        SignedBlock::decl(&ts_rs::Config::default()),
        Capability::decl(&ts_rs::Config::default()),
        AdmittedCapability::decl(&ts_rs::Config::default()),
        CallerIdentity::decl(&ts_rs::Config::default()),
        CallerKeyRevocation::decl(&ts_rs::Config::default()),
        CallerRevocation::decl(&ts_rs::Config::default()),
        CallerAuthority::decl(&ts_rs::Config::default()),
        CallerContext::decl(&ts_rs::Config::default()),
        GrantKind::decl(&ts_rs::Config::default()),
        PinValue::decl(&ts_rs::Config::default()),
        CaveatKind::decl(&ts_rs::Config::default()),
        TipProof::decl(&ts_rs::Config::default()),
        SubjectId::decl(&ts_rs::Config::default()),
        SubjectLineage::decl(&ts_rs::Config::default()),
        SubjectReference::decl(&ts_rs::Config::default()),
        SubjectReferenceKind::decl(&ts_rs::Config::default()),
        LineageAppend::decl(&ts_rs::Config::default()),
        Validity::decl(&ts_rs::Config::default()),
        Birth::decl(&ts_rs::Config::default()),
        BirthCertificate::decl(&ts_rs::Config::default()),
        CertifiedKey::decl(&ts_rs::Config::default()),
        KeyCertificate::decl(&ts_rs::Config::default()),
        CertificateChain::decl(&ts_rs::Config::default()),
        Revocation::decl(&ts_rs::Config::default()),
        KeyringSnapshot::decl(&ts_rs::Config::default()),
        SignedKeyringSnapshot::decl(&ts_rs::Config::default()),
        KeyRevocation::decl(&ts_rs::Config::default()),
        RevocationDelta::decl(&ts_rs::Config::default()),
        RevocationCheckpoint::decl(&ts_rs::Config::default()),
        RevocationPublication::decl(&ts_rs::Config::default()),
        SignedRevocation::decl(&ts_rs::Config::default()),
        AuditRecord::decl(&ts_rs::Config::default()),
        RecordAttributes::decl(&ts_rs::Config::default()),
        AuditDecision::decl(&ts_rs::Config::default()),
    ] {
        typescript.push_str("export ");
        typescript.push_str(&declaration);
        typescript.push('\n');
    }
    let path = root.join("typescript/packages/auth/src/contract.ts");
    fs::create_dir_all(path.parent().ok_or("missing output parent")?)?;
    let typescript = typescript
        .lines()
        .map(str::trim_end)
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    fs::write(path, typescript)?;
    if std::env::args_os().nth(2).as_deref() == Some(std::ffi::OsStr::new("--wire-baseline")) {
        fs::write(
            root.join("compatibility/descriptors/auth-v1.bin"),
            acyclic_auth::FILE_DESCRIPTOR_SET,
        )?;
    }
    Ok(())
}
