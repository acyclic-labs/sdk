//! Generate the shared auth protobuf and semantic TypeScript projections.
use acyclic_auth::contract::*;
use std::{fs, path::PathBuf};
use ts_rs::TS;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = PathBuf::from(std::env::args_os().nth(1).ok_or("expected output root")?);
    let mut typescript =
        String::from("// Generated from acyclic-auth contract-derive declarations.\n");
    let config = ts_rs::Config::default();
    // Declaration order is the generated file's order.
    let declarations: [fn(&ts_rs::Config) -> String; 43] = [
        PrincipalKind::decl,
        ResourcePattern::decl,
        ResourceGrant::decl,
        ExecutionGrant::decl,
        BillingGrant::decl,
        SocketPortRange::decl,
        SocketHostRule::decl,
        WebsocketConnectGrant::decl,
        Grant::decl,
        GrantSet::decl,
        Attribute::decl,
        Principal::decl,
        KeyRange::decl,
        AttributeMatch::decl,
        Filter::decl,
        Pin::decl,
        Limits::decl,
        Holder::decl,
        Caveat::decl,
        Block::decl,
        SignedBlock::decl,
        Capability::decl,
        GrantKind::decl,
        PinValue::decl,
        CaveatKind::decl,
        TipProof::decl,
        SubjectId::decl,
        SubjectLineage::decl,
        SubjectReference::decl,
        SubjectReferenceKind::decl,
        LineageAppend::decl,
        Validity::decl,
        Birth::decl,
        BirthCertificate::decl,
        CertifiedKey::decl,
        KeyCertificate::decl,
        CertificateChain::decl,
        Revocation::decl,
        KeyringSnapshot::decl,
        SignedKeyringSnapshot::decl,
        AuditRecord::decl,
        RecordAttributes::decl,
        AuditDecision::decl,
    ];
    for declaration in declarations {
        typescript.push_str("export ");
        typescript.push_str(&declaration(&config));
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
    Ok(())
}
