use super::render_proto_files;
use std::{
    fs, io,
    path::{Path, PathBuf},
};

#[test]
fn manifest_keeps_authored_inbox_and_refuses_host_invocation_id() {
    use super::{ContractError, validate_subscription_manifest, wire};

    let mut binding = wire::SubscriptionSpec {
        subscription_id: "inbox".into(),
        stream_path: "harness/inbox".into(),
        start: Some(wire::SubscriptionStart {
            start: Some(wire::subscription_start::Start::Cursor(0)),
        }),
        placement_anchor: true,
    };
    assert_eq!(
        validate_subscription_manifest(std::slice::from_ref(&binding)),
        Ok(())
    );
    binding.subscription_id = crate::invocation::HOST_INBOX.into();
    assert_eq!(
        validate_subscription_manifest(&[binding]),
        Err(ContractError::InvalidArgument)
    );
}

fn snapshot(root: &Path, directory: &Path) -> io::Result<Vec<(PathBuf, Vec<u8>)>> {
    let mut entries = Vec::new();
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            entries.extend(snapshot(root, &path)?);
        } else {
            entries.push((
                path.strip_prefix(root)
                    .map_err(io::Error::other)?
                    .to_owned(),
                fs::read(path)?,
            ));
        }
    }
    entries.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(entries)
}

#[test]
fn render_proto_files_is_byte_deterministic() {
    let root = std::env::temp_dir().join(format!(
        "acyclic-actors-contract-determinism-{}",
        std::process::id()
    ));
    let first = root.join("first");
    let second = root.join("second");
    let _ = fs::remove_dir_all(&root);

    render_proto_files(&first).expect("first contract render");
    render_proto_files(&second).expect("second contract render");

    assert_eq!(
        snapshot(&first, &first).expect("first generated snapshot"),
        snapshot(&second, &second).expect("second generated snapshot")
    );
    fs::remove_dir_all(root).expect("remove deterministic-render fixture");
}
