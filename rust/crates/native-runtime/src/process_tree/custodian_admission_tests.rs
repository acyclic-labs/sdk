//! The admission port is reachable by any local process; only a holder of
//! the tree's secret may name its members.
use super::*;
use std::net::TcpStream;
use std::process::{Command, Stdio};

/// Asks the custodian at `address` to admit `pid` into `tree`.
fn admit(address: &str, pid: u32, tree: u64, secret: &Secret) -> bool {
    let mut stream = TcpStream::connect(address).expect("connect");
    let mut request = Vec::with_capacity(28);
    request.extend_from_slice(&pid.to_le_bytes());
    request.extend_from_slice(&tree.to_le_bytes());
    request.extend_from_slice(secret);
    stream.write_all(&request).expect("request");
    let mut reply = [0];
    stream.read_exact(&mut reply).expect("reply");
    reply == [1]
}

#[test]
fn only_a_holder_of_the_trees_secret_is_admitted() {
    let root = tempfile::tempdir().expect("root");
    let custody =
        super::super::ProcessCustody::acquire(&root.path().join("trees")).expect("custody");
    // Reads its open stdin until the tree is killed.
    let mut command = Command::new(r"C:\Windows\System32\findstr.exe");
    command
        .arg("x")
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let mut tree =
        super::super::ProcessTree::spawn_in_custody(&mut command, &custody).expect("spawn");
    let endpoint = tree
        .custody
        .as_ref()
        .expect("a custodied tree has a record")
        .admission();
    let mut parts = endpoint.split('/');
    let (address, id, secret) = (
        parts.next().expect("address"),
        parts.next().and_then(|id| id.parse::<u64>().ok()).expect("tree"),
        parts.next().and_then(decode).expect("secret"),
    );
    let pid = tree.child.as_ref().expect("child").id();
    // Negative control: the right tree and a member, but a foreign secret.
    let mut foreign = secret;
    foreign[0] ^= 1;
    assert!(!admit(address, pid, id, &foreign), "a foreign request was admitted");
    assert!(!admit(address, pid, id, &[0; 16]), "an empty secret was admitted");
    assert!(admit(address, pid, id, &secret), "the tree's own secret was refused");
    tree.terminate(None).expect("terminate");
}
