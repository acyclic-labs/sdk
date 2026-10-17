//! The child process inspects live allocator storage before glibc releases it.
#![cfg(all(target_os = "linux", target_env = "gnu"))]
#[path = "common/signed.rs"]
mod signed;
use acyclic_auth::{contract::*, verify::verify};
use prost::Message;
use signed::*;
use std::process::Command;
use zeroize::Zeroizing;

#[test]
fn proof_buffers_are_scrubbed_before_deallocation() {
    let directory = tempfile::tempdir().unwrap();
    let library = directory.path().join("scrub.so");
    let compiler = Command::new("cc")
        .args(["-shared", "-fPIC", "-O2", "-Wall", "-Wextra", "-Werror"])
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/common/scrub_allocator.c"
        ))
        .arg("-o")
        .arg(&library)
        .output()
        .unwrap();
    assert!(
        compiler.status.success(),
        "{}",
        String::from_utf8_lossy(&compiler.stderr)
    );
    for case in [
        "valid",
        "duplicate-secret",
        "duplicate-sealed",
        "truncated",
        "bad-key",
        "encode",
        "broken",
    ] {
        let active = directory.path().join(case);
        let output = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "scrub_child", "--nocapture"])
            .env("LD_PRELOAD", &library)
            .env("AUTH_SCRUB_ACTIVE", &active)
            .env("AUTH_SCRUB_CASE", case)
            .output()
            .unwrap();
        let expected = if case == "broken" { 86 } else { 0 };
        assert_eq!(
            output.status.code(),
            Some(expected),
            "{case}: {} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[test]
fn scrub_child() {
    let Ok(case) = std::env::var("AUTH_SCRUB_CASE") else {
        return;
    };
    let mut bytes = Zeroizing::new(root(|_| {}));
    let trust = Snapshot::default();
    let cap = verify(&bytes, &trust, NOW).unwrap();
    match case.as_str() {
        "duplicate-secret" => {
            let end = bytes.len() - 2;
            bytes.splice(end..end, [vec![18, 32], vec![4; 32]].concat());
        }
        "duplicate-sealed" | "broken" => {
            let end = bytes.len() - 2;
            bytes.splice(end..end, [vec![26, 64], vec![0; 64]].concat());
        }
        "truncated" => {
            bytes.pop();
        }
        "bad-key" => {
            let end = bytes.len() - 3;
            *bytes.get_mut(end).unwrap() = 5;
        }
        _ => {}
    }
    std::fs::write(std::env::var_os("AUTH_SCRUB_ACTIVE").unwrap(), b"active").unwrap();
    match case.as_str() {
        "broken" => {
            drop(CapabilityProto::decode(bytes.as_slice()).unwrap());
        }
        "encode" => {
            drop(cap.encode());
            drop(cap.seal().unwrap().encode());
        }
        "valid" => {
            drop(verify(&bytes, &trust, NOW).unwrap());
        }
        _ => assert!(verify(&bytes, &trust, NOW).is_err()),
    }
}
