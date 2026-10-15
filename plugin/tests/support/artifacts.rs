use super::{ACYCLIC, command};
use sha2::{Digest as _, Sha256};
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

pub fn tree_snapshot(root: &Path) -> BTreeMap<PathBuf, (u64, String)> {
    fn visit(root: &Path, current: &Path, snapshot: &mut BTreeMap<PathBuf, (u64, String)>) {
        let mut entries = fs::read_dir(current)
            .expect("read snapshot directory")
            .collect::<Result<Vec<_>, _>>()
            .expect("read snapshot entries");
        entries.sort_by_key(fs::DirEntry::file_name);
        for entry in entries {
            let path = entry.path();
            let metadata = fs::symlink_metadata(&path).expect("snapshot metadata");
            if metadata.is_dir() {
                visit(root, &path, snapshot);
            } else if metadata.is_file() {
                let bytes = fs::read(&path).expect("snapshot file");
                snapshot.insert(
                    path.strip_prefix(root)
                        .expect("snapshot prefix")
                        .to_path_buf(),
                    (metadata.len(), hex::encode(Sha256::digest(bytes))),
                );
            }
        }
    }
    let mut snapshot = BTreeMap::new();
    visit(root, root, &mut snapshot);
    snapshot
}

pub fn write_qualification_receipt(host: &str, host_binary: &Path, invariants: &[&str]) {
    let Some(directory) = std::env::var_os("ACYCLIC_E2E_RECEIPT_DIR") else {
        return;
    };
    let version = acyclic_native_runtime::process_output(
        command(host_binary).arg("--version"),
        std::time::Duration::from_secs(120),
        8 * 1024 * 1024,
    )
    .expect("read host version");
    assert!(version.status.success(), "host --version failed");
    let package = qualification_environment("ACYCLIC_E2E_HOST_PACKAGE");
    let package_version = qualification_environment("ACYCLIC_E2E_HOST_PACKAGE_VERSION");
    let package_integrity = qualification_environment("ACYCLIC_E2E_HOST_PACKAGE_INTEGRITY");
    let platform_package = qualification_environment("ACYCLIC_E2E_HOST_PLATFORM_PACKAGE");
    let platform_version = qualification_environment("ACYCLIC_E2E_HOST_PLATFORM_VERSION");
    let platform_integrity = qualification_environment("ACYCLIC_E2E_HOST_PLATFORM_INTEGRITY");
    let lock_sha256 = qualification_environment("ACYCLIC_E2E_HOST_LOCK_SHA256");
    assert!(
        String::from_utf8_lossy(&version.stdout).contains(&package_version),
        "host version does not match the qualification lock"
    );
    let receipt = serde_json::json!({
        "schema": "acyclic-agent-qualification-v1",
        "acyclic": {
            "package": env!("CARGO_PKG_NAME"),
            "version": env!("CARGO_PKG_VERSION"),
            "executable_sha256": file_sha256(Path::new(ACYCLIC)),
        },
        "host": {
            "name": host,
            "version": String::from_utf8_lossy(&version.stdout).trim(),
            "executable_sha256": file_sha256(host_binary),
            "lock_sha256": lock_sha256,
            "package": {
                "name": package,
                "version": package_version,
                "integrity": package_integrity,
            },
            "platform_package": {
                "name": platform_package,
                "version": platform_version,
                "integrity": platform_integrity,
            },
        },
        "platform": {
            "os": std::env::consts::OS,
            "arch": std::env::consts::ARCH,
            "backend": match std::env::consts::OS {
                "linux" => "linux-fuse",
                "macos" => "macos-nfs",
                "windows" => "windows-projfs",
                other => other,
            },
        },
        "scenario_corpus_sha256": tree_digest(&Path::new(env!("CARGO_MANIFEST_DIR")).join("tests")),
        "satisfied_invariants": invariants,
        "passed": true,
    });
    let directory = PathBuf::from(directory);
    fs::create_dir_all(&directory).expect("receipt directory");
    fs::write(
        directory.join(format!(
            "{host}-{}-{}.json",
            std::env::consts::OS,
            std::env::consts::ARCH
        )),
        format!(
            "{}\n",
            serde_json::to_string_pretty(&receipt).expect("serialize qualification receipt")
        ),
    )
    .expect("write qualification receipt");
}

fn qualification_environment(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} is required for a qualification receipt"))
}

fn file_sha256(path: &Path) -> String {
    hex::encode(Sha256::digest(
        fs::read(path).expect("hash qualification file"),
    ))
}

fn tree_digest(root: &Path) -> String {
    let snapshot = tree_snapshot(root);
    let mut digest = Sha256::new();
    for (path, (length, hash)) in snapshot {
        digest.update(path.to_string_lossy().as_bytes());
        digest.update([0]);
        digest.update(length.to_le_bytes());
        digest.update(hash.as_bytes());
        digest.update([0]);
    }
    hex::encode(digest.finalize())
}
