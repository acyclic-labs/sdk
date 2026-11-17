use std::{env, fs, path::PathBuf, process::Command};

fn main() {
    let mut args = env::args_os().skip(1);
    let output = PathBuf::from(args.next().unwrap());
    let artifacts = PathBuf::from(args.next().unwrap());
    let rustup = fs::canonicalize(args.next().unwrap()).unwrap();
    let toolchain = args.next().unwrap().into_string().unwrap();
    let package = args.next().unwrap().into_string().unwrap();
    let manifest_path = PathBuf::from(args.next().unwrap());
    let selected_lock = PathBuf::from(args.next().unwrap());
    let library_name = args.next().unwrap().into_string().unwrap();
    let proc_macro = args.next().unwrap() == "proc-macro";
    let linker = args.next().unwrap();
    let root = output.with_extension("work");
    let source = root.join("source");
    let target = root.join("target");
    fs::create_dir_all(&source).unwrap();
    fs::create_dir_all(&target).unwrap();
    let target = fs::canonicalize(target).unwrap();
    while let Some(argument) = args.next() {
        if argument == "--" {
            break;
        }
        let input = PathBuf::from(argument);
        let relative = args.next().unwrap();
        let destination = source.join(relative);
        fs::create_dir_all(destination.parent().unwrap()).unwrap();
        fs::copy(input, destination).unwrap();
    }
    let home = rustup
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("rustup");
    let version = Command::new(&rustup)
        .args(["run", &toolchain, "rustc", "-vV"])
        .env("RUSTUP_HOME", &home)
        .env("CARGO_HOME", root.join("cargo-home"))
        .output()
        .unwrap();
    assert!(version.status.success(), "cannot identify fixture compiler");
    let version = String::from_utf8(version.stdout).unwrap();
    let host = version
        .lines()
        .find_map(|line| line.strip_prefix("host: "))
        .unwrap();
    let bin = home
        .join("toolchains")
        .join(format!("{toolchain}-{host}"))
        .join("bin");
    let mut command = Command::new(rustup);
    if !linker.is_empty() {
        command.env(
            format!(
                "CARGO_TARGET_{}_LINKER",
                host.replace('-', "_").to_ascii_uppercase()
            ),
            fs::canonicalize(linker).unwrap(),
        );
    }
    command
        .args([
            "run",
            &toolchain,
            "cargo",
            "rustdoc",
            "--locked",
            "--offline",
        ])
        .arg("--manifest-path")
        .arg(source.join(manifest_path))
        .args(["--package", &package, "--lib"])
        .args(["--target", host])
        .args(args);
    if !selected_lock.as_os_str().is_empty() {
        let lock = fs::canonicalize(source.join(selected_lock)).unwrap();
        command.args([
            "--config",
            &format!("resolver.lockfile-path={:?}", lock.to_str().unwrap()),
        ]);
    }
    command
        .args(["--", "-Z", "unstable-options", "--output-format", "json"])
        .env("RUSTUP_HOME", home)
        .env(
            "RUSTC",
            bin.join(format!("rustc{}", env::consts::EXE_SUFFIX)),
        )
        .env(
            "RUSTDOC",
            bin.join(format!("rustdoc{}", env::consts::EXE_SUFFIX)),
        )
        .env("CARGO_HOME", root.join("cargo-home"))
        .env("CARGO_PROFILE_DEV_DEBUG", "0")
        .env(
            "CARGO_ENCODED_RUSTFLAGS",
            if cfg!(windows) {
                "-C\u{1f}prefer-dynamic"
            } else {
                ""
            },
        )
        .env("CARGO_TARGET_DIR", &target);
    assert!(
        command.status().unwrap().success(),
        "fixture Rustdoc failed"
    );
    let rustdoc = (if proc_macro {
        target.join("doc")
    } else {
        target.join(host).join("doc")
    })
    .join(format!("{library_name}.json"));
    if artifacts.as_os_str().is_empty() {
        fs::copy(rustdoc, output).unwrap();
    } else {
        fn relocate(value: &mut serde_json::Value, prefix: &str) {
            match value {
                serde_json::Value::String(text) => {
                    if let Some(suffix) = text.strip_prefix(prefix) {
                        *text = format!("@fixture-target@{suffix}");
                    }
                }
                serde_json::Value::Array(values) => {
                    values.iter_mut().for_each(|value| relocate(value, prefix));
                }
                serde_json::Value::Object(values) => {
                    values
                        .values_mut()
                        .for_each(|value| relocate(value, prefix));
                }
                _ => {}
            }
        }
        let mut json: serde_json::Value =
            serde_json::from_slice(&fs::read(&rustdoc).unwrap()).unwrap();
        fs::remove_file(rustdoc).unwrap();
        relocate(&mut json, target.to_str().unwrap());
        fs::rename(&target, &artifacts).unwrap();
        fn check_artifacts(value: &serde_json::Value, artifacts: &std::path::Path) {
            match value {
                serde_json::Value::String(text) => {
                    if let Some(suffix) = text.strip_prefix("@fixture-target@") {
                        let path = artifacts.join(suffix.trim_start_matches(['/', '\\']));
                        assert!(
                            path.exists(),
                            "missing Rustdoc artifact: {}",
                            path.display()
                        );
                    }
                }
                serde_json::Value::Array(values) => {
                    values
                        .iter()
                        .for_each(|value| check_artifacts(value, artifacts));
                }
                serde_json::Value::Object(values) => {
                    values
                        .values()
                        .for_each(|value| check_artifacts(value, artifacts));
                }
                _ => {}
            }
        }
        check_artifacts(&json, &artifacts);
        fs::write(output, serde_json::to_vec(&json).unwrap()).unwrap();
    }
    fs::remove_dir_all(root).unwrap();
}
