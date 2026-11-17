use std::{env, fs, path::PathBuf, process::Command};

fn main() {
    let mut args = env::args_os().skip(1);
    let output = PathBuf::from(args.next().unwrap());
    let rustup = fs::canonicalize(args.next().unwrap()).unwrap();
    let root = output.with_extension("work");
    let source = root.join("source");
    let target = root.join("target");
    fs::create_dir_all(&source).unwrap();
    for argument in args.by_ref() {
        if argument == "--" {
            break;
        }
        let input = PathBuf::from(argument);
        let text = input.to_string_lossy().replace('\\', "/");
        let relative = text
            .split_once("tests/fixtures/feature-profile/")
            .unwrap()
            .1;
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
    let mut command = Command::new(rustup);
    command
        .args([
            "run",
            "nightly-2026-07-01",
            "cargo",
            "rustdoc",
            "--locked",
            "--offline",
        ])
        .arg("--manifest-path")
        .arg(source.join("Cargo.toml"))
        .args(["--package", "demo", "--lib"])
        .args(args)
        .args(["--", "-Z", "unstable-options", "--output-format", "json"])
        .env("RUSTUP_HOME", home)
        .env("CARGO_HOME", root.join("cargo-home"))
        .env("CARGO_TARGET_DIR", &target);
    assert!(
        command.status().unwrap().success(),
        "fixture Rustdoc failed"
    );
    fs::copy(target.join("doc/demo.json"), output).unwrap();
    fs::remove_dir_all(root).unwrap();
}
