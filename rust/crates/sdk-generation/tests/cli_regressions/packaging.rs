use super::*;
#[path = "../../../sdk-docs/src/sync.rs"]
mod cache;

static PACKAGE: cache::Mutex<Option<cache::Arc<(Fixture, BTreeMap<String, Vec<u8>>)>>> =
    cache::Mutex::new(None);

fn shared_package() -> cache::Arc<(Fixture, BTreeMap<String, Vec<u8>>)> {
    let cargo = runtime::cargo();
    cache::get_or_try_init(&PACKAGE, || {
        let (fixture, support) = historical_source_fixture(None);
        let source = fixture.root.join("rust/crates/demo");
        include_sources(&source);
        fs::write(
            fixture.root.join("release/cargo-crates.json"),
            b"[\"demo\"]",
        )
        .unwrap();
        init_git(&fixture.root, "original registry owner authority");
        let result = Command::new(&*cargo)
            .env("CARGO_HOME", fixture_cargo_home(&fixture))
            .current_dir(&fixture.root)
            .args([
                "package",
                "--offline",
                "--allow-dirty",
                "--no-verify",
                "--manifest-path",
            ])
            .arg(source.join("Cargo.toml"))
            .arg("--target-dir")
            .arg(support.join("package-target"))
            .output()
            .unwrap();
        assert!(result.status.success(), "{}", output_message(&result));
        let members = sdk_generation::historical::decode_archive_members(
            &fs::read(support.join("package-target/package/demo-1.0.0.crate")).unwrap(),
            "demo",
            "1.0.0",
        )
        .unwrap();
        fs::copy(
            runtime::binary("registry-transport-fixture"),
            support.join(if cfg!(windows) { "curl.exe" } else { "curl" }),
        )
        .unwrap();
        Ok::<_, std::convert::Infallible>((fixture, members))
    })
    .unwrap()
}

pub(super) fn include_sources(package: &Path) {
    let path = package.join("Cargo.toml");
    let source = fs::read_to_string(&path).unwrap();
    if !source.contains("include = [") {
        fs::write(path, source.replace("[package]\n", "[package]\ninclude = [\"Cargo.toml\", \"README.md\", \"src/**\", \"examples/**\", \"descriptor.bin\", \"input.json\", \"schema.proto\"]\n")).unwrap();
    }
}

pub(super) fn source_fixture() -> (Fixture, PathBuf) {
    let template = shared_package();
    let id = NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed);
    let root = template.0.root.with_extension(format!("case-{id}"));
    let output = template.0.output.with_extension(format!("case-{id}"));
    super::replay::copy_tree(&template.0.root, &root);
    let support = output.join("test-support");
    fs::create_dir_all(support.join("registry")).unwrap();
    (
        Fixture {
            root,
            rustdoc: output.join(".rustdoc"),
            output,
        },
        support,
    )
}

pub(super) fn materialize(package: &Path, support: &Path, package_name: &str) -> PathBuf {
    let template = shared_package();
    let source = support.join(format!("cargo-packaged-source/{package_name}-1.0.0"));
    for (name, bytes) in &template.1 {
        let file = source.join(name);
        fs::create_dir_all(file.parent().unwrap()).unwrap();
        fs::write(file, bytes).unwrap();
    }
    // Every case keeps its actual source, original manifest, build scripts and
    // examples. Only Cargo's immutable normalization scaffold is shared.
    super::replay::copy_tree(package, &source);
    let original = fs::read(package.join("Cargo.toml")).unwrap();
    let normalized =
        super::package_manifest::normalize(&template.1["Cargo.toml"], &original).unwrap();
    fs::write(source.join("Cargo.toml.orig"), original).unwrap();
    fs::write(source.join("Cargo.toml"), normalized).unwrap();
    let lock = fs::read_to_string(source.join("Cargo.lock")).unwrap();
    fs::write(
        source.join("Cargo.lock"),
        lock.replace("name = \"demo\"", &format!("name = {package_name:?}")),
    )
    .unwrap();
    source
}
pub(super) fn transport() -> PathBuf {
    shared_package()
        .0
        .output
        .join("test-support")
        .join(if cfg!(windows) { "curl.exe" } else { "curl" })
}
