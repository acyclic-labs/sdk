use super::*;

type Package = (Fixture, BTreeMap<String, Vec<u8>>);
static PACKAGE: cache::Mutex<Option<cache::Arc<Package>>> = cache::Mutex::new(None);

fn shared_package() -> cache::Arc<Package> {
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
        let mut members = sdk_generation::historical::decode_archive_members(
            &fs::read(support.join("package-target/package/demo-1.0.0.crate")).unwrap(),
            "demo",
            "1.0.0",
        )
        .unwrap();
        members.remove(".cargo_vcs_info.json");
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
    copy_source_fixture(&shared_package().0)
}

pub(super) fn copy_source_fixture(template: &Fixture) -> (Fixture, PathBuf) {
    let fixture = copy_fixture(template);
    let support = fixture.output.join("test-support");
    fs::create_dir_all(support.join("registry")).unwrap();
    (fixture, support)
}

pub(super) fn materialize(package: &Path, support: &Path, package_name: &str) -> PathBuf {
    let template = shared_package();
    let source = support.join(format!("cargo-packaged-source/{package_name}-1.0.0"));
    super::replay::copy_tree(package, &source);
    for (name, bytes) in template
        .1
        .iter()
        .filter(|(name, _)| !package.join(name).try_exists().unwrap())
    {
        let file = source.join(name);
        fs::create_dir_all(file.parent().unwrap()).unwrap();
        fs::write(file, bytes).unwrap();
    }
    let original = fs::read(package.join("Cargo.toml")).unwrap();
    let normalized =
        super::package_manifest::normalize(&template.1["Cargo.toml"], &original).unwrap();
    fs::write(source.join("Cargo.toml.orig"), original).unwrap();
    fs::write(source.join("Cargo.toml"), normalized).unwrap();
    if package_name != "demo" {
        let lock = source.join("Cargo.lock");
        let contents = fs::read_to_string(&lock).unwrap();
        fs::write(
            lock,
            contents.replace("name = \"demo\"", &format!("name = {package_name:?}")),
        )
        .unwrap();
    }
    source
}

pub(super) fn publisher_vcs(root: &Path, dirty: bool) -> Vec<u8> {
    serde_json::to_vec(&json!({
        "git": {"sha1": git_revision(root), "dirty": dirty},
        "path_in_vcs": "rust/crates/demo",
    }))
    .unwrap()
}
