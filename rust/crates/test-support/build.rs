#[path = "src/module_cases.rs"]
mod module_cases;

fn main() {
    println!("cargo:rerun-if-changed=src/module_cases.rs");
    println!("cargo:rerun-if-changed=build.rs");
    let output = std::path::PathBuf::from(std::env::var_os("OUT_DIR").expect("build output"));
    let compiler = std::env::var_os("RUSTC").expect("Cargo compiler");
    let mut reached = Vec::new();
    for (index, (declaration, child, leaf)) in module_cases::CASES.into_iter().enumerate() {
        let root = output.join("module-oracle").join(index.to_string());
        if root.exists() {
            std::fs::remove_dir_all(&root).expect("replace generated module fixture");
        }
        for (file, text) in [
            ("lib.rs", "mod root;"),
            ("root.rs", declaration),
            (child, "mod leaf;"),
            (
                leaf,
                "#[cfg(feature = \"test-support\")] compile_error!(\"policy descendant reached\");",
            ),
        ] {
            let file = root.join(file);
            std::fs::create_dir_all(file.parent().expect("fixture parent"))
                .expect("fixture directory");
            std::fs::write(file, text).expect("module fixture");
        }
        // This is a host compiler proof. The cross-built test executable only
        // scans the same fixtures with its target platform's path implementation.
        let compiled = std::process::Command::new(&compiler)
            .args([
                "--edition=2024",
                "--crate-type=lib",
                "--emit=metadata",
                "--cfg",
                "feature=\"test-support\"",
            ])
            .arg(root.join("lib.rs"))
            .arg("-o")
            .arg(root.join("proof.rmeta"))
            .output()
            .expect("module differential requires the build toolchain");
        let errors = String::from_utf8_lossy(&compiled.stderr);
        let found = !compiled.status.success() && errors.contains("policy descendant reached");
        assert!(found, "{declaration}: {errors}");
        reached.push(found);
    }
    std::fs::write(output.join("module-compiler.rs"), format!("{reached:?}"))
        .expect("compiler observations");
}
