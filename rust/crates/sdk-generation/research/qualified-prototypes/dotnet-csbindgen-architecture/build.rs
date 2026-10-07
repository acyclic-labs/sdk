use sha2::{Digest, Sha256};
use std::{env, fs, path::PathBuf};

fn hash(path: &PathBuf) -> String {
    let mut hasher = Sha256::new();
    hasher.update(fs::read(path).expect("source input must be readable"));
    format!("sha256:{:x}", hasher.finalize())
}

fn main() {
    let root = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap());
    let generated = root.join("generated/NativeMethods.g.cs");
    fs::create_dir_all(generated.parent().unwrap()).unwrap();

    csbindgen::Builder::default()
        .input_extern_file(root.join("src/lib.rs"))
        .csharp_dll_name("dotnet_csbindgen_architecture_probe")
        .csharp_namespace("Actors.ArchitectureProbe.Native")
        .csharp_class_name("NativeMethods")
        .csharp_class_accessibility("internal")
        .generate_csharp_file(&generated)
        .expect("csbindgen declaration generation failed");

    let receipt = format!(
        "{{\n  \"schema\": \"csbindgen-architecture-receipt-v1\",\n  \"generator\": \"csbindgen=1.9.8\",\n  \"source_sha256\": \"{}\",\n  \"manifest_sha256\": \"{}\",\n  \"lock_sha256\": \"{}\",\n  \"generated_sha256\": \"{}\",\n  \"managed_facade_sha256\": \"{}\",\n  \"conventions\": [\"u64\", \"explicit-presence\", \"status-int\", \"opaque-safehandle\"]\n}}\n",
        hash(&root.join("src/lib.rs")),
        hash(&root.join("Cargo.toml")),
        hash(&root.join("Cargo.lock")),
        hash(&generated),
        hash(&root.join("managed/Program.cs")),
    );
    fs::write(root.join("generated/binding-receipt.json"), receipt).unwrap();
}
