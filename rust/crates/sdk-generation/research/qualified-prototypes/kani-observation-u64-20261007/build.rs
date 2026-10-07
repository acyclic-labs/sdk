use std::{env, fs, path::PathBuf};

fn main() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let production_candidates = [
        manifest_dir.join("../../../rust/crates/actors/src/generated/acyclic.actors.v1.rs"),
        manifest_dir.join(
            "../../../../../../rust/crates/actors/src/generated/acyclic.actors.v1.rs",
        ),
    ];
    let production = production_candidates
        .into_iter()
        .find(|candidate| candidate.is_file())
        .expect("locate current production Actors wire source");
    println!("cargo:rerun-if-changed={}", production.display());

    let source = fs::read_to_string(&production).expect("read production Actors wire source");
    let marker = "include!(\"acyclic.actors.v1.tonic.rs\");";
    let marker_pos = source
        .find(marker)
        .expect("production Actors source must retain tonic include marker");
    let mut wire_prefix = &source[..marker_pos];
    // The generated transport include is guarded by this cfg attribute; drop
    // the guard with the excluded include so the extracted source ends at a
    // complete item boundary.
    let transport_cfg = "#[cfg(not(target_arch = \"wasm32\"))]";
    if let Some(cfg_pos) = wire_prefix.rfind(transport_cfg) {
        wire_prefix = &wire_prefix[..cfg_pos];
    }
    let out = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR"));
    fs::write(out.join("actors_wire_types.rs"), wire_prefix)
        .expect("write extracted Actors wire source");
}
