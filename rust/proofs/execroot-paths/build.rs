//! Writes the file the probe crate reads back through `OUT_DIR`.

fn main() {
    let out = std::env::var_os("OUT_DIR").expect("cargo_build_script sets OUT_DIR");
    std::fs::write(
        std::path::Path::new(&out).join("probe.txt"),
        "written by the build script",
    )
    .expect("the build script's OUT_DIR is writable");
}
