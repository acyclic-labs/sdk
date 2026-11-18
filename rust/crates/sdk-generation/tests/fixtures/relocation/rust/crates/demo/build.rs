fn main() {
    std::fs::write(
        std::path::PathBuf::from(std::env::var("OUT_DIR").unwrap()).join("wire.rs"),
        "pub struct GeneratedWire;\n",
    )
    .unwrap();
}
