//! Build the native input cancellation boundary.
fn main() {
    println!("cargo:rerun-if-changed=src/input_reader.c");
    println!("cargo:rerun-if-changed=src/input_reader_counts.c");
    if std::env::var("CARGO_CFG_TARGET_FAMILY").as_deref() == Ok("unix") {
        cc::Build::new()
            .file("src/input_reader.c")
            .flag_if_supported("-std=c11")
            .compile("acyclic_input_reader");
        if std::env::var_os("CARGO_FEATURE_TEST_SUPPORT").is_some() {
            cc::Build::new()
                .file("src/input_reader_counts.c")
                .flag_if_supported("-std=c11")
                .compile("acyclic_input_reader_counts");
        }
        println!("cargo:rustc-link-lib=pthread");
    }
}
