//! Native device metadata uses the production OS codec and representability checks.
#![cfg_attr(any(target_os = "linux", target_os = "macos"), no_main)]

#[cfg(any(target_os = "linux", target_os = "macos"))]
libfuzzer_sys::fuzz_target!(|input: &[u8]| acyclic_fs::native_host::check_device_codec(input));

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn main() {
    eprintln!(
        "native_device requires a Unix device-number codec; run cargo fuzz run native_device on Linux or macOS"
    );
    std::process::exit(1);
}
