//! Link settings for the `acyclic` executable.

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    let windows_msvc = std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("windows")
        && std::env::var("CARGO_CFG_TARGET_ENV").as_deref() == Ok("msvc");
    if windows_msvc {
        // Every hook is a fresh process, and loading these libraries and their
        // dependencies at startup costs about 1.5 ms that most hooks never
        // use. Delay-loading also lets the executable start where the optional
        // Projected File System is absent; mounting probes for it first.
        for library in [
            "projectedfslib.dll",
            "oleaut32.dll",
            "ws2_32.dll",
            "advapi32.dll",
        ] {
            println!("cargo:rustc-link-arg-bins=/DELAYLOAD:{library}");
        }
        println!("cargo:rustc-link-arg-bins=delayimp.lib");
    }
}
