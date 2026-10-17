//! Linux FUSE descriptor protocol; unsupported hosts fail explicitly.
#![cfg_attr(target_os = "linux", no_main)]
#[cfg(target_os = "linux")]
#[path = "fuse_reply/linux.rs"]
mod linux;

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("fuse_reply requires Linux SCM_RIGHTS and descriptor retirement semantics");
    std::process::exit(1);
}
