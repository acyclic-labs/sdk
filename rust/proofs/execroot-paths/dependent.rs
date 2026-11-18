//! Compiles against the probe crate's metadata, so linking it with the probe's rlib proves the two
//! actions produced one crate.

/// The probe crate's build script file.
pub fn probe() -> &'static str {
    execroot_probe::PROBE
}
