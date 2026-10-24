//! Shared fuzz/property exercises for production filesystem boundaries.
pub mod access_input;
/// Shared path input property exercise.
pub mod path_input;

/// Exercises the production FUSE reply decoder and descriptor custody.
#[cfg(all(target_os = "linux", feature = "native-mount"))]
pub fn fuse_reply(input: &[u8]) {
    crate::native_mount::fuzz_reply(input);
}

/// Platform-independent Windows callback path codecs used by property tests.
#[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
pub mod callback_path {
    /// Decode callback UTF-16 paths into canonical byte components.
    pub fn components(wide: &[u16]) -> Option<Vec<Vec<u8>>> {
        crate::native_mount::callback_path::components(wide)
    }
    /// Encode validated byte components into a Windows callback path.
    pub fn wide_components(parts: &[Vec<u8>]) -> Option<impl Iterator<Item = u16> + Clone + '_> {
        crate::native_mount::callback_path::wide_components(parts)
    }
}
