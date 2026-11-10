#[cfg(any(windows, test, feature = "native-mount"))]
pub(crate) fn windows_unicode_units(text: &str) -> Option<std::str::EncodeUtf16<'_>> {
    (!text.contains(['/', '\\'])).then(|| text.encode_utf16())
}

/// Borrows one canonical component as Windows units, without permitting a
/// logical name to introduce another native path component.
#[cfg(any(windows, test))]
pub(crate) fn windows_name_units(
    name: &crate::kernel::LogicalName,
) -> Option<impl Iterator<Item = u16> + Clone + '_> {
    let (native, unicode) = match name.encoding() {
        crate::kernel::NameEncoding::WindowsUtf16Le => (
            Some(crate::kernel::types::utf16le_units(name.as_bytes())?),
            None,
        ),
        crate::kernel::NameEncoding::Utf8 | crate::kernel::NameEncoding::PosixBytes => {
            let text = std::str::from_utf8(name.as_bytes()).ok()?;
            (None, Some(windows_unicode_units(text)?))
        }
    };
    Some(
        native
            .into_iter()
            .flatten()
            .chain(unicode.into_iter().flatten()),
    )
}

#[cfg(any(test, feature = "native-watch"))]
mod encoding;
#[cfg(any(test, feature = "native-mount"))]
pub(crate) use encoding::host_name_bytes_with_native_work;
#[cfg(all(windows, any(test, feature = "native-watch")))]
pub(crate) use encoding::host_wide_name_bytes_with_source_work;
#[cfg(any(test, feature = "native-watch"))]
pub(crate) use encoding::{host_name_bytes, host_name_bytes_with_source_work};
