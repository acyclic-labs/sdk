use super::*;

#[test]
fn portable_raw_posix_and_windows_names_remain_exact() -> Result<(), Box<dyn std::error::Error>> {
    let limits = VolumeLimits::default();
    let portable = PortablePath::parse("/workspace/src", limits)?;
    let converted = NamespacePath::from_portable(&portable, limits)?;
    assert_eq!(converted.depth(), 2);
    assert_eq!(converted.encoded_bytes(), 14);

    let posix = LogicalName::new(NameEncoding::PosixBytes, vec![0xff, b'a'], 255)?;
    let windows = LogicalName::new(
        NameEncoding::WindowsUtf16Le,
        "file".encode_utf16().flat_map(u16::to_le_bytes).collect(),
        255,
    )?;
    let native = NamespacePath::new(vec![posix.clone(), windows], limits)?;
    assert_eq!(
        native.split_last().and_then(|(parent, _)| parent.first()),
        Some(&posix)
    );
    assert!(native.is_within(&NamespacePath::new(vec![posix], limits)?));
    Ok(())
}

#[test]
fn bounds_are_checked_without_storage() -> Result<(), Box<dyn std::error::Error>> {
    let limits = VolumeLimits {
        maximum_path_bytes: 4,
        maximum_component_bytes: 2,
        maximum_path_depth: 1,
        ..VolumeLimits::default()
    };
    let name = LogicalName::new(NameEncoding::Utf8, b"ab".to_vec(), 2)?;
    assert!(NamespacePath::new(vec![name.clone()], limits).is_ok());
    assert_eq!(
        NamespacePath::new(vec![name.clone(), name], limits),
        Err(NamespacePathError::TooDeep)
    );

    let wide_limits = VolumeLimits {
        maximum_path_bytes: 64,
        maximum_component_bytes: 1,
        maximum_path_depth: 4,
        ..VolumeLimits::default()
    };
    let wide = LogicalName::new(NameEncoding::Utf8, b"ab".to_vec(), 2)?;
    assert_eq!(
        NamespacePath::new(vec![wide], wide_limits),
        Err(NamespacePathError::ComponentTooLong)
    );

    let short_path_limits = VolumeLimits {
        maximum_path_bytes: 2,
        maximum_component_bytes: 2,
        maximum_path_depth: 4,
        ..VolumeLimits::default()
    };
    let first = LogicalName::new(NameEncoding::Utf8, b"a".to_vec(), 2)?;
    let second = LogicalName::new(NameEncoding::Utf8, b"b".to_vec(), 2)?;
    assert_eq!(
        NamespacePath::new(vec![first, second], short_path_limits),
        Err(NamespacePathError::PathTooLong)
    );
    Ok(())
}
