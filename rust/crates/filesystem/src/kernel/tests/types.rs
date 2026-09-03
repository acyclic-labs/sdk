use super::*;

#[test]
fn windows_names_validate_utf16_without_lossy_normalization()
-> Result<(), Box<dyn std::error::Error>> {
    let emoji: Vec<u8> = "😀.rs".encode_utf16().flat_map(u16::to_le_bytes).collect();
    let name = LogicalName::new(NameEncoding::WindowsUtf16Le, emoji.clone(), 255)?;
    assert_eq!(name.as_bytes(), emoji);

    for invalid in [
        vec![0x00, 0xd8],
        vec![b'/', 0],
        vec![b'\\', 0],
        vec![b'.', 0],
        vec![b'.', 0, b'.', 0],
    ] {
        assert!(matches!(
            LogicalName::new(NameEncoding::WindowsUtf16Le, invalid, 255),
            Err(TreePageError::InvalidName)
        ));
    }
    Ok(())
}

#[test]
fn names_kinds_and_profiles_are_total() -> Result<(), Box<dyn std::error::Error>> {
    for (encoding, tag) in [
        (NameEncoding::Utf8, 1),
        (NameEncoding::PosixBytes, 2),
        (NameEncoding::WindowsUtf16Le, 3),
    ] {
        assert_eq!(encoding.tag(), tag);
        assert_eq!(NameEncoding::from_tag(tag)?, encoding);
    }
    assert_eq!(
        NameEncoding::from_tag(0),
        Err(TreePageError::UnknownNameEncoding(0))
    );
    assert_eq!(
        LogicalName::new(NameEncoding::Utf8, Vec::new(), 8),
        Err(TreePageError::EmptyName)
    );
    assert_eq!(
        LogicalName::new(NameEncoding::Utf8, b"abc".to_vec(), 2),
        Err(TreePageError::NameTooLarge)
    );
    for (encoding, bytes) in [
        (NameEncoding::Utf8, vec![0xff]),
        (NameEncoding::Utf8, b".".to_vec()),
        (NameEncoding::Utf8, b"..".to_vec()),
        (NameEncoding::Utf8, b"a/b".to_vec()),
        (NameEncoding::Utf8, b"a\0b".to_vec()),
        (NameEncoding::PosixBytes, b".".to_vec()),
        (NameEncoding::PosixBytes, b"..".to_vec()),
        (NameEncoding::PosixBytes, b"a/b".to_vec()),
        (NameEncoding::PosixBytes, b"a\0b".to_vec()),
        (NameEncoding::WindowsUtf16Le, vec![b'a']),
    ] {
        assert_eq!(
            LogicalName::new(encoding, bytes, 8),
            Err(TreePageError::InvalidName)
        );
    }

    let kinds = [
        FileKind::Regular,
        FileKind::Directory,
        FileKind::SymbolicLink,
        FileKind::Fifo,
        FileKind::Socket,
        FileKind::CharacterDevice,
        FileKind::BlockDevice,
        FileKind::ReparsePoint,
        FileKind::MountBoundary,
    ];
    for (index, kind) in kinds.into_iter().enumerate() {
        let tag = u8::try_from(index + 1)?;
        assert_eq!(kind.tag(), tag);
        assert_eq!(FileKind::from_tag(tag)?, kind);
    }
    assert_eq!(
        FileKind::from_tag(0),
        Err(TreePageError::UnknownFileKind(0))
    );
    for kind in [
        FileKind::Regular,
        FileKind::Directory,
        FileKind::SymbolicLink,
        FileKind::MountBoundary,
    ] {
        for profile in [
            FilesystemProfile::Portable,
            FilesystemProfile::Posix,
            FilesystemProfile::Windows,
            FilesystemProfile::Browser,
        ] {
            assert!(kind.is_supported_by_profile(profile));
        }
    }
    for kind in [
        FileKind::Fifo,
        FileKind::Socket,
        FileKind::CharacterDevice,
        FileKind::BlockDevice,
    ] {
        assert!(kind.is_supported_by_profile(FilesystemProfile::Posix));
        assert!(!kind.is_supported_by_profile(FilesystemProfile::Portable));
    }
    assert!(FileKind::ReparsePoint.is_supported_by_profile(FilesystemProfile::Windows));
    assert!(!FileKind::ReparsePoint.is_supported_by_profile(FilesystemProfile::Posix));
    Ok(())
}

#[test]
fn tree_page_validation_rejects_every_malformed_shape() -> Result<(), Box<dyn std::error::Error>> {
    let a = LogicalName::new(NameEncoding::Utf8, b"a".to_vec(), 8)?;
    let b = LogicalName::new(NameEncoding::Utf8, b"b".to_vec(), 8)?;
    let entry = |name| TreeEntry {
        name,
        file_id: FileId::from_bytes([1; 16]),
        kind: FileKind::Regular,
    };
    assert_eq!(TreePage::Leaf(Vec::new()).validate(0), Ok(()));
    assert_eq!(
        TreePage::Leaf(vec![entry(a.clone())]).validate(0),
        Err(TreePageError::TooManyItems)
    );
    assert_eq!(
        TreePage::Leaf(vec![entry(b.clone()), entry(a.clone())]).validate(2),
        Err(TreePageError::NamesNotStrictlyOrdered)
    );
    assert_eq!(
        TreePage::Internal(Vec::new()).validate(2),
        Err(TreePageError::EmptyInternalPage)
    );
    assert_eq!(
        TreePage::Internal(vec![TreeChild {
            first_name: a.clone(),
            page: digest_object(ObjectKind::Blob, [1; 32]),
        }])
        .validate(2),
        Err(TreePageError::WrongChildKind)
    );
    assert_eq!(
        TreePage::Internal(vec![
            TreeChild {
                first_name: b,
                page: digest_object(ObjectKind::TreePage, [1; 32]),
            },
            TreeChild {
                first_name: a,
                page: digest_object(ObjectKind::TreePage, [2; 32]),
            },
        ])
        .validate(2),
        Err(TreePageError::NamesNotStrictlyOrdered)
    );
    Ok(())
}

#[test]
fn extent_page_validation_rejects_every_malformed_shape() {
    let page = |kind| digest_object(kind, [1; 32]);
    let extent = |offset, length, kind| Extent {
        offset,
        length,
        kind,
    };
    let cases = [
        (
            ExtentPage::Leaf(vec![extent(0, 0, ExtentKind::Hole)]),
            ExtentPageError::ZeroLength,
        ),
        (
            ExtentPage::Leaf(vec![extent(u64::MAX, 2, ExtentKind::Hole)]),
            ExtentPageError::RangeOverflow,
        ),
        (
            ExtentPage::Leaf(vec![
                extent(0, 1, ExtentKind::Hole),
                extent(2, 1, ExtentKind::AllocatedZero),
            ]),
            ExtentPageError::NonContiguous,
        ),
        (
            ExtentPage::Leaf(vec![extent(
                0,
                1,
                ExtentKind::Content {
                    object: page(ObjectKind::Metadata),
                    object_offset: 0,
                },
            )]),
            ExtentPageError::WrongContentKind,
        ),
        (
            ExtentPage::Leaf(vec![extent(
                0,
                2,
                ExtentKind::Content {
                    object: page(ObjectKind::Blob),
                    object_offset: u64::MAX,
                },
            )]),
            ExtentPageError::RangeOverflow,
        ),
        (
            ExtentPage::Internal(Vec::new()),
            ExtentPageError::EmptyInternalPage,
        ),
        (
            ExtentPage::Internal(vec![ExtentChild {
                first_offset: 1,
                end_offset: 1,
                page: page(ObjectKind::ExtentPage),
            }]),
            ExtentPageError::InvalidChildRange,
        ),
        (
            ExtentPage::Internal(vec![ExtentChild {
                first_offset: 0,
                end_offset: 1,
                page: page(ObjectKind::Blob),
            }]),
            ExtentPageError::WrongChildKind,
        ),
        (
            ExtentPage::Internal(vec![
                ExtentChild {
                    first_offset: 0,
                    end_offset: 1,
                    page: page(ObjectKind::ExtentPage),
                },
                ExtentChild {
                    first_offset: 2,
                    end_offset: 3,
                    page: page(ObjectKind::ExtentPage),
                },
            ]),
            ExtentPageError::NonContiguous,
        ),
    ];
    for (page, error) in cases {
        assert_eq!(page.validate(8), Err(error));
    }
    assert_eq!(
        ExtentPage::Leaf(vec![extent(0, 1, ExtentKind::Hole)]).validate(0),
        Err(ExtentPageError::TooManyItems)
    );
}
