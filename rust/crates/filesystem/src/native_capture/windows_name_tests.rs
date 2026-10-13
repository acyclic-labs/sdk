use super::*;

#[cfg(all(test, windows))]
mod windows_name_tests {
    use super::*;

    #[test]
    fn posix_profile_names_round_trip_through_utf8_on_windows()
    -> Result<(), Box<dyn std::error::Error>> {
        let host_path = |name| {
            let path = NamespacePath::new(vec![name], crate::model::VolumeLimits::default())
                .map_err(CaptureError::from)?;
            namespace_to_host_path(&path)
        };
        let valid = LogicalName::new(
            NameEncoding::PosixBytes,
            "uni-ÃƒÂ©Ã¤Â¸Â­.txt".as_bytes().to_vec(),
            255,
        )?;
        assert_eq!(
            host_path(valid)?,
            std::ffi::OsString::from("uni-ÃƒÂ©Ã¤Â¸Â­.txt")
        );

        let invalid = LogicalName::new(NameEncoding::PosixBytes, vec![0xff], 255)?;
        assert!(matches!(
            host_path(invalid),
            Err(CaptureError::UnrepresentablePath)
        ));
        let alias = LogicalName::new(NameEncoding::PosixBytes, b"a\\b".to_vec(), 255)?;
        assert!(matches!(
            host_path(alias),
            Err(CaptureError::UnrepresentablePath)
        ));
        Ok(())
    }

    #[test]
    fn host_path_round_trips_through_windows_profile() -> Result<(), Box<dyn std::error::Error>> {
        let limits = crate::model::VolumeLimits {
            maximum_component_bytes: 510,
            ..crate::model::VolumeLimits::default()
        };
        let host = std::path::Path::new("src/ÃƒÂ¼nÃƒÂ¯cÃƒÂ¸de/Ã¦â€”Â¥Ã¦Å“Â¬Ã¨ÂªÅ¾.rs");
        let namespace = host_path_to_namespace(host, FilesystemProfile::Windows, limits)?;
        assert_eq!(namespace_to_host_path(&namespace)?, host);
        Ok(())
    }
}
