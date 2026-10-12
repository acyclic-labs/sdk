pub(super) fn native_mount_name(
    name: &LogicalName,
    carrier: &mut NativeDemandCarrier<'_>,
) -> Result<(Vec<u8>, Option<crate::NativeAllocation>), MountSourceError> {
    let mut inputs = InputBuffers::new(carrier)?;
    inputs.items(1)?;
    let bytes = match (name.encoding(), std::env::consts::OS) {
        (NameEncoding::Utf8 | NameEncoding::PosixBytes, "linux" | "macos")
        | (NameEncoding::WindowsUtf16Le, "windows") => {
            let delta = crate::WorkCounters {
                bytes_copied: name.as_bytes().len() as u64,
                ..crate::WorkCounters::UNCHARGED
            };
            inputs.admit(delta)?;
            let mut bytes = inputs.vec(name.as_bytes().len())?;
            bytes.extend_from_slice(name.as_bytes());
            inputs.record(delta)?;
            bytes
        }
        (NameEncoding::Utf8, "windows") => {
            let text =
                std::str::from_utf8(name.as_bytes()).map_err(|_| MountSourceError::NativeText {
                    kind: super::NativeSourceTextKind::Invalid,
                    text: "authenticated UTF-8 name is malformed",
                })?;
            let mut length = 0_usize;
            for scalar in text.chars() {
                inputs.items(1)?;
                length = length
                    .checked_add(scalar.len_utf16() * 2)
                    .ok_or(MountSourceError::Work(WorkError::Overflow))?;
            }
            let mut bytes = inputs.vec(length)?;
            for unit in text.encode_utf16() {
                let delta = crate::WorkCounters {
                    items_examined: 1,
                    bytes_encoded: 2,
                    ..crate::WorkCounters::UNCHARGED
                };
                inputs.admit(delta)?;
                bytes.extend_from_slice(&unit.to_le_bytes());
                inputs.record(delta)?;
            }
            bytes
        }
        _ => {
            return Err(MountSourceError::NativeText {
                kind: super::NativeSourceTextKind::Unsupported,
                text: "authenticated name encoding is incompatible with this native mount",
            });
        }
    };
    Ok((bytes, inputs.retain()))
}

impl<A, O> CheckoutMountSource<A, O> {
    fn path(&self, path: &MountPath) -> Result<NamespacePath, MountSourceError> {
        let mut components = self.root.components().to_vec();
        components.extend(
            path.components()
                .iter()
                .map(|component| self.logical_name(component))
                .collect::<Result<Vec<_>, _>>()?,
        );
        NamespacePath::new(components, self.limits).map_err(engine_error)
    }

    /// The native spelling every case of `bytes` resolves to, or `None` for
    /// a name this volume cannot represent (which no lookup resolves).
    fn folded_name(&self, bytes: &[u8]) -> Option<Vec<u8>> {
        let name = self.logical_name(bytes).ok()?;
        let folded = LogicalName::new(
            name.encoding(),
            name.case_fold_key(),
            self.limits.maximum_component_bytes,
        )
        .ok()?;
        native_mount_name(
            &folded,
            &mut NativeDemandCarrier::Borrowed(&mut crate::demand::DemandWork::new(
                boundary_budget(),
                &self.cancellation,
            )),
        )
        .ok()
        .map(|result| result.0)
    }

    fn logical_name(&self, bytes: &[u8]) -> Result<LogicalName, MountSourceError> {
        let (encoding, logical_bytes) = match (self.profile, std::env::consts::OS) {
            (FilesystemProfile::Posix, "linux" | "macos") => {
                (NameEncoding::PosixBytes, bytes.to_vec())
            }
            (FilesystemProfile::Portable | FilesystemProfile::Browser, "linux" | "macos") => {
                std::str::from_utf8(bytes).map_err(|_| {
                    MountSourceError::Unsupported(
                        "portable volume cannot represent a non-UTF-8 native name".to_owned(),
                    )
                })?;
                (NameEncoding::Utf8, bytes.to_vec())
            }
            (FilesystemProfile::Windows, "windows") => {
                (NameEncoding::WindowsUtf16Le, bytes.to_vec())
            }
            (FilesystemProfile::Portable | FilesystemProfile::Browser, "windows") => {
                let units = crate::kernel::types::utf16le_units(bytes)
                    .ok_or_else(|| {
                        MountSourceError::Invalid(
                            "native UTF-16 name has an odd byte length".to_owned(),
                        )
                    })?
                    .collect::<Vec<_>>();
                let value = String::from_utf16(&units).map_err(|_| {
                    MountSourceError::Unsupported(
                        "portable volume cannot represent an unpaired UTF-16 native name"
                            .to_owned(),
                    )
                })?;
                (NameEncoding::Utf8, value.into_bytes())
            }
            _ => {
                return Err(MountSourceError::Unsupported(
                    "volume name profile is incompatible with this native mount".to_owned(),
                ));
            }
        };
        LogicalName::new(encoding, logical_bytes, self.limits.maximum_component_bytes)
            .map_err(engine_error)
    }

    fn posix_attribute_name(&self, bytes: &[u8]) -> Result<AttributeName, MountSourceError> {
        if self.profile != FilesystemProfile::Posix {
            return Err(MountSourceError::Unsupported(
                "native POSIX attributes require a POSIX volume profile".to_owned(),
            ));
        }
        AttributeName::new(
            AttributeClass::PosixXattr,
            bytes.to_vec(),
            self.limits.maximum_component_bytes,
        )
        .map_err(engine_error)
    }
}
fn profile_is_native(profile: FilesystemProfile) -> bool {
    match std::env::consts::OS {
        "linux" | "macos" => !matches!(profile, FilesystemProfile::Windows),
        "windows" => !matches!(profile, FilesystemProfile::Posix),
        _ => false,
    }
}

fn profile_name(profile: FilesystemProfile) -> &'static str {
    match profile {
        FilesystemProfile::Portable => "portable",
        FilesystemProfile::Posix => "posix",
        FilesystemProfile::Windows => "windows",
        FilesystemProfile::Browser => "browser",
    }
}
