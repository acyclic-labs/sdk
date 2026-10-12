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
            let text = std::str::from_utf8(name.as_bytes()).map_err(|_| {
                MountSourceError::Invalid("authenticated UTF-8 name is malformed".into())
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
            return Err(MountSourceError::Unsupported(
                "authenticated name encoding is incompatible with this native mount".into(),
            ));
        }
    };
    Ok((bytes, inputs.retain()))
}

impl<A, O> CheckoutMountSource<A, O> {
    fn path(&self, path: &MountPath) -> Result<NamespacePath, MountSourceError> {
        let mut work = crate::demand::DemandWork::new(boundary_budget(), &self.cancellation);
        let mut carrier = NativeDemandCarrier::Borrowed(&mut work);
        let mut inputs = InputBuffers::new(&mut carrier)?;
        self.namespace_inputs(path, &mut inputs)
    }

    fn logical_name(&self, bytes: &[u8]) -> Result<LogicalName, MountSourceError> {
        let mut work = crate::demand::DemandWork::new(boundary_budget(), &self.cancellation);
        let mut carrier = NativeDemandCarrier::Borrowed(&mut work);
        let mut inputs = InputBuffers::new(&mut carrier)?;
        native_name(bytes, self.profile, self.limits, &mut inputs)
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
