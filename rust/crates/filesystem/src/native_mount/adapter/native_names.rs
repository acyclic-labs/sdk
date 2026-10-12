use super::*;

fn native_name_layout(bytes: &[u8], profile: FilesystemProfile, limits: VolumeLimits,
    inputs: &mut InputBuffers<'_, '_>,
) -> Result<(NameEncoding, usize), MountSourceError> {
    inputs.charge(crate::WorkCounters { source_path_components: 1, items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
    match (profile, std::env::consts::OS) {
        (FilesystemProfile::Posix, "linux" | "macos") => Ok((NameEncoding::PosixBytes, bytes.len())),
        (FilesystemProfile::Windows, "windows") => Ok((NameEncoding::WindowsUtf16Le, bytes.len())),
        (FilesystemProfile::Portable | FilesystemProfile::Browser, "linux" | "macos") => {
            let value = std::str::from_utf8(bytes).map_err(|_| MountSourceError::NativeText {
                kind: super::NativeSourceTextKind::Unsupported, text: "portable volume cannot represent a non-UTF-8 native name",
            })?;
            let length = LogicalName::validate_unicode(NameEncoding::Utf8, value, limits.maximum_component_bytes)
                .map_err(|error| MountSourceError::NamespacePath(crate::kernel::NamespacePathError::Name(error)))?;
            Ok((NameEncoding::Utf8, length))
        }
        (FilesystemProfile::Portable | FilesystemProfile::Browser, "windows") => {
            let units = crate::kernel::types::utf16le_units(bytes).ok_or(MountSourceError::NativeText {
                kind: super::NativeSourceTextKind::Invalid, text: "native UTF-16 name has an odd byte length",
            })?;
            let mut length = 0_usize;
            for scalar in char::decode_utf16(units) {
                inputs.items(1)?;
                let scalar = scalar.map_err(|_| MountSourceError::NativeText {
                    kind: super::NativeSourceTextKind::Unsupported, text: "portable volume cannot represent an unpaired UTF-16 native name",
                })?;
                length = length.checked_add(scalar.len_utf8()).ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
            }
            Ok((NameEncoding::Utf8, length))
        }
        _ => Err(MountSourceError::NativeText {
            kind: super::NativeSourceTextKind::Unsupported, text: "volume name profile is incompatible with this native mount",
        }),
    }
}

fn copy_name(encoding: NameEncoding, bytes: &[u8], limits: VolumeLimits,
    inputs: &mut InputBuffers<'_, '_>,
) -> Result<LogicalName, MountSourceError> {
    inputs.admit(crate::WorkCounters { bytes_copied: bytes.len() as u64, ..crate::WorkCounters::UNCHARGED })?;
    let mut owned = inputs.vec(bytes.len())?;
    owned.extend_from_slice(bytes);
    inputs.record(crate::WorkCounters { bytes_copied: bytes.len() as u64, ..crate::WorkCounters::UNCHARGED })?;
    LogicalName::new(encoding, owned, limits.maximum_component_bytes)
        .map_err(|error| MountSourceError::NamespacePath(crate::kernel::NamespacePathError::Name(error)))
}

impl<A, O> CheckoutMountSource<A, O> {
    fn namespace_inputs(&self, path: &MountPath, inputs: &mut InputBuffers<'_, '_>)
        -> Result<NamespacePath, MountSourceError>
    {
        let mut components = path.components().iter();
        let mut error = None;
        let lengths = self.root.components().iter().map(|name| name.as_bytes().len())
            .chain(std::iter::from_fn(|| {
                if error.is_some() { return None; }
                let component = components.next()?;
                match native_name_layout(component, self.profile, self.limits, inputs) {
                    Ok((_, length)) => Some(length),
                    Err(failure) => { error = Some(failure); None },
                }
            }));
        let checked = NamespacePath::validate_component_lengths(lengths, self.limits);
        if let Some(error) = error { return Err(error); }
        checked.map_err(MountSourceError::NamespacePath)?;
        let count = self.root.depth().checked_add(path.components().len())
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        let mut names = inputs.vec::<LogicalName>(count)?;
        for name in self.root.components() {
            names.push(copy_name(name.encoding(), name.as_bytes(), self.limits, inputs)?);
        }
        for component in path.components() {
            let (encoding, length) = native_name_layout(component, self.profile, self.limits, inputs)?;
            if cfg!(windows) && encoding == NameEncoding::Utf8 {
                inputs.admit(crate::WorkCounters { bytes_encoded: length as u64, ..crate::WorkCounters::UNCHARGED })?;
                let mut owned = inputs.vec(length)?;
                for scalar in char::decode_utf16(crate::kernel::types::utf16le_units(component).ok_or(MountSourceError::Stale)?) {
                    inputs.items(1)?;
                    let scalar = scalar.map_err(|_| MountSourceError::Stale)?;
                    let mut buffer = [0_u8; 4];
                    let encoded = scalar.encode_utf8(&mut buffer);
                    owned.extend_from_slice(encoded.as_bytes());
                    inputs.record(crate::WorkCounters { bytes_encoded: encoded.len() as u64, ..crate::WorkCounters::UNCHARGED })?;
                }
                names.push(LogicalName::new(encoding, owned, self.limits.maximum_component_bytes)
                    .map_err(|error| MountSourceError::NamespacePath(crate::kernel::NamespacePathError::Name(error)))?);
            } else { names.push(copy_name(encoding, component, self.limits, inputs)?); }
        }
        inputs.items(names.len())?;
        NamespacePath::new(names, self.limits).map_err(MountSourceError::NamespacePath)
    }

    fn capture_inputs(&self, source_root: &Path, paths: &[MountPath], expected_root_identity: NativeRootIdentity,
        scope: HostCaptureScope, work: &crate::NativeMaterializationWork,
    ) -> Result<(Vec<NamespacePath>, CaptureOptions, crate::NativeAllocation), MountSourceError> {
        if matches!(scope, HostCaptureScope::ExactPaths) && paths.len() > MAX_NATIVE_EXACT_CAPTURE_PATHS {
            return Err(MountSourceError::NativeText { kind: super::NativeSourceTextKind::Invalid,
                text: "capture path set exceeds the native per-operation limit" });
        }
        let mut carrier = NativeDemandCarrier::Native(work);
        let mut inputs = InputBuffers::new(&mut carrier)?;
        let mut rooted = inputs.vec(paths.len())?;
        for path in paths { rooted.push(self.namespace_inputs(path, &mut inputs)?); }
        let source_root = inputs.clone_path(source_root)?;
        let maximum_paths = match scope {
            HostCaptureScope::ExactPaths => u32::try_from(rooted.len()).map_err(|_| MountSourceError::Work(crate::WorkError::Overflow))?,
            HostCaptureScope::Subtree => MAX_NATIVE_SUBTREE_CAPTURE_PATHS,
        };
        let options = CaptureOptions { source_root, expected_root_identity, maximum_paths, maximum_extent_spans: 65_536 };
        Ok((rooted, options, inputs.retain().expect("native capture input")))
    }
}
