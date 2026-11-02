use super::*;

const AUTHORED: &str = "authored-path-v1:";

/// Exercises both real decoders and the original native result custody.
#[cfg(all(
    any(test, feature = "test-support"),
    feature = "filesystem",
    not(target_arch = "wasm32")
))]
pub fn check_version(input: &[u8]) {
    let Ok(version) = std::str::from_utf8(input) else {
        return;
    };
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .expect("version control admits its original ledger");
    let baseline = work.live_allocation_bytes();
    let portable = decode_selection(version, None);
    let native = decode_selection(version, Some(&work));
    match (&portable, &native) {
        (Ok((SelectedVersion::Content(left), _)), Ok((SelectedVersion::Content(right), _))) => {
            assert_eq!(left, right);
        }
        (Ok((SelectedVersion::Authored(left), _)), Ok((SelectedVersion::Authored(right), _))) => {
            assert_eq!(left, right);
        }
        (Err(_), Err(_)) => {}
        _ => panic!("version decoding disagrees across original native admission"),
    }
    if let Ok((selected, _)) = &native {
        assert!(decode_version(selected.path(), version, Some(&work)).is_ok());
        let mut other_path = selected.path().to_owned();
        other_path.push('\0');
        assert!(decode_version(&other_path, version, Some(&work)).is_err());
    }
    drop(native);
    assert_eq!(work.live_allocation_bytes(), baseline);
    cancellation.cancel();
    assert!(decode_selection(version, Some(&work)).is_err());
    assert_eq!(work.live_allocation_bytes(), baseline);
}

/// A version pins a namespace selection, without inventing file metadata.
/// Only the original read producer supplies the length in FileReadSource.
pub(super) enum SelectedVersion {
    Content(FileRef),
    Authored(crate::conversation::PathSelection),
}

impl SelectedVersion {
    pub(super) fn volume(&self) -> &crate::conversation::VolumeRef {
        match self {
            Self::Content(file) => file.volume(),
            Self::Authored(path) => &path.volume,
        }
    }
    pub(super) fn path(&self) -> &str {
        match self {
            Self::Content(file) => file.path(),
            Self::Authored(path) => &path.path,
        }
    }
    fn validate(&self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>) -> Result<()> {
        match self {
            Self::Content(file) => file.validate(work),
            Self::Authored(path) => path.validate(work),
        }
    }
    #[cfg(feature = "filesystem")]
    fn logical_owned_bytes(&self) -> Result<u64> {
        match self {
            Self::Content(file) => file.logical_owned_bytes(),
            Self::Authored(path) => path.logical_owned_bytes(),
        }
    }
    pub(super) fn matches_source(&self, source: &FileReadSource) -> bool {
        match (self, source) {
            (Self::Content(expected), FileReadSource::Content(actual)) => expected == actual,
            (Self::Authored(expected), FileReadSource::Authored(actual)) => {
                expected.matches_source(actual)
            }
            (Self::Authored(expected), FileReadSource::Content(actual)) => {
                expected.matches_content(actual)
            }
            (Self::Content(expected), FileReadSource::Authored(actual)) => {
                actual.path_selection().matches_content(expected)
                    && actual.byte_length == expected.descriptor().byte_length()
            }
        }
    }
}

pub(super) fn decode_tool_version(
    context: Option<&ToolContext>,
    path: &str,
    version: &str,
    work: crate::NativeWork<'_>,
) -> Result<(SelectedVersion, crate::NativeCustody)> {
    let Some(context) = context else {
        return decode_version(path, version, work);
    };
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    let owned_io = work
        .map(crate::executor::PreparationIo::from_native_work)
        .transpose()?;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    let io = owned_io.as_ref();
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let io = None;
    if path.starts_with(crate::layers::EFFECTIVE_PATH) {
        let (file, custody) = decode_selection(version, work)?;
        let layer = context.effective_tool_layer(path, Some((file.volume(), file.path())), io)?;
        context.authorize_tool_path_read(&layer, io)?;
        return Ok((file, custody));
    }
    let (volume, relative) = context.tool_path_selection(path, io)?;
    let (file, custody) = decode_version(&relative, version, work)?;
    if file.volume() != volume {
        return Err(super::executor::rejection(
            work,
            "file version belongs to another selected volume",
        ));
    }
    Ok((file, custody))
}

// A token transports the original reference; it is never an authority or an
// index into mutable process state. Its decoded reference goes through the
// same grant and immutable-content checks as every other content read.
pub(super) fn decode_version(
    path: &str,
    version: &str,
    work: crate::NativeWork<'_>,
) -> Result<(SelectedVersion, crate::NativeCustody)> {
    let (selected, custody) = decode_selection(version, work)?;
    if selected.path() != path {
        return Err(super::executor::rejection(
            work,
            "file version belongs to another path",
        ));
    }
    Ok((selected, custody))
}

pub(super) fn decode_selection(
    version: &str,
    work: crate::NativeWork<'_>,
) -> Result<(SelectedVersion, crate::NativeCustody)> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = work {
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let (file, mut reservation) = if let Some(selected) = version.strip_prefix(AUTHORED) {
            let (file, reservation) = io.decode::<crate::conversation::PathSelection>(
                std::marker::PhantomData,
                selected.as_bytes(),
                crate::wire::ErrorCode::Invalid,
            )?;
            (SelectedVersion::Authored(file), reservation)
        } else {
            let (file, reservation) = io.decode::<FileRef>(
                std::marker::PhantomData,
                version.as_bytes(),
                crate::wire::ErrorCode::Invalid,
            )?;
            (SelectedVersion::Content(file), reservation)
        };
        crate::executor::with_history_io(Some(&io), |history| file.validate(history))
            .map_err(Error::tool_input)?;
        let retained = (std::mem::size_of::<SelectedVersion>() as u64)
            .checked_add(file.logical_owned_bytes()?)
            .ok_or_else(|| Error::tool_rejected("file version ownership overflow"))?;
        reservation.substitute(retained)?;
        reservation.into_retained()?;
        return Ok((file, Some(io.into_native_custody(retained)?)));
    }
    let file = if let Some(selected) = version.strip_prefix(AUTHORED) {
        SelectedVersion::Authored(
            serde_json::from_str(selected).map_err(|error| Error::Invalid(error.to_string()))?,
        )
    } else {
        SelectedVersion::Content(
            serde_json::from_str(version).map_err(|error| Error::Invalid(error.to_string()))?,
        )
    };
    file.validate(&mut None)?;
    Ok((file, None))
}

pub(crate) fn encode_version(
    file: &FileRef,
    work: crate::NativeWork<'_>,
) -> Result<(String, crate::NativeCustody)> {
    encode_token(file, "", work)
}

pub(super) fn encode_source(
    file: &FileReadSource,
    work: crate::NativeWork<'_>,
) -> Result<(String, crate::NativeCustody)> {
    match file {
        FileReadSource::Content(file) => encode_version(file, work),
        FileReadSource::Authored(file) => {
            super::encode_path_version(&file.volume, &file.path, &file.generation, work)
        }
    }
}

/// Encodes the original namespace producer's selection, without materializing
/// a body, synthesizing a digest or claiming a regular-file length.
pub(crate) fn encode_path_version(
    volume: &crate::conversation::VolumeRef,
    path: &str,
    generation: &crate::resources::GenerationRef,
    work: crate::NativeWork<'_>,
) -> Result<(String, crate::NativeCustody)> {
    encode_token(
        &crate::conversation::PathSelectionView {
            volume,
            path,
            generation,
        },
        AUTHORED,
        work,
    )
}

fn encode_token<T: Serialize>(
    file: &T,
    prefix: &str,
    work: crate::NativeWork<'_>,
) -> Result<(String, crate::NativeCustody)> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = work {
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let bytes = io.encode(file, work.remaining()?.bytes_encoded, false)?;
        // The shared codec produces UTF-8 JSON; display retains the string on
        // the same original ledger while its encoded scratch is still alive.
        let text = std::str::from_utf8(&bytes).map_err(|_| Error::NativeBoundary {
            code: crate::wire::ErrorCode::Storage,
            reason: "file version encoder returned invalid UTF-8",
        })?;
        let (text, custody) = work.display(&format_args!("{prefix}{text}"))?;
        return Ok((text, Some(custody)));
    }
    serde_json::to_string(file)
        .map(|text| {
            (
                if prefix.is_empty() {
                    text
                } else {
                    format!("{prefix}{text}")
                },
                None,
            )
        })
        .map_err(|error| Error::Invalid(error.to_string()))
}
