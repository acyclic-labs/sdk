use super::*;
use crate::native_host::{self, NativeHostWork};

#[derive(Default)]
pub(in crate::native_mount) struct ProjFsRecoveryControl {
    directory: Option<cap_std::fs::Dir>,
    identity: Option<crate::NativeRootIdentity>,
    close: Option<crate::NativeWorkReservation>,
}

impl ProjFsRecoveryControl {
    pub(in crate::native_mount) fn prepare_retirement(
        &mut self,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        self.close = Some(work.allocation_work().reserve_work(crate::WorkCounters {
            backend_write_operations: 1,
            ..crate::WorkCounters::UNCHARGED
        })?);
        Ok(())
    }

    pub(in crate::native_mount) fn retire_controls(&mut self) -> Result<(), NativeMountError> {
        if self.directory.is_some() {
            self.close
                .as_mut()
                .ok_or(NativeMountError::ControlPending)?
                .started_drop_io(crate::WorkCounters {
                    backend_write_operations: 1,
                    ..crate::WorkCounters::UNCHARGED
                });
            drop(self.directory.take());
        }
        Ok(())
    }

    pub(in crate::native_mount) fn bind_root_identity(
        &mut self,
        identity: crate::NativeRootIdentity,
    ) {
        self.identity = Some(identity);
    }

    pub(in crate::native_mount) fn recover<W: NativeHostWork>(
        &mut self,
        destination: &std::path::Path,
        disposable: bool,
        work: &mut W,
    ) -> Result<(), NativeMountError>
    where
        NativeMountError: From<W::Error>,
    {
        let (directory, identity) = match native_host::open_root_directory(destination, work) {
            Ok(opened) => opened,
            Err(error) if W::io_error_kind(&error) == Some(std::io::ErrorKind::NotFound) => {
                return Ok(());
            }
            Err(error) => return Err(error.into()),
        };
        if self.identity != Some(identity) {
            return Err(NativeMountError::Demand(crate::DemandError::StaleSource));
        }
        if self.directory.is_none() {
            self.directory = Some(directory);
        }
        if disposable
            && let Some(tag) =
                reparse_tag_held(self.directory.as_ref().expect("held recovery root"), work)?
        {
            return Err(NativeMountError::UnsafeRecoveryDestination {
                reparse_tag: Some(tag),
            });
        }
        self.retire_controls()
    }

    pub(in crate::native_mount) fn controls_pending(&self) -> bool {
        self.directory.is_some()
    }
    pub(in crate::native_mount) fn namespace_detach_confirmed(&self) -> bool {
        false
    }
}

pub(in crate::native_mount) fn remove_empty_recovery_destination<W: NativeHostWork>(
    destination: &std::path::Path,
    expected: Option<crate::NativeRootIdentity>,
    work: &mut W,
) -> Result<(), NativeMountError>
where
    NativeMountError: From<W::Error>,
{
    let opened = match native_host::open_root_directory(destination, work) {
        Ok(opened) => opened,
        Err(error) if W::io_error_kind(&error) == Some(std::io::ErrorKind::NotFound) => {
            return Ok(());
        }
        Err(error) => return Err(error.into()),
    };
    if expected != Some(opened.1) {
        return Err(NativeMountError::Demand(crate::DemandError::StaleSource));
    }
    if let Some(tag) = reparse_tag_held(&opened.0, work)? {
        return Err(NativeMountError::UnsafeRecoveryDestination {
            reparse_tag: Some(tag),
        });
    }
    let parent = native_host::open_root_directory(
        destination
            .parent()
            .ok_or(NativeMountError::InvalidDestination)?,
        work,
    )
    .map(|(directory, _)| directory)
    .map_err(NativeMountError::from)?;
    let name = destination
        .file_name()
        .ok_or(NativeMountError::InvalidDestination)?;
    native_host::with_wide_path(std::path::Path::new(name), work, |name, work| {
        native_host::remove_windows_leaf(&parent, name, true, Some(opened.1), work)
    })
    .map_err(NativeMountError::from)?;
    drop(opened);
    match native_host::root_path_identity(destination, work) {
        Err(error) if W::io_error_kind(&error) == Some(std::io::ErrorKind::NotFound) => Ok(()),
        Err(error) => Err(error.into()),
        Ok(_) => Err(NativeMountError::ControlPending),
    }
}

pub(in crate::native_mount) fn reparse_tag_held<W: NativeHostWork>(
    file: &impl std::os::windows::io::AsRawHandle,
    work: &mut W,
) -> Result<Option<u32>, NativeMountError>
where
    NativeMountError: From<W::Error>,
{
    use windows::Win32::Foundation::ERROR_FILE_SYSTEM_VIRTUALIZATION_UNAVAILABLE;
    let result = work.attempt_io(
        crate::WorkCounters {
            backend_read_operations: 1,
            ..crate::WorkCounters::UNCHARGED
        },
        || match reparse_data(file, |data| {
            data.first_chunk().map(|tag| u32::from_le_bytes(*tag))
        }) {
            Err(error)
                if error.raw_os_error()
                    == Some(ERROR_FILE_SYSTEM_VIRTUALIZATION_UNAVAILABLE.0 as i32) =>
            {
                Ok(Some(Some(
                    windows::Win32::System::SystemServices::IO_REPARSE_TAG_PROJFS,
                )))
            }
            result => result,
        },
    );
    match result {
        Ok(None) => Ok(None),
        Ok(Some(Some(tag))) => Ok(Some(tag)),
        Ok(Some(None)) => Err(NativeMountError::Io(std::io::ErrorKind::InvalidData.into())),
        Err(error) => Err(error.into()),
    }
}
