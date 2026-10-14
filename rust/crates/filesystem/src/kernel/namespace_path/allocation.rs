use super::*;

pub(crate) fn finish_owned<T>(
    owned: super::super::file_read::OriginalReadAccounting<'_, '_>,
    result: Result<T, crate::FsError>,
) -> Result<(T, KernelOwnedAllocation), crate::FsError> {
    match result {
        Ok(value) => Ok((value, owned.into_allocation())),
        Err(error) => {
            owned.release_failed()?;
            Err(error)
        }
    }
}
