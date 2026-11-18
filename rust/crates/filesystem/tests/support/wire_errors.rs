use acyclic_fs::{FsError, ObjectStoreError, WorkspaceError};
use tonic::Status;

pub(crate) fn fs(error: FsError) -> WorkspaceError {
    WorkspaceError::Filesystem(error)
}
pub(crate) fn object(error: ObjectStoreError) -> WorkspaceError {
    fs(FsError::Object(error))
}
pub(crate) fn converted(error: impl Into<ObjectStoreError> + std::fmt::Display) -> WorkspaceError {
    let message = error.to_string();
    let error = object(error.into());
    assert_eq!(Status::from(&error).message(), message);
    error
}
pub(crate) fn allocation() -> Result<std::collections::TryReserveError, &'static str> {
    Vec::<u8>::new()
        .try_reserve(usize::MAX)
        .err()
        .ok_or("usize::MAX cannot be a Vec capacity")
}
pub(crate) fn io(kind: std::io::ErrorKind) -> std::io::Error {
    std::io::Error::new(kind, "original cause")
}
