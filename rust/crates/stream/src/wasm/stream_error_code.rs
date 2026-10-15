#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, Tsify)]
#[serde(rename_all = "snake_case")]
pub enum StreamErrorCode {
    InvalidPath,
    InvalidArgument,
    LimitExceeded,
    NotFound,
    AlreadyExists,
    PrefixNotRetained,
    OutOfRange,
    IdempotencyMismatch,
    Capacity,
    AccessDenied,
    Unavailable,
    HierarchyChanged,
    DeadlineElapsed,
    Unsupported,
}
