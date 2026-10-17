//! Definite input rejections are distinct from ownership and storage failures.
use crate::{Error, Outcome};
use serde_json::Value;

pub(super) type Checked<T> = std::result::Result<T, RequestRejection>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum RequestRejection {
    NotAttached,
    NativeSession,
    ChildNotAdmitted,
    ForeignChild,
    UnboundChildLabel,
    ChildBinding,
}

impl RequestRejection {
    pub(super) fn message(self) -> &'static str {
        match self {
            Self::NotAttached => "task is not an attached external session",
            Self::NativeSession => "request differs from the attached native session",
            Self::ChildNotAdmitted => "native child has not been admitted",
            Self::ForeignChild => "native stop does not own this child",
            Self::UnboundChildLabel => "native child has no committed fork binding",
            Self::ChildBinding => "native child differs from its immutable fork binding",
        }
    }

    pub(super) fn outcome(self) -> Outcome<Value> {
        Outcome::Failed {
            message: self.message().into(),
        }
    }

    pub(super) fn error(self) -> Error {
        match self {
            Self::ChildNotAdmitted | Self::UnboundChildLabel => {
                Error::NotFound(self.message().into())
            }
            _ => Error::Unauthorized(self.message().into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_input_rejection_is_a_failure_never_an_uncertain_publication() {
        for rejected in [
            RequestRejection::NotAttached,
            RequestRejection::NativeSession,
            RequestRejection::ChildNotAdmitted,
            RequestRejection::ForeignChild,
            RequestRejection::UnboundChildLabel,
            RequestRejection::ChildBinding,
        ] {
            assert!(
                matches!(rejected.outcome(),Outcome::Failed {message} if message == rejected.message())
            );
            assert!(matches!(
                rejected.error(),
                Error::Unauthorized(_) | Error::NotFound(_)
            ));
        }
    }
}
