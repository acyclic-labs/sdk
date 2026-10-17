//! Pure query rejections settle; uncertainty and native accounting propagate.
use super::*;
use crate::filesystem::PreparationIo;

pub(super) fn failure(error: Error, io: Option<&PreparationIo>) -> Result<TaskCommandProgress> {
    match error {
        Error::NotFound(message) | Error::Unauthorized(message) => {
            ready(Outcome::<Value>::Failed { message }, io)
        }
        other => Err(other),
    }
}

#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(48))]
        // Only absent/denied read selections can become terminal failures.
        // Unknown commits, fencing and original-ledger errors keep their variant.
        #[test]
        fn only_known_query_rejections_become_terminal_outcomes(
            message in ".{0,128}", choice in 0u8..7, id in any::<[u8;16]>(),
        ) {
            let operation = OperationId::from_bytes(id);
            let error = match choice {
                0 => Error::NotFound(message.clone()),
                1 => Error::Unauthorized(message.clone()),
                2 => Error::Conflict(message.clone()),
                3 => Error::Storage(message.clone()),
                4 => Error::Indeterminate(operation),
                5 => Error::NativeBoundary {code:crate::wire::ErrorCode::Unauthorized,reason:"original work"},
                _ => Error::Invalid(message.clone()),
            };
            match (choice,failure(error,None)) {
                (0|1,Ok(TaskCommandProgress::Ready(value))) => {
                    let outcome:Outcome<Value> = serde_json::from_value(value).unwrap();
                    prop_assert!(matches!(outcome,Outcome::Failed {message:ref actual} if actual == &message),
                        "a known query rejection must retain its message");
                }
                (2,Err(Error::Conflict(actual))) | (3,Err(Error::Storage(actual)))
                    | (6,Err(Error::Invalid(actual))) => prop_assert_eq!(actual,message),
                (4,Err(Error::Indeterminate(actual))) => prop_assert_eq!(actual,operation),
                (5,Err(Error::NativeBoundary {code,reason})) => {
                    prop_assert_eq!(code,crate::wire::ErrorCode::Unauthorized);
                    prop_assert_eq!(reason,"original work");
                }
                (_,other) => prop_assert!(false,"unexpected query classification: {}",other.is_ok()),
            }
        }
    }
}
