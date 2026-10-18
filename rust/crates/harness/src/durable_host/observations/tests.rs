use super::*;
use proptest::prelude::*;

proptest! {
    #[test]
    fn terminal_projection_preserves_failures_and_uncertainty(
        kind in 0u8..7, message in ".{0,128}", operation in any::<[u8; 16]>()
    ) {
        let id = OperationId::from_bytes(operation);
        let (input, expected) = match kind {
            0 => (Outcome::Succeeded(message), Outcome::Succeeded(())),
            1 => (Outcome::Failed { message: message.clone() }, Outcome::Failed { message }),
            2 => (Outcome::Cancelled, Outcome::Cancelled),
            3 => (
                Outcome::Indeterminate { operation_id: id, cause: crate::IndeterminateCause::Unobserved },
                Outcome::Indeterminate { operation_id: id, cause: crate::IndeterminateCause::Unobserved },
            ),
            _ => {
                let cause = crate::IndeterminateCause::Command(crate::CommandIndeterminacy {
                    command: message.clone(),
                    reason: match kind {
                        4 => crate::CommandIndeterminacyReason::Failed { code: message.clone(), error: message },
                        5 => crate::CommandIndeterminacyReason::Unsettled,
                        _ => crate::CommandIndeterminacyReason::InputLimit,
                    },
                });
                (Outcome::Indeterminate { operation_id: id, cause: cause.clone() },
                 Outcome::Indeterminate { operation_id: id, cause })
            }
        };
        prop_assert_eq!(project_terminal(&input), expected);
    }
}

#[test]
fn successful_metadata_requires_no_payload_traits() {
    struct Unreadable;
    assert_eq!(
        project_terminal(&Outcome::Succeeded(Unreadable)),
        Outcome::Succeeded(())
    );
}
