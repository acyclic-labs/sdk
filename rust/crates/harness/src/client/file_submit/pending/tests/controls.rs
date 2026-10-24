use super::{
    faults::Fault,
    fixture::{self, Input},
    observation::{self, Event},
};
use acyclic_test_support::differential::oracle::{Builder, Controlled, Oracle};

pub(super) fn oracle() -> Oracle<Input, Fault, Event> {
    builder().build()
}

pub(super) fn builder() -> Builder<Input, Fault, Event, Controlled> {
    let history = |actions| Input {
        actions,
        probe: None,
    };
    Oracle::declare(|input, fault: Option<&Fault>, see| {
        see.see(fixture::run(input, fault.copied()))
    })
    .invariant("admission-results", observation::admission)
    .invariant("retired-entry", observation::retirement)
    .invariant("ownership", observation::ownership)
    // Every fault is exposed at the final transition: no downstream API
    // outcome carries detection after deleting the targeted invariant.
    .control(
        "ownership",
        history(vec![Some((0, 1)), None]),
        Fault::LeakRefund,
    )
    .control(
        "ownership",
        history(vec![Some((0, 1)), None]),
        Fault::StaleIdentity,
    )
    .control(
        "ownership",
        history(vec![Some((0, 1))]),
        Fault::DoubleCharge,
    )
    .control(
        "ownership",
        history(vec![Some((2, 1)), Some((0, 2))]),
        Fault::SortByIdentity,
    )
    .control(
        "ownership",
        history(vec![Some((0, 1)), Some((0, 1))]),
        Fault::MutationOnRejection,
    )
    .control(
        "retired-entry",
        history(vec![Some((0, 1)), None]),
        Fault::WrongKey,
    )
    .control(
        "retired-entry",
        history(vec![Some((0, 1)), None]),
        Fault::WrongPayload,
    )
    .control(
        "retired-entry",
        history(vec![Some((0, 1)), None]),
        Fault::WrongCursor,
    )
    .control(
        "retired-entry",
        history(vec![Some((0, 1)), None]),
        Fault::DropPublication,
    )
    .control(
        "retired-entry",
        history(vec![Some((0, 1)), None]),
        Fault::DropChannel,
    )
    .control(
        "retired-entry",
        history(vec![Some((0, 1)), None]),
        Fault::DropRequester,
    )
    .control(
        "ownership",
        history(vec![Some((0, 1))]),
        Fault::WrongCommands,
    )
    .control(
        "ownership",
        history(vec![Some((0, 1))]),
        Fault::WrongByteLimit,
    )
    .control(
        "admission-results",
        history(vec![Some((0, 1))]),
        Fault::FalseRejection,
    )
    .control(
        "admission-results",
        history(vec![Some((0, 8)), Some((1, 8))]),
        Fault::WrongQuotaLimit,
    )
    .control(
        "admission-results",
        history(vec![Some((0, 0)), Some((1, 0)), Some((2, 0)), Some((3, 0))]),
        Fault::WrongQuotaLimit,
    )
    .control(
        "admission-results",
        history(vec![Some((0, 4)), Some((1, 4)), Some((2, 4)), Some((0, 1))]),
        Fault::WrongConflict,
    )
    .control(
        "admission-results",
        Input {
            actions: vec![Some((0, 1))],
            probe: Some(u64::MAX),
        },
        Fault::AllowOverQuota,
    )
    .control(
        "admission-results",
        history(vec![Some((0, 1))]),
        Fault::RefusePreflight,
    )
}
