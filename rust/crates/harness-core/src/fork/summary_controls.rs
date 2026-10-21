use super::*;

pub(super) fn oracle() -> Oracle<Case, Fault, Seen> {
    let mut builder = Oracle::declare(run)
        .invariant("size", |seen| acceptance(seen, "size"))
        .invariant("metadata", |seen| acceptance(seen, "metadata"))
        .invariant("seed-order", |seen| acceptance(seen, "seed-order"))
        .invariant("order", |seen| match seen {
            Seen::Order { actual, expected } if actual != expected => {
                Err("inherited files differ from independent input order".into())
            }
            _ => Ok(()),
        })
        .control(
            "size",
            Case::Sizes(healthy(vec![], vec![])),
            Fault::RejectAll,
        );
    for index in 0..4 {
        let mut input = Input {
            sizes: [1, 1],
            payloads: vec![1],
            references: vec![1],
            file_limit: 1,
            render_limit: 8,
        };
        match index {
            0 | 1 => input.sizes[index] = 2,
            2 => input.payloads[0] = 2,
            _ => input.references[0] = 2,
        }
        builder = builder.control("size", Case::Sizes(input), Fault::FileBudget);
    }
    for index in 0..2 {
        let mut input = Input {
            sizes: [1, 1],
            payloads: vec![],
            references: vec![],
            file_limit: 8,
            render_limit: 1,
        };
        input.sizes[index] = 2;
        builder = builder.control("size", Case::Sizes(input), Fault::RenderBudget);
    }
    for violation in VIOLATIONS {
        builder = builder.control(
            "metadata",
            Case::Metadata(healthy(vec![], vec![]), Some(violation), false),
            Fault::CheckOriginal,
        );
    }
    for fault in [
        Fault::DropContext,
        Fault::DropPayload,
        Fault::DuplicatePayload,
        Fault::ReversePayloads,
        Fault::LeakReference,
    ] {
        builder = builder.control("order", Case::Order(healthy(vec![1, 2], vec![1])), fault);
    }
    for violation in SEED_VIOLATIONS {
        builder = builder.control(
            "seed-order",
            Case::SeedOrder(healthy(vec![1, 2], vec![]), Some(violation)),
            Fault::CheckOriginal,
        );
    }
    builder.build()
}
