use crate::ItemKind;

/// The only possible source of a rendered item. This decision reads fixed
/// metadata only: no payload, history, allocation, IO or wait is involved.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Selection {
    Native,
    Foreign,
    Drop,
}

pub(crate) fn select(kind: ItemKind, same_family: bool, same_provider: bool) -> Selection {
    match kind {
        ItemKind::Reasoning if !same_family || !same_provider => Selection::Drop,
        _ if same_family => Selection::Native,
        _ => Selection::Foreign,
    }
}

#[cfg(kani)]
mod proofs {
    use super::*;

    // Proves the production decision for every provenance relation, including
    // independent providers speaking the same protocol. Payload identity is
    // preserved by seg returning the original borrowed RawValue; JSON ingress
    // and byte serialization are separate obligations, not covered here.
    #[kani::proof]
    fn reasoning_never_selects_a_foreign_payload() {
        let same_family: bool = kani::any();
        let same_provider: bool = kani::any();
        let selection = select(ItemKind::Reasoning, same_family, same_provider);
        assert_ne!(selection, Selection::Foreign);
        assert_eq!(selection == Selection::Native, same_family && same_provider);
        kani::cover!(selection == Selection::Native);
        kani::cover!(!same_provider && same_family && selection == Selection::Drop);
        kani::cover!(same_provider && !same_family && selection == Selection::Drop);
    }

    #[kani::proof]
    fn ordinary_items_are_never_silently_dropped() {
        let same_family: bool = kani::any();
        let same_provider: bool = kani::any();
        let selection = select(ItemKind::Message, same_family, same_provider);
        assert_ne!(selection, Selection::Drop);
        assert_eq!(selection == Selection::Native, same_family);
        assert_eq!(selection == Selection::Foreign, !same_family);
        kani::cover!(selection == Selection::Native && !same_provider);
        kani::cover!(selection == Selection::Foreign);
    }
}
