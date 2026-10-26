//! Fixed-size evidence for the tip and parent of one authenticated public chain.
use super::*;

#[derive(Default)]
pub(super) struct WorkloadAuthentication {
    latest: Option<AuthenticatedPrefix>,
}

struct Endpoint {
    length: usize,
    id: BlockId,
}

struct AuthenticatedPrefix {
    tip: Endpoint,
    parent: Option<Endpoint>,
}

impl Endpoint {
    fn matches(&self, ids: &[BlockId]) -> bool {
        self.length.checked_sub(1).and_then(|index| ids.get(index)) == Some(&self.id)
    }
}

impl WorkloadAuthentication {
    pub(super) fn authenticated_through(&self, ids: &[BlockId]) -> usize {
        let Some(latest) = &self.latest else {
            return 0;
        };
        if latest.tip.matches(ids) {
            latest.tip.length
        } else {
            latest
                .parent
                .as_ref()
                .filter(|parent| parent.matches(ids))
                .map_or(0, |parent| parent.length)
        }
    }

    pub(super) fn retain(&mut self, ids: &[BlockId]) {
        self.latest = ids.last().map(|id| AuthenticatedPrefix {
            tip: Endpoint {
                length: ids.len(),
                id: *id,
            },
            parent: ids.len().checked_sub(2).and_then(|index| {
                ids.get(index).map(|id| Endpoint {
                    length: index + 1,
                    id: *id,
                })
            }),
        });
    }
}
