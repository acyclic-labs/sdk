use super::*;
use acyclic_test_support::{AllocationInfo, differential, measure};
use std::cell::Cell;

// Independent implementation from before stable token pages. It owns its own
// storage and retains the original four-to-sixteen-slot growth and reset.
#[derive(Default)]
struct PreviousQueue {
    pages: VecDeque<Vec<Token>>,
    start: usize,
    length: usize,
}

impl PreviousQueue {
    fn push(&mut self, token: Token) {
        if self
            .pages
            .back()
            .is_none_or(|page| page.len() == super::super::forest::PAGE)
        {
            if self.pages.len() == self.pages.capacity() {
                crate::work::copy(self.pages.len() * std::mem::size_of::<Vec<Token>>());
            }
            let capacity = if self.pages.is_empty() {
                4
            } else {
                super::super::forest::PAGE
            };
            self.pages.push_back(Vec::with_capacity(capacity));
        }
        let page = self.pages.back_mut().expect("previous token page");
        if page.len() == page.capacity() {
            crate::work::copy(page.len() * std::mem::size_of::<Token>());
            page.reserve_exact(super::super::forest::PAGE - page.len());
        }
        page.push(token);
        self.length += 1;
    }

    fn pop(&mut self) -> Option<Token> {
        if self.length == 0 {
            return None;
        }
        let token = self.pages.front()?.get(self.start)?.clone();
        self.start += 1;
        self.length -= 1;
        if self
            .pages
            .front()
            .is_some_and(|page| self.start == page.len())
        {
            self.pages.pop_front();
            self.start = 0;
        }
        Some(token)
    }
}

fn queue_work(old: bool) -> (Vec<Option<Token>>, AllocationInfo, usize) {
    let mut previous = PreviousQueue::default();
    let mut current = Queue::default();
    let mut values = Vec::with_capacity(600);
    let mut allocations = AllocationInfo::default();
    let before = crate::work::parts().get(1).copied().expect("copy counter");
    for index in 0..600 {
        let token = Token {
            range: index..index + 1,
            kind: Kind::Text,
        };
        allocations += measure(|| {
            if old {
                previous.push(token);
            } else {
                current.push_back(token);
            }
        });
        differential::record((0u8, index, None::<Token>));
        if (index + 1).is_multiple_of(6) {
            for _ in 0..6 {
                let token = if old {
                    previous.pop()
                } else {
                    current.pop_front()
                };
                differential::record((1u8, index, token.clone()));
                values.push(token);
            }
        }
    }
    let copied = crate::work::parts().get(1).copied().expect("copy counter") - before;
    (values, allocations, copied)
}

fn current_cost(allocations: AllocationInfo, copied: usize) {
    assert_eq!(allocations.count_total, 44);
    assert_eq!(copied, 3 * std::mem::size_of::<Vec<Token>>());
}

#[test]
fn queue_allocations_and_work_match_the_independent_previous_queue() {
    let previous_cost = Cell::new((AllocationInfo::default(), 0));
    let candidate_cost = Cell::new((AllocationInfo::default(), 0));
    let ((values, _), _) = acyclic_test_support::differential(
        || {
            differential::observe::<_, _, (u8, usize, Option<Token>)>(
                || (),
                |_| {
                    let (values, allocations, copied) = queue_work(true);
                    previous_cost.set((allocations, copied));
                    values
                },
            )
        },
        || {
            differential::observe::<_, _, (u8, usize, Option<Token>)>(
                || (),
                |_| {
                    let (values, allocations, copied) = queue_work(false);
                    candidate_cost.set((allocations, copied));
                    values
                },
            )
        },
    );
    let (allocations, copied) = candidate_cost.get();
    current_cost(allocations, copied);
    let (allocations, copied) = previous_cost.get();
    assert_eq!(allocations.count_total, 201);
    assert_eq!(copied, 100 * 4 * std::mem::size_of::<Token>());
    assert_eq!(values.len(), 600);
    for (index, token) in values.into_iter().enumerate() {
        assert_eq!(
            token,
            Some(Token {
                range: index..index + 1,
                kind: Kind::Text
            })
        );
    }
}

#[test]
fn queue_cost_assertions_detect_the_previous_reallocation_path() {
    let (_, allocations, copied) = queue_work(true);
    let broken = std::panic::catch_unwind(|| current_cost(allocations, copied));
    assert!(broken.is_err());
}
