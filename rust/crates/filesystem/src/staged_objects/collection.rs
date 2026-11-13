//! Private candidates join the same collection cut as durable objects.

use super::*;
use crate::kernel::allocation::KernelAccounting;
use std::ops::Bound::{Excluded, Included, Unbounded};

#[derive(Clone, Copy)]
enum Stamp {
    Resident { order: u64, incarnation: u64 },
    Spilled { offset: u64, stored_length: u64 },
}

#[derive(Clone, Copy)]
struct Candidate {
    id: ObjectId,
    stamp: Stamp,
}

impl From<Candidate> for crate::Digest {
    fn from(candidate: Candidate) -> Self {
        candidate.id.digest
    }
}

impl Candidate {
    fn unchanged(self, index: &Index) -> bool {
        match (self.stamp, index.objects.get(&self.id)) {
            (
                Stamp::Resident { order, incarnation },
                Some(Staged::Resident {
                    order: current_order,
                    incarnation: current_incarnation,
                    ..
                }),
            ) => order == *current_order && incarnation == *current_incarnation,
            (
                Stamp::Spilled {
                    offset,
                    stored_length,
                },
                Some(Staged::Spilled {
                    offset: current_offset,
                    stored_length: current_length,
                    ..
                }),
            ) => offset == *current_offset && stored_length == *current_length,
            _ => false,
        }
    }
}

impl<S> StagedObjects<S> {
    /// Reclaims private entries after the shared marker and admission cut.
    /// Each page holds the index for bounded work; physical readers drain on
    /// the existing file lease before any accepted entry can be removed.
    pub(crate) async fn sweep_private(
        &self,
        collecting: &crate::collection::Collecting,
        marked: &HashSet<crate::Digest>,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::LocalGarbageCollection, crate::FsError> {
        const PAGE: usize = 256;
        let (resident_end, spilled_end) = {
            let index = self.index();
            (
                index.order.last_key_value().map(|(&key, _)| key),
                index.spilled.last_key_value().map(|(&key, _)| key),
            )
        };
        let mut accounting =
            KernelAccounting::standalone(crate::WorkBudget::UNBOUNDED, cancellation);
        let mut result = crate::LocalGarbageCollection::default();
        let mut page = Vec::with_capacity(PAGE);
        let mut resident_after = None;
        let mut spilled_after = None;
        for resident in [true, false] {
            loop {
                cancellation.check()?;
                page.clear();
                {
                    let index = self.index();
                    if resident {
                        let Some(end) = resident_end else { break };
                        for (&order, &id) in index
                            .order
                            .range((resident_after.map_or(Unbounded, Excluded), Included(end)))
                            .take(PAGE)
                        {
                            let Some(Staged::Resident {
                                order: current,
                                incarnation,
                                ..
                            }) = index.objects.get(&id)
                            else {
                                return Err(crate::FsError::Object(ObjectStoreError::Corrupt));
                            };
                            if order != *current {
                                return Err(crate::FsError::Object(ObjectStoreError::Corrupt));
                            }
                            page.push(Candidate {
                                id,
                                stamp: Stamp::Resident {
                                    order,
                                    incarnation: *incarnation,
                                },
                            });
                        }
                    } else {
                        let Some(end) = spilled_end else { break };
                        for (&(offset, id), &stored_length) in index
                            .spilled
                            .range((spilled_after.map_or(Unbounded, Excluded), Included(end)))
                            .take(PAGE)
                        {
                            page.push(Candidate {
                                id,
                                stamp: Stamp::Spilled {
                                    offset,
                                    stored_length,
                                },
                            });
                        }
                    }
                }
                let Some(last) = page.last() else { break };
                match last.stamp {
                    Stamp::Resident { order, .. } => resident_after = Some(order),
                    Stamp::Spilled { offset, .. } => spilled_after = Some((offset, last.id)),
                }
                result.examined = result.examined.saturating_add(page.len() as u64);
                page.retain(|candidate| !marked.contains(&candidate.id.digest));
                if page.is_empty() {
                    continue;
                }
                // Same order as private admission and publication: cut, file,
                // index. The file lease also stabilizes residents selected by
                // pressure relief. The filter retains the original identity stamp.
                let (_gate, filtered) = accounting.wait_for(collecting.sweepable(page)).await?;
                page = filtered;
                if page.is_empty() {
                    continue;
                }
                let _spill = accounting.wait_for(self.spill_io.write()).await?;
                {
                    let mut index = self.index_mut();
                    let mut changed = false;
                    for candidate in page.drain(..) {
                        if candidate.unchanged(&index) {
                            if !changed {
                                collecting.sweeping();
                                changed = true;
                            }
                            index.remove(candidate.id);
                            result.removed = result.removed.saturating_add(1);
                        }
                    }
                }
            }
        }
        // Reads can promote every extent without deleting an object, so this
        // terminal also handles an empty file left by accepted recovery.
        let mut spill = accounting.wait_for(self.spill_io.write()).await?;
        let retired = {
            let mut index = self.index_mut();
            if index.spilled.is_empty() {
                index.reset_spill();
                spill.take()
            } else {
                None
            }
        };
        // Native descriptor retirement does not hold the object index lock.
        drop(retired);
        Ok(result)
    }
}
