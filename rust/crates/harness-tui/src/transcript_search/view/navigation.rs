use super::*;

pub(super) struct Probe {
    range: Range<usize>,
    wrapped: bool,
}

impl TranscriptSearch {
    /// Navigate by source identity. A partial viewport never establishes a wrap.
    pub(super) fn navigate(&mut self, projection: &WrappedLines, budget: usize) {
        if self.steps == 0 || self.selected.is_none() {
            return;
        }
        if self.complete {
            if let Some(rank) = self.selected_rank {
                let count = self.matches.len() as i64;
                let rank = (rank as i64 + self.steps.rem_euclid(count)).rem_euclid(count) as usize;
                self.selected = Some(self.matches[rank].clone());
                self.selected_rank = Some(rank);
                self.steps = 0;
                self.navigation = None;
            }
            return;
        }
        let anchor = self.selected.clone().unwrap();
        if let Ok(rank) = self
            .local
            .binary_search_by_key(&(anchor.start, anchor.end), |range| {
                charge(1, false);
                (range.start, range.end)
            })
        {
            let next = (rank as i64).saturating_add(self.steps);
            if (0..self.local.len() as i64).contains(&next) {
                self.selected = Some(self.local[next as usize].clone());
                self.steps = 0;
                self.navigation = None;
                return;
            }
        }
        let backwards = self.steps < 0;
        let len = projection.index.text.len();
        let probe = self.navigation.get_or_insert_with(|| {
            let row = projection.source_rows[anchor.start];
            let folded = &projection.folded_rows[row];
            Probe {
                range: if backwards {
                    0..folded.end
                } else {
                    folded.start..len
                },
                wrapped: false,
            }
        });
        let budget = budget.max(self.needle.len()).max(1);
        let range = if backwards {
            probe
                .range
                .end
                .saturating_sub(budget)
                .max(probe.range.start)..probe.range.end
        } else {
            probe.range.start
                ..probe
                    .range
                    .end
                    .min(probe.range.start.saturating_add(budget))
        };
        let mut candidate = None;
        scan(
            projection,
            &self.needle,
            &self.prefix,
            range.clone(),
            true,
            |at| {
                let found = projection.index.source(at, self.needle.len());
                let eligible = probe.wrapped
                    || if backwards {
                        (found.start, found.end) < (anchor.start, anchor.end)
                    } else {
                        (found.start, found.end) > (anchor.start, anchor.end)
                    };
                if eligible && (candidate.is_none() || backwards) {
                    candidate = Some(found);
                }
            },
        );
        if let Some(found) = candidate {
            self.selected = Some(found);
            self.steps -= self.steps.signum();
            self.navigation = None;
        } else {
            if backwards {
                probe.range.end = range.start;
            } else {
                probe.range.start = range.end;
            }
            if probe.range.is_empty() {
                // Exhausting this direction proves the actual transcript boundary.
                probe.range = 0..len;
                probe.wrapped = true;
            }
        }
    }
}
