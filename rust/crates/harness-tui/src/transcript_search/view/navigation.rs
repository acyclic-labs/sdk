use super::*;

pub(super) struct Probe {
    range: Range<usize>,
    wrapped: bool,
}

impl TranscriptSearch {
    /// Navigate by source identity. A partial viewport never establishes a wrap.
    pub(super) fn navigate(&mut self, projection: TranscriptRows<'_>, budget: usize) {
        if self.steps == 0 || self.selected.is_none() {
            return;
        }
        if self.complete {
            if let Some(rank) = self.selected_rank {
                let rank = cyclic_rank(rank, self.steps, self.matches.len());
                self.selected = self.matches.get(rank).cloned();
                self.selected_rank = Some(rank);
                self.steps = 0;
                self.navigation = None;
            }
            return;
        }
        let Some(anchor) = self.selected.clone() else {
            return;
        };
        if let Ok(rank) = self
            .local
            .binary_search_by_key(&(anchor.start, anchor.end), |range| {
                charge(1, false);
                (range.start, range.end)
            })
        {
            let next = i128::try_from(rank).unwrap_or(i128::MAX) + i128::from(self.steps);
            if let Ok(next) = usize::try_from(next)
                && let Some(selected) = self.local.get(next)
            {
                self.selected = Some(selected.clone());
                self.steps = 0;
                self.navigation = None;
                return;
            }
        }
        let backwards = self.steps < 0;
        let len = projection.folded_len();
        let Some(folded) = projection
            .source_row(anchor.start)
            .and_then(|row| projection.folded_row(row))
        else {
            return;
        };
        let probe = self.navigation.get_or_insert(Probe {
            range: if backwards {
                0..folded.end
            } else {
                folded.start..len
            },
            wrapped: false,
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
                let found = projection.source(at, self.needle.len());
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
