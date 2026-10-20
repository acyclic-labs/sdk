#[cfg(test)]
use crate::Line;
use crate::line_buffer::Buffer;

// An alternate derived presentation. The finalized prefix is synchronized only
// when needed, so successive open blocks never copy the document's history.
#[derive(Default)]
pub(crate) struct Spare {
    pub(crate) lines: Buffer,
    valid: usize,
    active: bool,
}

impl Spare {
    pub(crate) fn invalidate(&mut self, first: usize) {
        #[cfg(test)]
        if KEEP_STALE_PREFIX.get() {
            return;
        }
        self.valid = self.valid.min(first);
    }

    pub(crate) fn finish(&mut self, lines: &Buffer, stable: usize, changed: usize) {
        self.invalidate(changed);
        #[cfg(test)]
        let rendering = crate::work::rendering(true);
        if self.active {
            self.prefix(lines, stable);
        }
        #[cfg(test)]
        crate::work::rendering(rendering);
    }

    fn prefix(&mut self, lines: &Buffer, end: usize) {
        let start = self.valid.min(end);
        let existing = end.min(self.lines.len());
        if start < end {
            #[cfg(test)]
            {
                crate::work::copy((end - start) * std::mem::size_of::<Line>());
            }
            for (index, original) in lines.iter().enumerate().take(existing).skip(start) {
                if let Some(row) = self.lines.get_mut(index) {
                    row.clone_from(original);
                }
            }
            self.lines
                .extend(lines.iter().skip(existing).take(end - existing).cloned());
            self.valid = end;
        }
    }

    fn synchronize(&mut self, lines: &Buffer, end: usize) {
        let start = self.valid.min(end);
        #[cfg(test)]
        {
            crate::work::copy((end - start) * std::mem::size_of::<Line>());
        }
        self.lines.truncate(start);
        self.lines
            .extend(lines.iter().skip(start).take(end - start).cloned());
        self.valid = end;
    }

    pub(crate) fn capture(&mut self, lines: &Buffer) {
        self.active = true;
        self.synchronize(lines, lines.len());
    }

    pub(crate) fn exchange(&mut self, lines: &mut Buffer, first: usize) {
        // Synchronize only the finalized prefix; the alternate open block is
        // already rendered and must retain its previous tail for diffing.
        debug_assert!(first <= self.lines.len());
        self.prefix(lines, first);
        std::mem::swap(lines, &mut self.lines);
        self.valid = first;
    }
}

#[cfg(test)]
thread_local! { pub(crate) static KEEP_STALE_PREFIX: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; pub(crate) static REPLAY: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn replacement(count: usize, first: usize, stale: bool) {
        let mut lines: Buffer = (0..count)
            .map(|_| Line::plain("old", crate::Style::default()))
            .collect();
        let mut spare = Spare::default();
        spare.capture(&lines);
        for index in first..count {
            *lines.get_mut(index).expect("existing row") =
                Line::plain("resolved", crate::Style::default());
        }
        let expected = lines.all().to_vec();
        KEEP_STALE_PREFIX.set(stale);
        spare.invalidate(first);
        KEEP_STALE_PREFIX.set(false);
        spare.exchange(&mut lines, count);
        assert_eq!(lines.all(), expected);
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]
        #[test]
        fn replacements_survive_alternate_preview_exchange(count in 1usize..32, offset in any::<usize>()) {
            replacement(count, offset % count, false);
        }
    }
    #[test]
    fn replacement_property_detects_stale_preview_prefixes() {
        assert!(std::panic::catch_unwind(|| replacement(4, 1, true)).is_err());
    }
}
