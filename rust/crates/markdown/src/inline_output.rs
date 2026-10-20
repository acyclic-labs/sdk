use super::*;

// The first and last row remain separate, so concatenation and enclosing
// styles touch a constant number of rows. Only completed middle rows enter
// the immutable row tree.
#[derive(Clone, Default)]
pub(in crate::inline_scan) struct Output {
    head: Spans,
    pub(in crate::inline_scan) middle: Rows,
    tail: Spans,
    // None is a single row; Some records whether the first break is required.
    boundary: Option<bool>,
}

impl Output {
    fn tail(&self) -> Option<&Spans> {
        self.boundary.map(|_| &self.tail)
    }

    fn required(&self) -> bool {
        self.boundary == Some(true)
    }

    #[cfg(test)]
    pub(in crate::inline_scan) fn spans(&self) -> impl Iterator<Item = &Spans> {
        std::iter::once(&self.head).chain(self.tail())
    }

    pub(in crate::inline_scan) fn suffix_after(&self, prefix: &Self) -> Option<Self> {
        #[cfg(test)]
        if SCAN_SUFFIX.get() {
            for row in self.middle.iter() {
                let _ = row.owned();
            }
        }
        let Some(start) = prefix.tail() else {
            return Some(Self {
                head: self.head.suffix_after(&prefix.head),
                middle: self.middle.clone(),
                tail: self.tail.clone(),
                boundary: self.boundary,
            });
        };
        let at = 1 + prefix.middle.len();
        let last = 1 + self.middle.len();
        if at == last {
            return Some(Self::text(self.tail()?.suffix_after(start)));
        }
        let index = at - 1;
        let (row, required) = self.middle.row(index)?;
        Some(Self {
            head: row.suffix_after(start),
            middle: self.middle.suffix(index + 1)?,
            tail: self.tail.clone(),
            #[cfg(test)]
            boundary: Some(required || FORGET_BREAK_KIND.get()),
            #[cfg(not(test))]
            boundary: Some(required),
        })
    }

    pub(in crate::inline_scan) fn visible(&self) -> usize {
        #[cfg(test)]
        if SCAN_VISIBLE.get() {
            return self
                .lines()
                .iter()
                .rposition(|line| {
                    crate::work::visibility(1);
                    !line.spans.is_empty()
                })
                .map_or(0, |index| index + 1);
        }
        if self.tail().is_some_and(|tail| !tail.is_empty()) {
            return self.len();
        }
        if let Some(index) = self.middle.last_visible() {
            return self.head_count() + index + 1;
        }
        if self.head.is_empty() { 0 } else { 1 }
    }
    fn head_count(&self) -> usize {
        usize::from(self.boundary.is_none() || self.required() || !self.head.is_empty())
    }

    fn len(&self) -> usize {
        self.head_count() + self.middle.displayed() + usize::from(self.boundary.is_some())
    }

    pub(in crate::inline_scan) fn text(head: Spans) -> Self {
        Self {
            head,
            ..Self::default()
        }
    }

    pub(in crate::inline_scan) fn newline(required: bool) -> Self {
        Self {
            boundary: Some(required),
            ..Self::default()
        }
    }

    pub(in crate::inline_scan) fn append(&mut self, other: Self) {
        let Self {
            head,
            middle,
            tail,
            boundary,
        } = other;
        if self.boundary.is_some() {
            self.tail.concat(head);
            self.tail.freeze();
            if let Some(required) = boundary {
                let row = Rows::line(std::mem::take(&mut self.tail), required);
                self.middle = std::mem::take(&mut self.middle).concat(row).concat(middle);
                self.tail = tail;
            }
        } else {
            self.head.concat(head);
            self.head.freeze();
            if boundary.is_some() {
                self.middle = middle;
                self.tail = tail;
                self.boundary = boundary;
            }
        }
    }

    pub(in crate::inline_scan) fn masked(self, style: Style, link: Option<Arc<str>>) -> Self {
        Self {
            head: self.head.masked(style, link.clone()),
            middle: self.middle.masked(style, link.clone()),
            tail: self.tail.masked(style, link),
            boundary: self.boundary,
        }
    }

    pub(in crate::inline_scan) fn lines(&self) -> Vec<Line> {
        let mut lines = Vec::new();
        if self.boundary.is_none() || self.required() || !self.head.is_empty() {
            lines.push(Line {
                spans: self.head.clone(),
            });
        }
        lines.extend(
            self.middle
                .iter()
                .filter(|row| row.displayed())
                .map(|row| row.owned()),
        );
        lines.extend(self.tail().into_iter().cloned().map(|spans| Line { spans }));
        lines
    }

    pub(in crate::inline_scan) fn same_mask(&self, previous: &Self) -> bool {
        fn mask(rows: &Rows) -> Mask<'_> {
            match rows.root.as_deref().map(|node| &node.kind) {
                Some(Kind::Mask(_, style, link)) => Mask {
                    style: *style,
                    link: link.as_ref(),
                },
                _ => Mask::default(),
            }
        }
        mask(&self.middle).same(mask(&previous.middle))
    }

    pub(in crate::inline_scan) fn update(
        &self,
        previous: &Self,
        lines: &mut crate::line_buffer::Buffer,
        first: usize,
    ) -> usize {
        if self.head_count() != previous.head_count()
            || self.boundary.is_some() != previous.boundary.is_some()
            || lines.len() != first + previous.len()
        {
            let next = self.lines();
            #[cfg(test)]
            {
                crate::work::copy(next.len() * std::mem::size_of::<Line>());
            }
            lines.truncate(first);
            lines.extend(next);
            return first;
        }
        let mut changed = first + self.len();
        let end = first + self.len();
        if end > lines.len() {
            // New rows are appended from the changed iterator below; do not
            // initialize empty rows only to overwrite them with rendered ones.
            lines.reserve(end - lines.len());
        } else {
            lines.truncate(end);
        }
        if self.head_count() != 0 && !self.head.same_snapshot(&previous.head) {
            if let Some(line) = lines.get_mut(first) {
                line.spans.clone_from(&self.head);
            }
            changed = first;
        }
        for (index, row) in self.middle.changed(&previous.middle) {
            let at = first + self.head_count() + index;
            #[cfg(test)]
            crate::work::copy(std::mem::size_of::<Line>());
            if at == lines.len() {
                lines.push(row.owned());
            } else {
                if let Some(line) = lines.get_mut(at) {
                    *line = row.owned();
                }
            }
            changed = changed.min(at);
        }
        if let (Some(next), Some(old)) = (self.tail(), previous.tail())
            && (self.len() != previous.len() || !next.same_snapshot(old))
        {
            let at = first + self.len() - 1;
            if at == lines.len() {
                #[cfg(test)]
                crate::work::copy(std::mem::size_of::<Line>());
                lines.push(Line {
                    spans: next.clone(),
                });
            } else {
                if let Some(line) = lines.get_mut(at) {
                    line.spans.clone_from(next);
                }
            }
            changed = changed.min(at);
        }
        debug_assert_eq!(lines.len(), end);
        changed
    }
}

#[cfg(test)]
pub(in crate::inline_scan) use SCAN_VISIBLE as RESCAN_VISIBLE;

#[cfg(test)]
thread_local! { pub(in crate::inline_scan) static SCAN_VISIBLE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }

#[cfg(test)]
thread_local! { pub(super) static FORGET_BREAK_KIND: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }

#[cfg(test)]
thread_local! { pub(super) static SCAN_SUFFIX: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }
