use super::*;
use table::{Table, border, width};

pub(super) struct Stream {
    parsed: usize,
    header: String,
    table: Table,
    widths: Vec<usize>,
    displayed: Vec<usize>,
    first: usize,
    pub(super) cost: usize,
    pub(super) valid: Range<usize>,
}

impl Markdown {
    pub(super) fn begin_table(
        &mut self,
        source: &str,
        events: &[(Event<'_>, Range<usize>)],
        base: usize,
    ) {
        if self.offset != base || !self.context.is_empty() || !source.ends_with('\n') {
            return;
        }
        let budget = self
            .source
            .len()
            .max(100_000)
            .saturating_sub(self.reference_cost);
        if !matches!(events.first(), Some((Event::Start(Tag::Table(_)), _))) {
            return;
        }
        let (events, calls) = events::scoped(source, &self.references, options(), budget);
        let Some(table) = parsed_table(source, &events, &self.references) else {
            return;
        };
        let mut header = String::from("|");
        for _ in &table.alignments {
            header.push_str("x|");
        }
        header.push_str("\n|");
        for alignment in &table.alignments {
            header.push_str(match alignment {
                pulldown_cmark::Alignment::Left => ":---|",
                pulldown_cmark::Alignment::Center => ":---:|",
                pulldown_cmark::Alignment::Right => "---:|",
                pulldown_cmark::Alignment::None => "---|",
            });
        }
        header.push('\n');
        let widths = table.widths();
        let (cost, valid) = references::expansion(calls.iter());
        #[cfg(test)]
        work::copy(header.len() + widths.len() * std::mem::size_of::<usize>() * 2);
        self.table = Some(Stream {
            parsed: self.source.len(),
            header,
            table,
            displayed: widths.clone(),
            widths,
            first: self.stable_lines,
            cost,
            valid,
        });
    }

    pub(super) fn advance_table(&mut self) -> Option<usize> {
        let stream = self.table.as_ref()?;
        let budget = self
            .source
            .len()
            .max(100_000)
            .saturating_sub(self.reference_cost);
        if !stream.valid.contains(&budget) {
            return None;
        }
        let tail = self.source.get(stream.parsed..)?;
        // Empty lines and block interrupts close the table through normal framing.
        if tail.lines().any(|line| line.trim().is_empty()) {
            return None;
        }
        let source = format!("{}{tail}", stream.header);
        #[cfg(test)]
        work::copy(source.len());
        let (events, calls) = events::scoped(
            &source,
            &self.references,
            options(),
            budget.saturating_sub(stream.cost),
        );
        let mut addition = parsed_table(&source, &events, &self.references)?;
        let stream = self.table.as_mut()?;
        #[cfg(test)]
        work::copy(stream.widths.len() * std::mem::size_of::<usize>());
        let mut widths = stream.widths.clone();
        for row in addition.rows.iter().skip(1) {
            for (column, cell) in row.iter().enumerate() {
                if let Some(size) = widths.get_mut(column) {
                    *size = (*size).max(width(cell));
                }
            }
        }
        let old_rows = stream.table.rows.len();
        let changed = if widths == stream.displayed {
            // top border, header, separator, then previously committed body rows.
            let first = stream.first + old_rows + 2;
            self.lines.truncate(first);
            first
        } else {
            // A width change invalidates the rendered rows, never their parsing.
            self.lines.truncate(stream.first);
            self.lines.push(border('┌', '┬', '┐', &widths));
            self.lines.extend(
                stream
                    .table
                    .render_rows(0, &widths)
                    .into_iter()
                    .enumerate()
                    .flat_map(|(index, line)| {
                        if index == 0 {
                            vec![line, border('├', '┼', '┤', &widths)]
                        } else {
                            vec![line]
                        }
                    }),
            );
            stream.first
        };
        let rows = addition.render_rows(1, &widths);
        #[cfg(test)]
        {
            work::copy((rows.len() + 2) * std::mem::size_of::<Line>());
            if self.lines.len() + rows.len() + 2 > self.lines.capacity() {
                work::copy(self.lines.len() * std::mem::size_of::<Line>());
            }
        }
        self.lines.extend(rows);
        self.lines.push(border('└', '┴', '┘', &widths));
        self.lines.push(Line::default());
        // The final unterminated physical row is provisional and is reparsed alone.
        let complete = tail.rfind('\n').map_or(0, |index| index + 1);
        let pending = usize::from(complete < tail.len());
        let keep = addition.rows.len().saturating_sub(pending);
        addition.rows.truncate(keep);
        for row in addition.rows.drain(1..) {
            for (column, cell) in row.iter().enumerate() {
                if let Some(size) = stream.widths.get_mut(column) {
                    *size = (*size).max(width(cell));
                }
            }
            #[cfg(test)]
            if stream.table.rows.len() == stream.table.rows.capacity() {
                work::copy(stream.table.rows.len() * std::mem::size_of::<Vec<Line>>());
            }
            stream.table.rows.push(row);
        }
        (stream.cost, stream.valid) = references::extend_expansion(
            stream.cost,
            &stream.valid,
            calls
                .iter()
                .filter(|(end, _)| *end <= stream.header.len() + complete),
        );
        stream.parsed += complete;
        stream.displayed = widths;
        self.finish_visible();
        Some(changed)
    }
}

fn parsed_table(
    source: &str,
    events: &[(Event<'_>, Range<usize>)],
    references: &References,
) -> Option<Table> {
    let Some((Event::Start(Tag::Table(alignments)), start)) = events.first() else {
        return None;
    };
    if start.start != 0
        || !events.last().is_some_and(|(event, range)| {
            matches!(event, Event::End(TagEnd::Table)) && range.end == source.len()
        })
    {
        return None;
    }
    let mut table = Table::new(alignments.clone(), Style::default());
    for (event, _) in events.iter().skip(1).take(events.len().saturating_sub(2)) {
        table.event(event, Some(references));
    }
    Some(table)
}

#[cfg(test)]
impl Stream {
    pub(super) fn retained(&self, targets: &mut HashSet<*const u8>) -> (usize, usize) {
        let mut payload = self.header.capacity();
        let mut metadata = (self.widths.capacity() + self.displayed.capacity())
            * std::mem::size_of::<usize>()
            + self.table.alignments.capacity() * std::mem::size_of::<pulldown_cmark::Alignment>()
            + self.table.rows.capacity() * std::mem::size_of::<Vec<Line>>();
        for row in &self.table.rows {
            metadata += row.capacity() * std::mem::size_of::<Line>();
            for cell in row {
                metadata += cell.spans.capacity() * std::mem::size_of::<Span>();
                for span in &cell.spans {
                    payload += span.text.capacity();
                    if let Some(target) = &span.link
                        && targets.insert(std::sync::Arc::as_ptr(target).cast::<u8>())
                    {
                        payload += target.len();
                    }
                }
            }
        }
        (payload, metadata)
    }
}
