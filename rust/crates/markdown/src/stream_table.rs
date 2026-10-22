use super::*;
use table::{Table, border};

pub(super) struct Stream {
    parsed: usize,
    header: String,
    table: Table,
    widths: Vec<usize>,
    displayed: Vec<usize>,
    columns: usize,
    missing: usize,
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
        let Some(table) =
            parsed_table(source, &events, &self.references, self.state.render_width())
        else {
            return;
        };
        let Some((Event::Start(Tag::Table(logical)), _)) = events.first() else {
            return;
        };
        let columns = logical.len();
        let alignments = if table.alignments.is_empty() {
            &[pulldown_cmark::Alignment::None][..]
        } else {
            &table.alignments
        };
        let mut header = String::from("|");
        for _ in alignments {
            header.push_str("x|");
        }
        header.push_str("\n|");
        for alignment in alignments {
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
            columns,
            missing: completions(source, &events, columns, source.len()).0,
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
        let complete = tail.rfind('\n').map_or(0, |index| index + 1);
        let (missing, committed) = completions(
            &source,
            &events,
            stream.columns,
            stream.header.len() + complete,
        );
        // Match the pinned parser's per-table cap across incremental row batches.
        if stream.missing.saturating_add(missing) > 1 << 18 {
            return None;
        }
        let mut addition = parsed_table(
            &source,
            &events,
            &self.references,
            self.state.render_width(),
        )?;
        let stream = self.table.as_mut()?;
        let widths = stream.widths_for(&addition);
        let old_rows = stream.table.rows.len();
        let table_start = self.stable_lines;
        let changed = if widths == stream.displayed {
            // top border, header, separator, then previously committed body rows.
            let first = table_start + old_rows + 2;
            self.lines.truncate(first);
            first
        } else {
            // A width change invalidates the rendered rows, never their parsing.
            self.lines.truncate(table_start);
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
            table_start
        };
        let rows = addition.render_rows(1, &widths);
        #[cfg(test)]
        {
            work::copy((rows.len() + 2) * std::mem::size_of::<Line>());
            work::grow((rows.len() + 2) * std::mem::size_of::<Line>());
        }
        self.lines.extend(rows);
        self.lines.push(border('└', '┴', '┘', &widths));
        self.lines.push(Line::default());
        // The final unterminated physical row is provisional and is reparsed alone.
        let pending = usize::from(complete < tail.len());
        let keep = addition.rows.len().saturating_sub(pending);
        addition.rows.truncate(keep);
        stream.append_rows(addition.rows.drain(1..));
        (stream.cost, stream.valid) = references::extend_expansion(
            stream.cost,
            &stream.valid,
            calls
                .iter()
                .filter(|(end, _)| *end <= stream.header.len() + complete),
        );
        stream.missing += committed;
        stream.parsed += complete;
        stream.displayed = widths;
        self.finish_visible();
        Some(changed)
    }
}

impl Stream {
    fn widths_for(&self, addition: &Table) -> Vec<usize> {
        #[cfg(test)]
        work::copy(self.widths.len() * std::mem::size_of::<usize>());
        let mut widths = self.widths.clone();
        for row in addition.rows.iter().skip(1) {
            update_widths(&self.table, &mut widths, row);
        }
        widths
    }

    fn append_rows(&mut self, rows: impl Iterator<Item = Vec<Line>>) {
        for row in rows {
            update_widths(&self.table, &mut self.widths, &row);
            #[cfg(test)]
            work::grow(std::mem::size_of::<Vec<Line>>());
            self.table.rows.push(row);
        }
    }
}

fn update_widths(table: &Table, widths: &mut [usize], row: &[Line]) {
    for (column, cell) in row.iter().enumerate() {
        if let Some(size) = widths.get_mut(column) {
            *size = (*size).max(table.cell_width(cell));
        }
    }
}

fn completions(
    source: &str,
    events: &[(Event<'_>, Range<usize>)],
    columns: usize,
    complete: usize,
) -> (usize, usize) {
    let (mut total, mut committed) = (0usize, 0usize);
    for (event, range) in events {
        #[cfg(test)]
        work::scan(1);
        if matches!(event, Event::Start(Tag::TableRow)) {
            let missing =
                columns.saturating_sub(row_cells(source.get(range.clone()).unwrap_or_default()));
            total = total.saturating_add(missing);
            if range.end <= complete {
                committed = committed.saturating_add(missing);
            }
        }
    }
    (total, committed)
}

fn row_cells(row: &str) -> usize {
    #[cfg(test)]
    work::scan(row.len() * 3);
    let row = row.trim_matches([' ', '\t', '\n', '\r', '\u{b}', '\u{c}']);
    if row.is_empty() {
        return 0;
    }
    let (mut pipes, mut previous, mut trailing) = (0usize, 0u8, false);
    for byte in row.bytes() {
        // GFM strips escaped pipes before inline parsing, including \\|.
        trailing = byte == b'|' && previous != b'\\';
        pipes += usize::from(trailing);
        previous = byte;
    }
    (pipes + 1).saturating_sub(usize::from(row.starts_with('|')) + usize::from(trailing))
}

fn parsed_table(
    source: &str,
    events: &[(Event<'_>, Range<usize>)],
    references: &References,
    render_width: u16,
) -> Option<Table> {
    let Some((Event::Start(Tag::Table(alignments)), start)) = events.first() else {
        return None;
    };
    if !source
        .get(..start.start)
        .is_some_and(|prefix| prefix.bytes().all(|byte| matches!(byte, b' ' | b'\t')))
        || !events.last().is_some_and(|(event, range)| {
            matches!(event, Event::End(TagEnd::Table)) && range.end == source.len()
        })
    {
        return None;
    }
    let mut table = Table::new(alignments.clone(), Style::default(), render_width);
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

#[cfg(test)]
#[path = "../tests/table_shape.rs"]
mod tests;
