use super::{Cache, Key, Output, text_output};
use crate::inline_scan::forest::{Forest, Kind, Rendered};
use crate::{Range, Style};

pub(in crate::inline_scan) struct Projection<'a, 'b> {
    pub(in crate::inline_scan) source: &'a str,
    pub(in crate::inline_scan) visible: Range<usize>,
    pub(in crate::inline_scan) hard_breaks: &'b std::collections::HashSet<usize>,
}

impl Cache {
    fn entity(&mut self, source: &str, range: Range<usize>) -> Option<usize> {
        let parsed = source.get(range.clone())?;
        let (events, _) =
            crate::events::scoped(parsed, &crate::References::default(), crate::options(), 0);
        let slice = crate::inline_scan::Slice {
            original: source,
            parsed,
            base: range.start,
            start: self.input.origin?,
            sentinel: 0,
            boundary: 0,
            classified: true,
        };
        let mut body = 0;
        for (event, part) in events {
            if let crate::Event::Text(text) = event {
                let leaf = self.value(
                    14,
                    range.start + part.start..range.start + part.end,
                    &text,
                    &slice,
                    Style::default(),
                )?;
                body = self.join(body, leaf)?;
            }
        }
        Some(body)
    }

    fn break_value(&mut self, source: &str, range: Range<usize>, escaped: bool) -> usize {
        let key = Key::Leaf(15, range.start, range.end);
        if let Some(&(id, _)) = self.keys.get(&key) {
            return id;
        }
        let hard = escaped || crate::inline::hard_break(source, range.start) == Some(true);
        let value = if hard {
            Output::newline(false)
        } else {
            text_output(" ".into(), Style::default())
        };
        self.insert(key, value)
    }

    fn projected(&self, forest: &Forest, cursor: usize) -> Option<Rendered> {
        if cursor == 0 {
            return Some(Rendered {
                epoch: self.epoch,
                ..Rendered::default()
            });
        }
        let output = forest.node(cursor)?.output;
        (output.epoch == self.epoch).then_some(output)
    }

    pub(in crate::inline_scan) fn freeze_prefix(
        &mut self,
        forest: &mut Forest,
        root: usize,
        projection: &Projection<'_, '_>,
    ) -> Option<Rendered> {
        if !self.rendered {
            return None;
        }
        #[cfg(test)]
        let rendering = crate::work::rendering(true);
        let result = (|| {
            let old = self.resolve(forest, root, projection)?;
            let ids = [old.full, old.trimmed, old.whitespace, self.last];
            let mut saved: [Option<Output>; 4] = std::array::from_fn(|_| None);
            for (i, (&id, slot)) in ids.iter().zip(&mut saved).enumerate() {
                if id != 0 && !ids.get(..i)?.contains(&id) {
                    let (page, offset) = self.slot(id);
                    let output = self.nodes.get_mut(page)?.get_mut(offset)?;
                    *slot = Some(std::mem::take(output));
                    #[cfg(test)]
                    crate::work::scan(1);
                }
            }
            let input = std::mem::take(&mut self.input);
            let destinations = std::mem::take(&mut self.destinations);
            #[cfg(test)]
            let committed = self.committed;
            *self = Self::default();
            self.input = input;
            self.destinations = destinations;
            #[cfg(test)]
            {
                self.committed = committed;
            }
            self.store(Output::default());
            let mut compiled = [0; 4];
            for (i, (&id, saved)) in ids.iter().zip(&mut saved).enumerate() {
                let next = if id == 0 {
                    0
                } else if let Some(previous) = ids.get(..i)?.iter().position(|old| *old == id) {
                    *compiled.get(previous)?
                } else {
                    self.store(saved.take()?)
                };
                *compiled.get_mut(i)? = next;
            }
            let [full, trimmed, whitespace, last] = compiled;
            self.last = last;
            self.rendered = true;
            Some(Rendered {
                epoch: self.epoch,
                full,
                trimmed,
                whitespace,
                escape: None,
            })
        })();
        #[cfg(test)]
        crate::work::rendering(rendering);
        result
    }

    fn suffix(&mut self, body: usize, prefix: usize) -> Option<usize> {
        if prefix == 0 {
            return Some(body);
        }
        let key = Key::Suffix(body, prefix);
        if let Some(&(id, _)) = self.keys.get(&key) {
            return Some(id);
        }
        let output = self.output(body)?.suffix_after(self.output(prefix)?)?;
        Some(self.insert(key, output))
    }

    #[cfg(test)]
    pub(in crate::inline_scan) fn forest(
        &mut self,
        forest: &mut Forest,
        root: usize,
        source: &str,
        visible: Range<usize>,
        lines: &mut crate::line_buffer::Buffer,
        first: usize,
    ) -> Option<(usize, usize)> {
        let hard_breaks = pulldown_cmark::Parser::new_ext(source, crate::options())
            .into_offset_iter()
            .filter_map(|(event, range)| {
                matches!(event, crate::Event::HardBreak).then_some(range.start)
            })
            .collect();
        self.project(
            forest,
            root,
            &Projection {
                source,
                visible,
                hard_breaks: &hard_breaks,
            },
            lines,
            first,
            None,
        )
    }

    pub(in crate::inline_scan) fn project(
        &mut self,
        forest: &mut Forest,
        root: usize,
        projection: &Projection<'_, '_>,
        lines: &mut crate::line_buffer::Buffer,
        first: usize,
        spare: Option<&mut crate::inline_views::Spare>,
    ) -> Option<(usize, usize)> {
        let Rendered {
            trimmed: mut current,
            escape,
            ..
        } = self.resolve(forest, root, projection)?;
        if let Some((literal, start)) = escape
            && !projection.hard_breaks.contains(&start)
        {
            current = literal;
        }
        #[cfg(test)]
        let spare = spare.filter(|_| !crate::inline_views::REPLAY.get());
        let mut exchanged = false;
        if let Some(spare) = spare {
            if self.alternate.is_some_and(|(_, start)| start != first) {
                self.alternate = None;
            }
            if self.rendered && !self.output(current)?.same_mask(self.output(self.last)?) {
                if let Some((alternate, _)) = self.alternate
                    && self.output(current)?.same_mask(self.output(alternate)?)
                {
                    spare.exchange(lines, first);
                    self.alternate = Some((self.last, first));
                    self.last = alternate;
                    exchanged = true;
                } else {
                    spare.capture(lines);
                    self.alternate = Some((self.last, first));
                }
            }
        }
        let output = self.output(current)?;
        let changed = output.update(self.output(self.last)?, lines, first);
        let changed = if exchanged { first } else { changed };
        let visible = output.visible();
        self.last = current;
        self.rendered = true;
        Some((changed, visible))
    }

    fn code_output(
        &mut self,
        source: &str,
        range: Range<usize>,
        body: Range<usize>,
    ) -> Option<usize> {
        Some({
            let key = Key::Leaf(1, range.start, range.end);
            if let Some(&(id, _)) = self.keys.get(&key) {
                id
            } else {
                let output = text_output(
                    self.input.code(body, source)?,
                    Style {
                        foreground: Some(crate::style::YELLOW),
                        ..Style::default()
                    },
                );
                self.insert(key, output)
            }
        })
    }

    fn raw_output(&mut self, source: &str, range: Range<usize>) -> Option<usize> {
        let key = Key::Leaf(6, range.start, range.end);
        if let Some(&(id, _)) = self.keys.get(&key) {
            Some(id)
        } else {
            let output = self.raw(source, range, Style::default())?;
            Some(self.insert(key, output))
        }
    }

    fn compile_node(
        &mut self,
        forest: &mut Forest,
        cursor: usize,
        projection: &Projection<'_, '_>,
    ) -> Option<()> {
        let source = projection.source;
        let visible = &projection.visible;
        let node = forest.node(cursor)?;
        let (previous, children, stop, kind, range) = (
            node.previous,
            node.children,
            node.stop,
            node.kind,
            node.range.clone(),
        );
        #[cfg(test)]
        crate::work::scan(1);
        let mut compiled = self.projected(forest, previous)?;
        let content = match kind {
            Kind::Text | Kind::Escape => {
                let range = if kind == Kind::Text {
                    self.input.literal_range(range)?
                } else {
                    range
                };
                let left = range.start + usize::from(kind == Kind::Escape);
                let full = self.raw_output(source, left..range.end)?;
                let end = range.end.min(visible.end);
                let right = if range.start < end {
                    self.input.trim_end(range.start..end)?
                } else {
                    end
                };
                let left = if right < range.end { range.start } else { left };
                if range.start < range.end && right == range.start {
                    compiled.trimmed = compiled.whitespace;
                }
                if left < right {
                    let visible = self.raw_output(source, left..right)?;
                    compiled.trimmed = self.join(compiled.full, visible)?;
                    compiled.whitespace = compiled.trimmed;
                    compiled.escape = None;
                }
                full
            }
            Kind::Code => self.code_output(source, range, children..stop)?,
            Kind::Atom | Kind::Prefix => stop,
            Kind::Entity => self.entity(source, range)?,
            Kind::Break(escaped) => {
                let body = self.break_value(source, range.clone(), escaped);
                if escaped {
                    let slash = self.raw_output(source, range.start..range.start + 1)?;
                    compiled.trimmed = self.join(compiled.full, slash)?;
                    compiled.escape = Some((compiled.trimmed, range.start));
                } else {
                    compiled.full = compiled.trimmed;
                }
                body
            }
            _ => {
                let children = self.projected(forest, children)?.full;
                let stop = self.projected(forest, stop)?.full;
                let body = self.suffix(children, stop)?;
                self.mask(
                    body,
                    Style {
                        italic: matches!(kind, Kind::Emphasis | Kind::StrongEmphasis),
                        bold: matches!(kind, Kind::Strong | Kind::StrongEmphasis),
                        dim: kind == Kind::Strike,
                        strikethrough: kind == Kind::Strike,
                        ..Style::default()
                    },
                    None,
                )?
            }
        };
        let full = self.join(compiled.full, content)?;
        if !matches!(kind, Kind::Text | Kind::Escape | Kind::Break(_)) {
            compiled.trimmed = full;
            compiled.whitespace = full;
            compiled.escape = None;
        }
        if matches!(kind, Kind::Break(_)) {
            compiled.whitespace = full;
        }
        compiled.epoch = self.epoch;
        compiled.full = full;
        forest.node_mut(cursor)?.output = compiled;
        Some(())
    }

    pub(super) fn resolve(
        &mut self,
        forest: &mut Forest,
        root: usize,
        projection: &Projection<'_, '_>,
    ) -> Option<Rendered> {
        let source = projection.source;
        let visible = &projection.visible;
        #[cfg(test)]
        if super::REPLAY.get() {
            let epoch = self.epoch.checked_add(1)?;
            self.input = super::Input::default();
            self.epoch = epoch;
        }
        self.input.feed(source, visible.start);
        if self.nodes.is_empty() {
            self.store(Output::default());
        }
        let mut pending = vec![root];
        while let Some(cursor) = pending.pop() {
            if self.projected(forest, cursor).is_some() {
                continue;
            }
            let node = forest.node(cursor)?;
            let (previous, children, stop, kind) =
                (node.previous, node.children, node.stop, node.kind);
            let scoped = matches!(
                kind,
                Kind::Emphasis | Kind::Strong | Kind::StrongEmphasis | Kind::Strike
            );
            let dependencies = [
                previous,
                if scoped { children } else { 0 },
                if scoped { stop } else { 0 },
            ];
            if dependencies
                .iter()
                .any(|&id| self.projected(forest, id).is_none())
            {
                pending.push(cursor);
                pending.extend(
                    dependencies
                        .into_iter()
                        .filter(|&id| self.projected(forest, id).is_none()),
                );
                continue;
            }
            self.compile_node(forest, cursor, projection)?;
        }
        self.projected(forest, root)
    }
}

#[cfg(test)]
#[path = "../tests/inline_projection.rs"]
mod tests;
