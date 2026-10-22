use crate::{
    Line, Style, code,
    style::{BLUE, CYAN, MAGENTA, MUTED, YELLOW},
};
use pulldown_cmark::{CodeBlockKind, Event, HeadingLevel, Tag, TagEnd};
use std::sync::Arc;

#[cfg(test)]
pub(crate) fn render_styled(events: &[Event<'_>], style: Style) -> Vec<Line> {
    render_styled_in(events, style, None)
}

pub(crate) fn render_styled_in(
    events: &[Event<'_>],
    style: Style,
    references: Option<&crate::References>,
) -> Vec<Line> {
    let mut out = Renderer {
        style,
        ..Renderer::default()
    };
    for event in events {
        out.event_in(event, references);
    }
    out.flush();
    out.lines
}

fn destination(url: &str, id: &str, references: Option<&crate::References>) -> Arc<str> {
    if !id.is_empty()
        && let Some((destination, _, _)) =
            references.and_then(|references| references.get(&crate::normalize(id)))
    {
        return destination.clone();
    }
    #[cfg(test)]
    crate::work::copy(url.len());
    Arc::from(url)
}

#[derive(Default)]
pub(crate) struct Renderer {
    pub(crate) lines: Vec<Line>,
    line: Line,
    styles: Vec<(Style, Option<Arc<str>>)>,
    style: Style,
    link: Option<Arc<str>>,
    new_link: bool,
    lists: Vec<Option<u64>>,
    prefixes: Vec<String>,
    code: Option<code::Code>,
    code_text: String,
    table: Option<crate::table::Table>,
    width: u16,
}

impl Renderer {
    pub(super) fn with_width(width: u16) -> Self {
        Self {
            width: width.max(1),
            ..Self::default()
        }
    }

    pub(super) fn render_width(&self) -> u16 {
        if self.width == 0 { 80 } else { self.width }
    }

    pub(super) fn inline(line: Line) -> Self {
        Self {
            line,
            ..Self::default()
        }
    }
    pub(super) fn reuse_line(&mut self, mut line: Line) {
        line.spans.clear();
        #[cfg(test)]
        crate::work::copy(self.line.spans.len() * std::mem::size_of::<crate::Span>());
        line.spans.append(&mut self.line.spans);
        self.line = line;
    }

    pub(super) fn inline_line(&self) -> &Line {
        &self.line
    }
    pub(super) fn into_inline(self) -> Line {
        self.line
    }

    pub(crate) fn checkpoint(&self) -> Self {
        #[cfg(test)]
        crate::work::copy(
            self.line
                .spans
                .iter()
                .map(|span| span.text.len())
                .sum::<usize>()
                + self.prefixes.iter().map(String::len).sum::<usize>(),
        );
        Self {
            line: self.line.clone(),
            styles: self.styles.clone(),
            style: self.style,
            link: self.link.clone(),
            new_link: self.new_link,
            lists: self.lists.clone(),
            prefixes: self.prefixes.clone(),
            width: self.width,
            ..Self::default()
        }
    }

    pub(crate) fn flush(&mut self) {
        if !self.line.spans.is_empty() {
            self.lines.push(std::mem::take(&mut self.line));
        }
    }

    fn text(&mut self, text: &str) {
        for part in text.split_inclusive(['\r', '\n']) {
            if self.line.spans.is_empty() {
                self.line.push(&self.prefix(), MUTED, None);
            }
            let text = part.trim_end_matches(['\r', '\n']);
            if self.new_link && self.link.is_some() && !text.is_empty() {
                #[cfg(test)]
                crate::work::copy(text.len());
                self.line.push_new(text, self.style, self.link.as_ref());
                self.new_link = false;
            } else {
                self.line.push(text, self.style, self.link.as_ref());
            }
            if part.ends_with(['\r', '\n']) {
                if self.line.spans.is_empty() {
                    self.lines.push(Line::default());
                } else {
                    self.flush();
                }
            }
        }
    }

    fn prefix(&self) -> String {
        self.prefixes.concat()
    }

    pub(crate) fn block_line(&mut self, line: Line) {
        if self.prefixes.is_empty() {
            self.lines.push(line);
            return;
        }
        let mut prefixed = Line::plain(&self.prefix(), MUTED);
        prefixed.spans.extend(line.spans);
        self.lines.push(prefixed);
    }

    pub(crate) fn event(&mut self, event: &Event<'_>) {
        self.event_in(event, None);
    }

    pub(crate) fn event_in(&mut self, event: &Event<'_>, references: Option<&crate::References>) {
        if let Some(table) = &mut self.table {
            if matches!(event, Event::End(TagEnd::Table)) {
                if let Some(table) = self.table.take() {
                    for line in table.render() {
                        self.block_line(line);
                    }
                }
                if let Some((style, link)) = self.styles.pop() {
                    self.style = style;
                    self.link = link;
                }
            } else {
                table.event(event, references);
            }
            return;
        }
        match event {
            Event::Start(tag) => self.start(tag, references),
            Event::End(tag) => self.end(*tag),
            Event::Text(text) => {
                if self.code.is_some() {
                    self.code_text.push_str(text);
                } else {
                    self.text(text);
                }
            }
            Event::Html(text)
            | Event::InlineHtml(text)
            | Event::InlineMath(text)
            | Event::DisplayMath(text) => self.text(text),
            Event::Code(text) => {
                let previous = self.style;
                self.style.foreground = Some(YELLOW);
                self.text(text);
                self.style = previous;
            }
            Event::SoftBreak => self.text(" "),
            Event::HardBreak => self.flush(),
            Event::Rule => {
                self.flush();
                self.block_line(Line::plain(&"─".repeat(40), MUTED));
            }
            Event::TaskListMarker(done) => {
                // Replace the bullet, retaining any nesting indentation.
                if let Some(marker) = self.line.spans.last_mut() {
                    marker.text.clear();
                }
                if let Some(indent) = self.prefixes.last_mut() {
                    *indent = "    ".into();
                }
                self.text(if *done { "[✓] " } else { "[ ] " });
            }
            Event::FootnoteReference(text) => self.text(&format!("[{text}]")),
        }
    }

    fn start(&mut self, tag: &Tag<'_>, references: Option<&crate::References>) {
        self.styles.push((self.style, self.link.clone()));
        match tag {
            Tag::Heading { level, .. } => {
                self.flush();
                self.style.foreground =
                    Some(if matches!(level, HeadingLevel::H1 | HeadingLevel::H2) {
                        CYAN
                    } else {
                        BLUE
                    });
                self.style.bold = matches!(level, HeadingLevel::H1 | HeadingLevel::H5);
                self.text(match level {
                    HeadingLevel::H1 | HeadingLevel::H2 => "# ",
                    HeadingLevel::H3 | HeadingLevel::H4 => "## ",
                    _ => "### ",
                });
            }
            Tag::Emphasis => self.style.italic = true,
            Tag::Strong => self.style.bold = true,
            Tag::Strikethrough => {
                self.style.dim = true;
                self.style.strikethrough = true;
            }
            Tag::Link { dest_url, id, .. } | Tag::Image { dest_url, id, .. } => {
                self.style.foreground = Some(BLUE);
                self.style.underline = true;
                self.new_link = true;
                self.link = Some(destination(dest_url, id, references));
            }
            Tag::BlockQuote(_) => {
                self.flush();
                self.prefixes.push("│ ".into());
                self.style.italic = true;
                self.style.dim = true;
            }
            Tag::List(start) => {
                self.flush();
                self.lists.push(*start);
            }
            Tag::Item => {
                self.flush();
                self.line.push(&self.prefix(), MUTED, None);
                let marker = match self.lists.last_mut() {
                    Some(Some(number)) => {
                        let marker = format!("{number}. ");
                        *number += 1;
                        marker
                    }
                    _ => "• ".into(),
                };
                self.line.push(
                    &marker,
                    Style {
                        foreground: Some(MAGENTA),
                        ..Style::default()
                    },
                    None,
                );
                self.prefixes.push(" ".repeat(marker.chars().count()));
            }
            Tag::CodeBlock(kind) => {
                self.flush();
                let language = match kind {
                    CodeBlockKind::Fenced(info) => {
                        info.split_whitespace().next().unwrap_or_default()
                    }
                    CodeBlockKind::Indented => "",
                };
                self.block_line(code::header(language));
                self.code = Some(code::highlighter(language));
            }
            Tag::Table(alignments) => {
                self.flush();
                let indent: usize = self
                    .prefixes
                    .iter()
                    .map(|prefix| unicode_width::UnicodeWidthStr::width(prefix.as_str()))
                    .sum();
                let width = self
                    .render_width()
                    .saturating_sub(u16::try_from(indent).unwrap_or(u16::MAX))
                    .max(1);
                self.table = Some(crate::table::Table::new(
                    alignments.clone(),
                    self.style,
                    width,
                ));
            }
            _ => {}
        }
    }

    fn end(&mut self, tag: TagEnd) {
        match tag {
            TagEnd::Paragraph | TagEnd::Heading(_) | TagEnd::HtmlBlock => self.flush(),
            TagEnd::Item | TagEnd::BlockQuote(_) => {
                self.flush();
                self.prefixes.pop();
            }
            TagEnd::List(_) => {
                self.flush();
                self.lists.pop();
            }
            TagEnd::CodeBlock => {
                if let Some(mut code) = self.code.take() {
                    let text = std::mem::take(&mut self.code_text);
                    for line in text.split_inclusive('\n') {
                        self.block_line(code::highlight(&mut code, line));
                    }
                }
                self.code_text.clear();
                self.block_line(code::footer());
            }
            _ => {}
        }
        if let Some((style, link)) = self.styles.pop() {
            self.style = style;
            self.link = link;
        }
        if matches!(tag, TagEnd::Link | TagEnd::Image) {
            self.new_link = self.link.is_some();
        }
    }
}
