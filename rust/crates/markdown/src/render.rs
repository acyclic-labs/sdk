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

fn edit<T: Clone>(values: &mut Arc<Vec<T>>) -> &mut Vec<T> {
    #[cfg(test)]
    if Arc::strong_count(values) != 1 {
        crate::work::copy(values.len() * std::mem::size_of::<T>());
    }
    Arc::make_mut(values)
}

fn pop<T: Clone>(values: &mut Option<Arc<Vec<T>>>) -> Option<T> {
    let value = edit(values.as_mut()?).pop();
    if values.as_ref().is_some_and(|values| values.is_empty()) {
        *values = None;
    }
    value
}

fn discard<T: Clone>(values: &mut Option<Arc<Vec<T>>>) {
    if values.as_ref().is_some_and(|values| values.len() <= 1) {
        *values = None;
    } else {
        pop(values);
    }
}

fn styled(tag: TagEnd) -> bool {
    matches!(
        tag,
        TagEnd::Heading(_)
            | TagEnd::BlockQuote(_)
            | TagEnd::Emphasis
            | TagEnd::Strong
            | TagEnd::Strikethrough
            | TagEnd::Link
            | TagEnd::Image
    )
}

#[derive(Default)]
pub(crate) struct Renderer {
    pub(crate) lines: Vec<Line>,
    line: Line,
    context: Option<Arc<Context>>,
    style: Style,
    link: Option<Arc<str>>,
    new_link: bool,
    code: Option<code::Code>,
    code_text: String,
    table: Option<crate::table::Table>,
    width: u16,
}

type StyleFrame = (Style, Option<Arc<str>>);

#[derive(Clone, Default)]
struct Context {
    styles: Option<Arc<Vec<StyleFrame>>>,
    lists: Option<Arc<Vec<Option<u64>>>>,
    prefixes: Option<Arc<Vec<Arc<str>>>>,
}

impl Renderer {
    fn context(&self) -> &Context {
        const EMPTY: Context = Context {
            styles: None,
            lists: None,
            prefixes: None,
        };
        self.context.as_deref().unwrap_or(&EMPTY)
    }

    fn edit_context(&mut self) -> &mut Context {
        let context = self.context.get_or_insert_with(Arc::default);
        #[cfg(test)]
        if Arc::strong_count(context) != 1 {
            crate::work::copy(std::mem::size_of::<Context>());
        }
        Arc::make_mut(context)
    }

    fn checkpoint_context(&self) -> Option<Arc<Context>> {
        #[cfg(test)]
        if CLONE_CONTEXT.get() {
            fn duplicate<T: Clone>(values: &Option<Arc<Vec<T>>>) -> Option<Arc<Vec<T>>> {
                values.as_ref().map(|values| {
                    crate::work::copy(values.len() * std::mem::size_of::<T>());
                    Arc::new(values.as_ref().clone())
                })
            }
            return self.context.as_ref().map(|context| {
                Arc::new(Context {
                    styles: duplicate(&context.styles),
                    lists: duplicate(&context.lists),
                    prefixes: duplicate(&context.prefixes),
                })
            });
        }
        self.context.clone()
    }
    pub(super) fn with_width(width: u16) -> Self {
        Self {
            width: width.max(1),
            ..Self::default()
        }
    }

    pub(super) fn render_width(&self) -> u16 {
        if self.width == 0 { 80 } else { self.width }
    }

    #[cfg(test)]
    pub(super) fn inline(line: Line) -> Self {
        Self {
            line,
            ..Self::default()
        }
    }
    pub(super) fn reuse_line(&mut self, mut line: Line) {
        line.spans.clear();
        line.spans = std::mem::take(&mut self.line.spans);
        self.line = line;
    }

    #[cfg(test)]
    pub(super) fn into_inline(self) -> Line {
        self.line
    }

    pub(crate) fn checkpoint(&self) -> Self {
        #[cfg(test)]
        crate::work::copy(std::mem::size_of::<Option<Arc<Context>>>());
        Self {
            line: self.line.clone(),
            context: self.checkpoint_context(),
            style: self.style,
            link: self.link.clone(),
            new_link: self.new_link,
            width: self.width,
            ..Self::default()
        }
    }

    #[cfg(test)]
    pub(crate) fn text_retained(
        &self,
        nodes: &mut std::collections::HashSet<usize>,
        texts: &mut std::collections::HashSet<usize>,
        links: &mut std::collections::HashSet<*const u8>,
    ) -> (usize, usize) {
        let (mut payload, mut metadata) = (0, self.lines.capacity() * std::mem::size_of::<Line>());
        for line in std::iter::once(&self.line).chain(&self.lines) {
            let (bytes, size) = line.spans.retained(nodes, texts, links);
            payload += bytes;
            metadata += size;
        }
        if let Some(context) = &self.context
            && nodes.insert(Arc::as_ptr(context) as usize)
        {
            metadata += std::mem::size_of::<Context>();
        }
        for (id, bytes) in self
            .context()
            .styles
            .iter()
            .map(|values| {
                (
                    Arc::as_ptr(values) as usize,
                    std::mem::size_of::<Vec<(Style, Option<Arc<str>>)>>()
                        + values.capacity() * std::mem::size_of::<(Style, Option<Arc<str>>)>(),
                )
            })
            .chain(self.context().lists.iter().map(|values| {
                (
                    Arc::as_ptr(values) as usize,
                    std::mem::size_of::<Vec<Option<u64>>>()
                        + values.capacity() * std::mem::size_of::<Option<u64>>(),
                )
            }))
            .chain(self.context().prefixes.iter().map(|values| {
                (
                    Arc::as_ptr(values) as usize,
                    std::mem::size_of::<Vec<Arc<str>>>()
                        + values.capacity() * std::mem::size_of::<Arc<str>>(),
                )
            }))
        {
            if nodes.insert(id) {
                metadata += bytes;
            }
        }
        for value in self
            .context()
            .styles
            .iter()
            .flat_map(|values| values.iter())
            .filter_map(|(_, link)| link.as_ref())
            .chain(self.link.iter())
            .chain(
                self.context()
                    .prefixes
                    .iter()
                    .flat_map(|values| values.iter()),
            )
        {
            if links.insert(Arc::as_ptr(value).cast::<u8>()) {
                payload += value.len();
            }
        }
        (payload, metadata)
    }

    pub(crate) fn flush(&mut self) {
        if !self.line.spans.is_empty() {
            self.lines.push(std::mem::take(&mut self.line));
        }
    }

    fn text(&mut self, text: &str) {
        #[cfg(test)]
        crate::work::render_feed(text.len());
        for part in text.split_inclusive(['\r', '\n']) {
            if self.line.spans.is_empty() {
                self.line.push(&self.prefix(), MUTED, None);
            }
            let text = part.trim_end_matches(['\r', '\n']);
            if self.new_link && self.link.is_some() && !text.is_empty() {
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
        self.context()
            .prefixes
            .iter()
            .flat_map(|prefixes| prefixes.iter())
            .map(AsRef::as_ref)
            .collect()
    }

    pub(crate) fn block_line(&mut self, line: Line) {
        let mut prefixed = Line::plain(&self.prefix(), MUTED);
        prefixed.spans.append_shared(line.spans);
        self.lines.push(prefixed);
    }

    pub(crate) fn event(&mut self, event: &Event<'_>) {
        self.event_in(event, None);
    }

    pub(crate) fn event_in(&mut self, event: &Event<'_>, references: Option<&crate::References>) {
        #[cfg(test)]
        let rendering = crate::work::rendering(true);
        if let Some(table) = &mut self.table {
            if matches!(event, Event::End(TagEnd::Table)) {
                if let Some(table) = self.table.take() {
                    for line in table.render() {
                        self.block_line(line);
                    }
                }
            } else {
                table.event(event, references);
            }
            #[cfg(test)]
            crate::work::rendering(rendering);
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
                self.block_line(rule());
            }
            Event::TaskListMarker(done) => {
                // Replace the bullet, retaining any nesting indentation.
                if let Some(marker) = self.line.spans.last_mut() {
                    marker.text.clear();
                }
                if let Some(indent) = self
                    .edit_context()
                    .prefixes
                    .as_mut()
                    .and_then(|prefixes| edit(prefixes).last_mut())
                {
                    *indent = "    ".into();
                }
                self.text(if *done { "[✓] " } else { "[ ] " });
            }
            Event::FootnoteReference(text) => self.text(&format!("[{text}]")),
        }
        #[cfg(test)]
        crate::work::rendering(rendering);
    }

    fn item(&mut self) {
        self.flush();
        self.line.push(&self.prefix(), MUTED, None);
        let (marker, indent) = if let Some(Some(number)) = self
            .context()
            .lists
            .as_ref()
            .and_then(|lists| lists.last())
            .copied()
        {
            let marker = format!("{number}. ");
            if let Some(Some(next)) = self
                .edit_context()
                .lists
                .as_mut()
                .and_then(|lists| edit(lists).last_mut())
            {
                *next += 1;
            }
            let indent = marker.chars().count();
            (crate::Text::from(marker), indent)
        } else {
            static BULLET: std::sync::OnceLock<crate::Text> = std::sync::OnceLock::new();
            (BULLET.get_or_init(|| "• ".into()).clone(), 2)
        };
        self.line.spans.push_content(
            crate::Span {
                text: marker,
                style: Style {
                    foreground: Some(MAGENTA),
                    ..Style::default()
                },
                link: None,
            },
            false,
        );
        edit(
            self.edit_context()
                .prefixes
                .get_or_insert_with(Arc::default),
        )
        .push(" ".repeat(indent).into());
    }

    fn start(&mut self, tag: &Tag<'_>, references: Option<&crate::References>) {
        if styled(tag.to_end()) {
            let saved = (self.style, self.link.clone());
            edit(self.edit_context().styles.get_or_insert_with(Arc::default)).push(saved);
        }
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
                edit(
                    self.edit_context()
                        .prefixes
                        .get_or_insert_with(Arc::default),
                )
                .push("│ ".into());
                self.style.italic = true;
                self.style.dim = true;
            }
            Tag::List(start) => {
                self.flush();
                edit(self.edit_context().lists.get_or_insert_with(Arc::default)).push(*start);
            }
            Tag::Item => self.item(),
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
                    .context()
                    .prefixes
                    .iter()
                    .flat_map(|prefixes| prefixes.iter())
                    .map(|prefix| unicode_width::UnicodeWidthStr::width(prefix.as_ref()))
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
                discard(&mut self.edit_context().prefixes);
            }
            TagEnd::List(_) => {
                self.flush();
                discard(&mut self.edit_context().lists);
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
        if styled(tag)
            && let Some((style, link)) = pop(&mut self.edit_context().styles)
        {
            self.style = style;
            self.link = link;
        }
        if matches!(tag, TagEnd::Link | TagEnd::Image) {
            self.new_link = self.link.is_some();
        }
        if self.context().styles.is_none()
            && self.context().lists.is_none()
            && self.context().prefixes.is_none()
        {
            self.context = None;
        }
    }
}

#[cfg(test)]
thread_local! { pub(super) static CLONE_CONTEXT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }

#[cfg(test)]
#[path = "../tests/render_context.rs"]
mod context_tests;

/// The rendered thematic break.
pub(crate) fn rule() -> Line {
    static RULE: std::sync::OnceLock<Line> = std::sync::OnceLock::new();
    RULE.get_or_init(|| Line::plain(&"─".repeat(40), MUTED))
        .clone()
}
