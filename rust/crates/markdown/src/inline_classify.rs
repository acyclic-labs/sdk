#[derive(Default)]
struct Table {
    phase: u8,
    cells: usize,
    invalid: bool,
    first_pipe: bool,
}
impl Table {
    fn push(&mut self, c: char) {
        if self.invalid {
            return;
        }
        match c {
            ' ' | '\t' => {
                if self.phase == 2 {
                    self.phase = 3;
                }
            }
            ':' if self.phase == 0 => self.phase = 1,
            ':' if self.phase == 2 => self.phase = 3,
            '-' if self.phase <= 2 => self.phase = 2,
            '|' if self.phase >= 2 => {
                self.cells += 1;
                self.phase = 0;
                self.first_pipe = true;
            }
            '|' if self.cells == 0 && self.phase == 0 && !self.first_pipe => self.first_pipe = true,
            _ => self.invalid = true,
        }
    }
    fn accepts(&self, columns: usize) -> bool {
        !self.invalid && self.cells + usize::from(self.phase >= 2) == columns && columns != 0
    }
}

#[derive(Default)]
enum Definition {
    #[default]
    Start,
    Label(bool),
    Colon,
    Leading,
    Angle(bool),
    Bare(usize, bool),
    AfterURL,
    Title(char, bool),
    AfterTitle,
    Invalid,
}
impl Definition {
    fn push(&mut self, c: char) -> bool {
        let mut ready = false;
        *self = match *self {
            Self::Start if matches!(c, ' ' | '\t') => Self::Start,
            Self::Start if c == '[' => Self::Label(false),
            Self::Label(false) if c == '\\' => Self::Label(true),
            Self::Label(false) if c == ']' => Self::Colon,
            Self::Label(false) if c == '[' => Self::Invalid,
            Self::Label(_) => Self::Label(false),
            Self::Colon if c == ':' => Self::Leading,
            Self::Leading if c.is_ascii_whitespace() => Self::Leading,
            Self::Leading if c == '<' => Self::Angle(false),
            Self::Leading if c == '(' => Self::Bare(1, false),
            Self::Leading => {
                ready = true;
                Self::Bare(0, false)
            }
            Self::Angle(false) if c == '\\' => Self::Angle(true),
            Self::Angle(false) if c == '>' => {
                ready = true;
                Self::AfterURL
            }
            Self::Angle(false) if matches!(c, '<' | '\n') => Self::Invalid,
            Self::Angle(_) => Self::Angle(false),
            Self::Bare(depth, false) if c == '\\' => Self::Bare(depth, true),
            Self::Bare(depth, false) if c == '(' && depth < 33 => Self::Bare(depth + 1, false),
            Self::Bare(1, false) if c == ')' => {
                ready = true;
                Self::Bare(0, false)
            }
            Self::Bare(0, false) if c == ')' => Self::Invalid,
            Self::Bare(depth, false) if c == ')' => Self::Bare(depth - 1, false),
            Self::Bare(0, false) | Self::AfterURL if c.is_ascii_whitespace() => Self::AfterURL,
            Self::Bare(_, false) if c.is_ascii_whitespace() || c == '(' => Self::Invalid,
            Self::Bare(depth, _) => Self::Bare(depth, false),
            Self::AfterURL if matches!(c, '\'' | '"' | '(') => {
                Self::Title(if c == '(' { ')' } else { c }, false)
            }
            Self::Title(close, false) if c == '\\' => Self::Title(close, true),
            Self::Title(close, false) if c == close => {
                ready = true;
                Self::AfterTitle
            }
            Self::Title(close, _) => Self::Title(close, false),
            Self::AfterTitle if c.is_ascii_whitespace() => Self::AfterTitle,
            Self::Start | Self::Colon | Self::AfterURL | Self::AfterTitle | Self::Invalid => {
                Self::Invalid
            }
        };
        ready
    }
}

#[derive(Default)]
pub(super) struct Rows {
    read: usize,
    closed: bool,
    origin: usize,
    row: usize,
    previous: usize,
    after_break: bool,
    significant: usize,
    first: Option<usize>,
    marker: bool,
    space: bool,
    pipes: usize,
    leading_pipe: bool,
    trailing_pipe: bool,
    row_content: bool,
    escaped: bool,
    columns: usize,
    table: Table,
    table_accepted: bool,
    definition: Definition,
}
impl Rows {
    pub(super) fn at(origin: usize) -> Self {
        Self {
            read: origin,
            closed: false,
            origin,
            row: origin,
            previous: origin,
            after_break: true,
            significant: 0,
            first: None,
            marker: false,
            space: false,
            pipes: 0,
            leading_pipe: false,
            trailing_pipe: false,
            row_content: false,
            escaped: false,
            columns: 0,
            table: Table::default(),
            table_accepted: false,
            definition: Definition::default(),
        }
    }
    pub(super) fn permanent(&self) -> bool {
        matches!(self.definition, Definition::Invalid)
    }

    pub(super) fn closed(&self) -> bool {
        self.closed
    }
    pub(super) fn inline_at(&self, position: usize) -> bool {
        self.first.is_some_and(|first| first < position)
            && matches!(
                self.definition,
                Definition::Start | Definition::Label(_) | Definition::Colon | Definition::Invalid
            )
    }
    // While the current row can still become a block interrupt, its start is
    // reclassified on every push, so no checkpoint may pass it.
    pub(super) fn undecided(&self) -> Option<usize> {
        (self.first.is_none() || (self.marker && self.significant <= "</blockquote>".len() + 1))
            .then_some(self.row)
    }
    fn marker_probe(&mut self, source: &str, c: char, white: bool, broad: bool) -> bool {
        // Asterisks can interrupt only as a three-marker rule or a
        // bullet followed by horizontal whitespace and content.
        // Once another byte disqualifies both, this row is inline.
        let star = self.first.and_then(|at| source.as_bytes().get(at)) == Some(&b'*');
        let bullet = self
            .first
            .and_then(|at| source.as_bytes().get(at + 1))
            .is_some_and(|byte| matches!(byte, b' ' | b'\t'));
        if !broad && star && !bullet && !white && c != '*' {
            self.marker = false;
        }
        // Interrupts are decided within a marker, nine ordered-list digits,
        // or the longest HTML block tag plus content.
        self.marker
            && (broad || !star || bullet || self.significant >= 3)
            && self.significant <= "</blockquote>".len() + 1
            && (!white || !self.space)
    }

    pub(super) fn push(&mut self, source: &str) -> Option<usize> {
        let mut probe = None;
        #[cfg(test)]
        let broad_probes = super::REPLAY_ROWS.get();
        #[cfg(not(test))]
        let broad_probes = false;
        for (relative, c) in source.get(self.read..)?.char_indices() {
            #[cfg(test)]
            crate::work::scan(c.len_utf8());
            let at = self.read + relative;
            if self.definition.push(c) {
                probe = Some(self.origin);
            }
            if c == '\n' {
                if self.after_break && !self.row_content {
                    self.closed = true;
                    probe = Some(probe.map_or(self.previous, |p: usize| p.min(self.previous)));
                }
                self.columns = (self.pipes + 1).saturating_sub(
                    usize::from(self.leading_pipe) + usize::from(self.trailing_pipe),
                );
                self.previous = self.row;
                self.row = at + 1;
                self.after_break = true;
                self.significant = 0;
                self.first = None;
                self.marker = false;
                self.space = false;
                self.pipes = 0;
                self.leading_pipe = false;
                self.trailing_pipe = false;
                self.row_content = false;
                self.escaped = false;
                self.table = Table::default();
                self.table_accepted = false;
                continue;
            }
            if self.after_break {
                self.table.push(c);
                let accepted = self.table.accepts(self.columns);
                if accepted && !self.table_accepted {
                    probe = Some(probe.map_or(self.previous, |p| p.min(self.previous)));
                }
                self.table_accepted = accepted;
                let white = matches!(c, ' ' | '\t' | '\u{b}' | '\u{c}');
                if !white {
                    self.significant += 1;
                    if self.significant == 1 {
                        self.first = Some(at);
                        self.marker = matches!(
                            c,
                            '#' | '=' | '-' | '_' | '+' | '*' | '~' | '`' | '>' | '<' | '0'..='9'
                        );
                    }
                }
                if self.marker_probe(source, c, white, broad_probes) {
                    probe = Some(probe.map_or(self.row, |p| p.min(self.row)));
                }
                self.space = white;
            }
            if c == '|' && !self.escaped {
                self.pipes += 1;
                self.leading_pipe |= !self.row_content;
                self.trailing_pipe = true;
            } else if !matches!(c, ' ' | '\t') {
                self.trailing_pipe = false;
            }
            self.row_content |= !matches!(c, ' ' | '\t');
            self.escaped = c == '\\' && !self.escaped;
        }
        self.read = source.len();
        probe
    }
}
