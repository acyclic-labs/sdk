use super::*;

fn attributes(scope: &Scope, events: &[(Event<'_>, Range<usize>)]) -> Option<(bool, bool, bool)> {
    match &events.get(scope.events.start)?.0 {
        Event::Start(tag @ (Tag::Link { link_type, .. } | Tag::Image { link_type, .. })) => {
            let labelled = !matches!(
                link_type,
                pulldown_cmark::LinkType::Autolink | pulldown_cmark::LinkType::Email
            );
            let hint = matches!(
                link_type,
                pulldown_cmark::LinkType::Shortcut
                    | pulldown_cmark::LinkType::ShortcutUnknown
                    | pulldown_cmark::LinkType::Collapsed
                    | pulldown_cmark::LinkType::CollapsedUnknown
                    | pulldown_cmark::LinkType::Reference
                    | pulldown_cmark::LinkType::ReferenceUnknown
            );
            Some((labelled, hint, labelled && matches!(tag, Tag::Link { .. })))
        }
        _ => Some((false, false, false)),
    }
}

impl Scanner {
    pub(super) fn bind_scope(
        &mut self,
        source: &Slice<'_>,
        index: usize,
        scopes: &[Scope],
        events: &[(Event<'_>, Range<usize>)],
        references: &References,
    ) -> Option<usize> {
        // Frames retain only the current nesting chain. URL tokens never enter
        // the inline state; label tokens do, including their code checkpoints.
        let mut parents = Vec::<(usize, usize, usize, state::anchors::Anchor)>::new();
        let mut frame = None;
        let mut scope_index = index;
        let mut token_index = self
            .tokens
            .ordinal_at(scopes.get(index)?.range.start)?
            .checked_sub(self.discarded)?;
        loop {
            let scope = scopes.get(scope_index)?;
            let output = self.render.compile(
                source,
                scope_index,
                scopes,
                events,
                &self.tokens,
                references,
            )?;
            let (labelled, hint, disables) = attributes(scope, events)?;
            if !labelled
                || (!hint
                    && self.state.foreign_neutral()
                    && self.render.independent_label(source, scope, events))
            {
                if matches!(events.get(scope.events.start), Some((Event::Html(_), _))) {
                    self.state.finish_block(scope.range.start);
                    self.pending_break = None;
                }
                self.state.scope(scope, output, hint, disables);
                token_index = self
                    .tokens
                    .after(scope.range.end)
                    .unwrap_or(self.tokens.ready.len());
                scope_index = scope.next;
            } else {
                let before = self.state.resume();
                if let Some(parent) = frame.take() {
                    #[cfg(test)]
                    work::copy(
                        std::mem::size_of::<(usize, usize, usize, state::anchors::Anchor)>()
                            * (1 + if parents.len() == parents.capacity() {
                                parents.len()
                            } else {
                                0
                            }),
                    );
                    parents.push(parent);
                }
                let token = self.tokens.ready.get(token_index)?;
                self.state.feed(token.range.clone(), token.kind, false);
                frame = Some((scope_index, output, scope.close, before));
                #[cfg(test)]
                {
                    self.fed_bytes += token.range.len();
                    work::scan(1);
                }
                token_index += 1;
                scope_index += 1;
            }
            loop {
                let Some((parent, output, close, _)) = frame else {
                    return Some(scopes.get(index)?.range.end);
                };
                let scope = scopes.get(parent)?;
                let token = self.tokens.ready.get(token_index)?;
                if scope_index < scope.next
                    && scopes.get(scope_index)?.range.start == token.range.start
                {
                    break;
                }
                let range = token.range.clone();
                self.state.feed(range.clone(), token.kind, false);
                #[cfg(test)]
                {
                    self.fed_bytes += range.len();
                    work::scan(1);
                }
                token_index += 1;
                if range.end == close {
                    let (_, _, _, before) = frame.take()?;
                    let (_, hint, disables) = attributes(scope, events)?;
                    if hint || self.state.code_pending() {
                        #[cfg(test)]
                        work::copy(std::mem::size_of::<state::anchors::Anchor>());
                        let anchor = self.tokens.anchor_at(scope.range.start)?;
                        self.state.remember_resume(anchor, before.clone());
                    } else if let Some(anchor) = self.tokens.anchor_at(scope.range.start) {
                        self.state.forget(anchor);
                    }
                    self.state.bind_resume(&before)?;
                    self.state.scope(scope, output, hint, disables);
                    token_index = self.tokens.after(scope.range.end)?;
                    scope_index = scope.next;
                    frame = parents.pop();
                } else if range.end >= scope.range.end || self.state.invalid {
                    return None;
                }
            }
        }
    }
}
