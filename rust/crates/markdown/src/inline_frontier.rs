use super::*;

impl Scanner {
    fn removed_scope(
        &self,
        source: &Slice<'_>,
        events: &[(Event<'_>, Range<usize>)],
        syntax: &Syntax,
    ) -> Option<usize> {
        if !source.classified {
            return None;
        }
        let (base, sentinel) = (source.base, source.sentinel);

        let first = self
            .capsules
            .partition_point(|scope| scope.end <= base.max(self.frozen));
        let end = base + source.parsed.len().saturating_sub(sentinel);
        let mut next = 0;
        let mut codes = events
            .iter()
            .filter(|(event, _)| {
                #[cfg(test)]
                work::scan(1);
                matches!(event, Event::Code(_))
            })
            .map(|(_, range)| {
                base + range.start.saturating_sub(sentinel)
                    ..base + range.end.saturating_sub(sentinel)
            })
            .peekable();
        self.capsules.get(first..)?.iter().find_map(|old| {
            #[cfg(test)]
            work::scan(1);
            if old.start < base.max(self.frozen) || old.end > end {
                return None;
            }
            // Native code snapshots own hiding/restoring their children.
            // A provisional closer must not erase those compiled children.
            while codes.peek().is_some_and(|code| code.end < old.end) {
                codes.next();
            }
            if codes
                .peek()
                .is_some_and(|code| code.start <= old.start && old.end <= code.end)
            {
                return None;
            }
            while syntax
                .outer
                .get(next)
                .is_some_and(|scope| scope.range.end < old.end)
            {
                next += 1;
                #[cfg(test)]
                work::scan(1);
            }
            (!syntax
                .outer
                .get(next)
                .is_some_and(|scope| scope.range.start <= old.start && old.end <= scope.range.end))
            .then_some(old.start)
        })
    }

    fn prepare_syntax(
        &mut self,
        source: &Slice<'_>,
        events: &[(Event<'_>, Range<usize>)],
    ) -> (Syntax, Option<usize>) {
        let (base, sentinel) = (source.base, source.sentinel);
        let mut syntax = Syntax::new(
            events,
            source.parsed,
            &self.tokens,
            base,
            sentinel,
            self.discarded,
            self.frozen,
        );
        syntax.absolute(base, sentinel);
        let changed = syntax.changed_before(&self.syntax, base, self.processed, &self.capsules);
        let removed = self.removed_scope(source, events, &syntax);
        let changed = changed.into_iter().chain(removed).min();
        while self
            .capsules
            .last()
            .is_some_and(|scope| scope.end > base.max(self.frozen))
        {
            #[cfg(test)]
            work::scan(1);
            self.capsules.pop();
        }
        for scope in &syntax.outer {
            if self
                .capsules
                .last()
                .is_none_or(|last| last.end < scope.range.end)
            {
                #[cfg(test)]
                work::copy(
                    std::mem::size_of::<Range<usize>>()
                        * (1 + if self.capsules.len() == self.capsules.capacity() {
                            self.capsules.len()
                        } else {
                            0
                        }),
                );
                self.capsules.push(scope.range.clone());
            }
        }
        let fresh_breaks = std::mem::take(&mut syntax.hard_breaks);
        syntax.hard_breaks = std::mem::take(&mut self.syntax.hard_breaks);
        for position in fresh_breaks {
            #[cfg(test)]
            work::copy(
                std::mem::size_of::<usize>()
                    * (1 + if syntax.hard_breaks.len() == syntax.hard_breaks.capacity() {
                        syntax.hard_breaks.len()
                    } else {
                        0
                    }),
            );
            syntax.hard_breaks.insert(position);
        }
        (syntax, changed)
    }

    fn restore_classification(&mut self, source: &Slice<'_>, changed: Option<usize>) -> bool {
        let base = source.base;
        #[cfg(test)]
        let replay = self.replay;
        #[cfg(not(test))]
        let replay = false;
        if replay || changed.is_some() {
            self.pending_break = None;
            let restored = if replay {
                None
            } else {
                changed.and_then(|position| {
                    let resume = self.state.classification_start(position);
                    [position, resume, base].into_iter().find_map(|position| {
                        let ordinal = self.tokens.ordinal_at(position)?;
                        if ordinal < self.discarded {
                            return None;
                        }
                        let anchor = self.tokens.anchor_at(position)?;
                        self.state.restore_at(anchor).map(|()| (position, ordinal))
                    })
                })
            };
            if let Some((position, ordinal)) = restored {
                self.processed = position;
                self.fed = ordinal - self.discarded;
                self.frontier = self.frontier.min(position);
            } else {
                self.state.reset();
                self.line_content = false;
                if !replay && base > source.start {
                    self.state.invalid = true;
                    return false;
                }
                self.processed = base;
                self.fed = 0;
                self.frontier = base;
            }
        }
        true
    }

    fn line_end(
        &mut self,
        original: &str,
        kind: Kind,
        start: usize,
        end: usize,
        pending: Option<usize>,
    ) {
        if kind == Kind::Break && !self.line_content {
            self.state.finish_block(end);
        }
        self.line_content = if original.as_bytes().get(end.saturating_sub(1)) == Some(&b'\n') {
            false
        } else {
            match kind {
                Kind::Break | Kind::Escape('\n') => false,
                Kind::Text if !self.line_content => original.get(start..end).is_some_and(|text| {
                    text.bytes().any(|byte| {
                        #[cfg(test)]
                        work::scan(1);
                        !matches!(byte, b' ' | b'\t')
                    })
                }),
                _ => true,
            }
        };
        if kind == Kind::Escape('\n') {
            self.pending_break = Some(start);
            self.pending_after_break = false;
        } else if kind == Kind::Break && self.pending_break.is_some() {
            self.pending_after_break = true;
        }
        if self.state.neutral_before(end)
            && self.pending_break.is_none()
            && (self.tokens.neutral() || pending.is_some_and(|start| start >= end))
        {
            self.frontier = end;
        }
    }

    fn feed_tokens(
        &mut self,
        source: &Slice<'_>,
        syntax: &mut Syntax,
        events: &[(Event<'_>, Range<usize>)],
        references: &References,
    ) -> bool {
        let original = source.original;
        let pending = self.tokens.preview().map(|token| token.range.start);
        let mut scope = 0;
        let mut token_index = self.fed;
        while let Some(token) = self.tokens.ready.get(token_index) {
            let kind = token.kind;
            token_index += 1;
            #[cfg(test)]
            work::scan(1);
            if token.range.end <= self.processed {
                continue;
            }
            let start = token.range.start.max(source.start).max(self.processed);
            let mut end = token.range.end;
            if self.first_expansion.is_none() && kind.expands(token.range.clone()) {
                self.first_expansion = Some(start);
            }
            #[cfg(test)]
            {
                self.fed_bytes += end - start;
            }
            if !matches!(kind, Kind::Escape('\n') | Kind::Break)
                && self.pending_break.is_some()
                && original.get(start..end).is_some_and(|raw| {
                    raw.bytes().any(|byte| {
                        #[cfg(test)]
                        work::scan(1);
                        let expanded = self.first_expansion.is_some_and(|first| {
                            self.pending_break.is_some_and(|pending| first < pending)
                        });
                        !matches!(byte, b' ' | b'\t' | b'\n')
                            && (!expanded
                                || self.pending_after_break
                                || !matches!(byte, 0x0b | 0x0c))
                    })
                })
                && let Some(previous) = self.pending_break.take()
            {
                #[cfg(test)]
                work::copy(
                    std::mem::size_of::<usize>()
                        * (1 + if syntax.hard_breaks.len() == syntax.hard_breaks.capacity() {
                            syntax.hard_breaks.len()
                        } else {
                            0
                        }),
                );
                syntax.hard_breaks.insert(previous);
            }
            self.freeze_before(source, start, &syntax.hard_breaks);
            while syntax
                .outer
                .get(scope)
                .is_some_and(|scope| scope.range.end <= start)
            {
                scope += 1;
            }
            if syntax
                .outer
                .get(scope)
                .is_some_and(|scope| scope.range.start == start)
            {
                #[cfg(test)]
                {
                    self.fed_bytes -= end - start;
                }
                let Some(consumed) =
                    self.bind_scope(source, scope, &syntax.outer, events, references)
                else {
                    self.state.invalid = true;
                    return false;
                };
                end = consumed;
                if let Some(after) = self.tokens.after_absolute(end) {
                    token_index = token_index.max(after.saturating_sub(self.discarded));
                }
            } else {
                if matches!(kind, Kind::AngleOpen | Kind::Open { .. }) {
                    let Some(anchor) = self.tokens.anchor_at(start) else {
                        self.state.invalid = true;
                        return false;
                    };
                    self.state.remember(anchor);
                }
                self.state.feed(start..end, kind, false);
            }
            self.line_end(original, kind, start, end, pending);
            self.processed = end;
        }
        true
    }

    pub(crate) fn frontier(
        &mut self,
        source: Slice<'_>,
        events: &[(Event<'_>, Range<usize>)],
        references: &References,
    ) -> usize {
        let (base, sentinel) = (source.base, source.sentinel);
        self.tokens.push(source.original);
        self.render
            .track_destinations(source.original, source.start);
        let (mut syntax, changed) = self.prepare_syntax(&source, events);
        if !self.restore_classification(&source, changed)
            || !self.feed_tokens(&source, &mut syntax, events, references)
        {
            return sentinel;
        }
        // The lexer may extend its final plain-text token on the next push.
        self.fed = self.tokens.ready.len().saturating_sub(usize::from(
            self.tokens
                .ready
                .back()
                .is_some_and(|token| token.kind == Kind::Text),
        ));
        if self.state.invalid {
            return sentinel;
        }
        // An unresolved construct can reopen older ancestors. Advance only
        // through a boundary actually observed with neutral scanner state.
        let candidate = self
            .pending_break
            .map_or(self.frontier, |start| self.frontier.min(start));
        self.projected = self.state.projection(self.tokens.preview().as_ref());
        self.syntax = syntax;
        candidate.saturating_sub(base) + sentinel
    }
}
