use super::*;

type ResumeCode = (usize, usize, (usize, usize, usize));

fn truncate_codes(
    codes: &[ResumeCode],
    widths: &mut HashMap<usize, usize>,
    head: &mut usize,
    parent: usize,
) -> bool {
    while *head != parent {
        let Some(&(width, previous, _)) = head.checked_sub(1).and_then(|index| codes.get(index))
        else {
            return false;
        };
        widths.remove(&width);
        *head = previous;
        #[cfg(test)]
        work::scan(1);
    }
    true
}

impl Scanner {
    pub(crate) fn classification_resume(&self, end: usize, reopen: Option<usize>) -> usize {
        let destination = self
            .render
            .completed_destinations()
            .filter(|start| *start <= end && self.state.destination_possible(*start))
            .min();
        let mut base = self.state.resume_closures(0, end, destination);
        if let Some(closed) = reopen {
            base = base.min(self.reopen_start(end, closed));
        }
        self.resume_tokens(base)
    }

    fn resume_tokens(&self, mut base: usize) -> usize {
        let mut depth = 0usize;
        let mut old = self.state.resume_bracket();
        let mut old_depth = self.state.resume_code_depth();
        // A code closer restores the bracket cursor from its opener. The
        // retained code prefix is read-only; only this chunk needs scratch nodes.
        let mut codes = Vec::<ResumeCode>::new();
        let mut head = 0usize;
        let mut widths = HashMap::<usize, usize>::new();
        let ready =
            (self.fed..self.tokens.ready.len()).filter_map(|index| self.tokens.ready.get(index));
        let preview = self.tokens.preview();
        for token in ready.chain(preview.as_ref()) {
            #[cfg(test)]
            work::scan(1);
            if token.range.end <= self.processed {
                continue;
            }
            if token.kind != Kind::Text {
                base = base.min(token.range.start);
            }
            let (physical, opening) = match token.kind {
                Kind::Run { byte: b'`', .. } => (token.range.len(), token.range.len()),
                Kind::Escape('`') => (token.range.len() - 1, token.range.len().saturating_sub(2)),
                Kind::Open { .. } => {
                    depth += 1;
                    continue;
                }
                Kind::Close if depth != 0 => {
                    depth -= 1;
                    continue;
                }
                Kind::Close => {
                    if let Some((start, previous)) = self.state.resume_opener(old) {
                        base = base.min(start);
                        old = previous;
                    }
                    continue;
                }
                _ => continue,
            };
            #[cfg(test)]
            work::scan(1);
            if let Some(&matched) = widths.get(&physical) {
                let Some(&(_, parent, saved)) =
                    matched.checked_sub(1).and_then(|index| codes.get(index))
                else {
                    return base;
                };
                if !truncate_codes(&codes, &mut widths, &mut head, parent) {
                    return base;
                }
                (depth, old, old_depth) = saved;
            } else if let Some((_, before, bracket, start)) = self
                .state
                .resume_code(physical)
                .filter(|(depth, _, _, _)| *depth <= old_depth)
            {
                if !truncate_codes(&codes, &mut widths, &mut head, 0) {
                    return base;
                }
                old = bracket;
                old_depth = before;
                depth = 0;
                base = base.min(start);
            } else if opening != 0
                && !widths.contains_key(&opening)
                && self
                    .state
                    .resume_code(opening)
                    .is_none_or(|(depth, _, _, _)| depth > old_depth)
            {
                #[cfg(test)]
                {
                    work::copy(
                        std::mem::size_of::<ResumeCode>()
                            * (1 + if codes.len() == codes.capacity() {
                                codes.len()
                            } else {
                                0
                            }),
                    );
                    work::copy(
                        std::mem::size_of::<(usize, usize)>()
                            * (1 + if widths.len() == widths.capacity() {
                                widths.len()
                            } else {
                                0
                            }),
                    );
                }
                codes.push((opening, head, (depth, old, old_depth)));
                head = codes.len();
                widths.insert(opening, head);
            }
        }
        base
    }
}
