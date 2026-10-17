use super::*;

impl Scanner {
    pub(super) fn freeze_before(
        &mut self,
        source: &Slice<'_>,
        position: usize,
        breaks: &HashSet<usize>,
    ) {
        // Ready tokens start at a neutral lexer boundary. Resolve any delayed
        // break before feeding the token that opens the next construct.
        if !self.state.neutral_before(position) || self.pending_break.is_some() {
            return;
        }
        self.frontier = self.frontier.max(position);
        if !self.permanent
            || position != self.processed
            || self.state.forest.structures() == self.folded_structures
        {
            return;
        }
        #[cfg(test)]
        if KEEP_KEYS.get() {
            return;
        }
        let root = self.state.forest.snapshot();
        let Some(anchors) = self.tokens.anchors_before(position) else {
            return;
        };
        let Some(output) = self.render.freeze_prefix(
            &mut self.state.forest,
            root,
            &forest::glyphs::events::projection::Projection {
                source: source.original,
                visible: self.origin..position,
                hard_breaks: breaks,
            },
        ) else {
            return;
        };
        self.state.prefix(self.origin..position, output, anchors);
        self.folded_structures = self.state.forest.structures();
        self.projected = self.state.forest.snapshot();
    }
}
