use super::State;
use crate::inline_scan::Scope;

impl State {
    pub(in crate::inline_scan) fn scope(
        &mut self,
        scope: &Scope,
        output: usize,
        hint: bool,
        disables: bool,
    ) {
        if let Some(eof) = &mut self.eof {
            eof.invalidate();
        }
        self.forest.atom(scope.range.clone(), output);
        self.labels = if hint {
            [self.remember_closed(scope.range.clone(), 0), 0]
        } else {
            [0, 0]
        };
        if disables {
            self.link_floor = scope.range.end;
        }
    }
}
