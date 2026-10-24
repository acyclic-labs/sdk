use super::fuzzy_score;
use std::collections::BTreeSet;

/// Maximum pathname results in one UI publication. Tree cardinality does not
/// change the amount of matching work the editor performs on a keystroke.
pub const PATH_RESULT_COUNT: usize = 32;

/// Streaming fuzzy selection for a cancellable search worker. Retains the best
/// bounded set as index pages arrive; it never retains the candidate namespace.
#[derive(Clone)]
pub struct PathMatches {
    query: String,
    best: BTreeSet<(usize, String)>,
}

impl PathMatches {
    /// Starts an independent query. Publishers include this text in the result
    /// file so superseded work cannot be mistaken for current results.
    pub fn new(query: &str) -> Self {
        Self {
            query: query.to_owned(),
            best: BTreeSet::new(),
        }
    }

    /// Scores one newly streamed name, retaining at most PATH_RESULT_COUNT paths.
    pub fn push(&mut self, path: String) {
        if let Some(score) = fuzzy_score(&self.query, &path) {
            if self.best.len() == PATH_RESULT_COUNT
                && self
                    .best
                    .last()
                    .is_some_and(|(worst, name)| (score, &path) >= (*worst, name))
            {
                return;
            }
            self.best.insert((score, path));
            if self.best.len() > PATH_RESULT_COUNT {
                self.best.pop_last();
            }
        }
    }

    /// Ranked raw paths for one bounded result publication.
    pub fn paths(&self) -> impl ExactSizeIterator<Item = &str> {
        self.best.iter().map(|(_, path)| path.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn generated(count: usize, retain_all: bool, insert_rejected: bool) {
        let mut matches = PathMatches::new("rs");
        let mut retained = Vec::new();
        let started = std::time::Instant::now();
        let memory = acyclic_test_support::measure(|| {
            for i in 0..count {
                let path = format!("generated/{i:06}.rs");
                if retain_all {
                    retained.push(path.clone());
                }
                if insert_rejected {
                    // Negative control: bounded retention alone still churns
                    // tree nodes for candidates worse than every retained row.
                    if let Some(score) = fuzzy_score(&matches.query, &path) {
                        matches.best.insert((score, path));
                        if matches.best.len() > PATH_RESULT_COUNT {
                            matches.best.pop_last();
                        }
                    }
                } else {
                    matches.push(path);
                }
            }
        });
        assert_eq!(matches.paths().len(), PATH_RESULT_COUNT);
        assert!(
            memory.bytes_max < 64 * 1024,
            "stream retained the namespace: {memory:?}"
        );
        assert!(
            memory.count_total <= count as u64 + (4 * PATH_RESULT_COUNT) as u64,
            "allocated ranking nodes for rejected top-k candidates: {memory:?}"
        );
        eprintln!(
            "volume-search benchmark component=streaming-top-k entries={count} elapsed_us={} peak_bytes={} allocations={} retained_results={}",
            started.elapsed().as_micros(),
            memory.bytes_max,
            memory.count_total,
            matches.paths().len(),
        );
        std::hint::black_box(retained);
    }

    #[test]
    fn million_generated_candidates_keep_only_the_bounded_top_k() {
        generated(1_000_000, false, false);
    }

    #[test]
    #[should_panic(expected = "stream retained the namespace")]
    fn negative_control_detects_retaining_every_candidate() {
        generated(10_000, true, false);
    }

    #[test]
    #[should_panic(expected = "allocated ranking nodes for rejected top-k candidates")]
    fn negative_control_detects_inserting_every_rejected_candidate() {
        generated(10_000, false, true);
    }
}
