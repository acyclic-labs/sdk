use std::collections::BTreeMap;

// ASCII radix index: one bounded-alphabet edge per input byte, including misses.
// It indexes canonical names and aliases together; discovery never scans specs.
#[derive(Clone, Debug)]
pub(super) struct NameIndex {
    nodes: Vec<Node>,
}

#[derive(Clone, Debug, Default)]
struct Node {
    edges: BTreeMap<u8, usize>,
    canonical: Option<String>,
}

impl Default for NameIndex {
    fn default() -> Self {
        Self {
            nodes: vec![Node::default()],
        }
    }
}

impl NameIndex {
    pub(super) fn insert(&mut self, name: &str, canonical: &str) -> Option<String> {
        let mut node = 0;
        for byte in name.bytes().map(|byte| byte.to_ascii_lowercase()) {
            step();
            node = match self.nodes[node].edges.get(&byte) {
                Some(&next) => next,
                None => {
                    let next = self.nodes.len();
                    self.nodes.push(Node::default());
                    self.nodes[node].edges.insert(byte, next);
                    next
                }
            };
        }
        self.nodes[node].canonical.replace(canonical.into())
    }

    pub(super) fn get(&self, name: &str) -> Option<&str> {
        let mut node = 0;
        for byte in name.bytes().map(|byte| byte.to_ascii_lowercase()) {
            step();
            node = *self.nodes[node].edges.get(&byte)?;
        }
        self.nodes[node].canonical.as_deref()
    }
}

#[inline]
fn step() {
    #[cfg(test)]
    STEPS.with(|steps| steps.set(steps.get() + 1));
}

#[cfg(test)]
thread_local! { static STEPS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]
        // Actual production insert/miss/hit paths visit at most one index edge
        // per name byte. Each edge map has at most 128 ASCII keys, independent of
        // catalog size. Retained nodes are also linear in definition bytes.
        #[test]
        fn discovery_index_work_is_linear(
            names in proptest::collection::btree_set("[a-zA-Z0-9:_-]{1,48}", 1..256),
        ) {
            STEPS.with(|steps| steps.set(0));
            let mut index = NameIndex::default();
            let mut budget = 0;
            for name in &names {
                let canonical = name.to_ascii_lowercase();
                budget += name.len();
                index.insert(name, &canonical);
                budget += name.len();
                prop_assert_eq!(index.get(&name.to_ascii_uppercase()), Some(canonical.as_str()));
                let absent = format!("{name}!");
                budget += absent.len();
                prop_assert_eq!(index.get(&absent), None);
            }
            prop_assert!(STEPS.with(|steps| steps.get()) <= budget);
            prop_assert!(index.nodes.len() <= 1 + names.iter().map(String::len).sum::<usize>());
        }
    }
}
