fn read_history(history: Option<&Arc<History>>, tail: u64, from: u64, limit: usize) -> Vec<Record> {
    let mut windows = Vec::new();
    collect_history_windows(history, tail, from, &mut windows);
    windows
        .into_iter()
        .flat_map(|window| {
            (window.start..window.end).map(move |index| window.records[index].clone())
        })
        .take(limit)
        .collect()
}

fn is_direct_child(parent: &StreamPath, candidate: &StreamPath) -> bool {
    candidate.parent().as_ref() == Some(parent)
}
