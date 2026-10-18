fn names(prompt: &str, name: &[u8]) -> bool {
    prompt
        .as_bytes()
        .windows(name.len())
        .any(|text| text.eq_ignore_ascii_case(name))
}

pub(super) fn selection(prompt: &str, goal: bool, plans: bool) -> (bool, bool) {
    (
        goal || names(prompt, b"goal.md") || names(prompt, b"goal mode"),
        plans || names(prompt, b"plans/") || names(prompt, b"plan mode"),
    )
}
