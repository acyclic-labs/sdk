// Compile the shipping selector itself, without exporting a testing API.
#[path = "../../src/local/context/guidance/selection.rs"]
mod selection;

pub fn check(input: &[u8]) {
    if input.len() > 4096 {
        return;
    }
    if let Ok(prompt) = std::str::from_utf8(input) {
        assert_eq!(prompt.as_bytes(), input);
        let lower = prompt.to_ascii_lowercase();
        for goal in [false, true] {
            for plans in [false, true] {
                assert_eq!(
                    selection::selection(prompt, goal, plans),
                    (
                        goal || lower.contains("goal.md") || lower.contains("goal mode"),
                        plans || lower.contains("plans/") || lower.contains("plan mode")
                    )
                );
            }
        }
    }
}
