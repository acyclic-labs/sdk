//! Shared private namespace and admission predicate for composed mount names.

pub(crate) const CHECKOUT_RECOVERY_PREFIX: &str = ".volumes/agents/.publication-v1/";

pub(crate) fn valid_component(name: &str) -> bool {
    !name.is_empty()
        && !matches!(name, "." | "..")
        && !CHECKOUT_RECOVERY_PREFIX
            .split('/')
            .skip(1)
            .any(|reserved| name.eq_ignore_ascii_case(reserved))
        && !name.contains(['/', '\\', '\0', ':'])
        && !name.chars().any(char::is_control)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recovery_namespace_is_disjoint_from_volume_and_agent_aliases() {
        for reserved in CHECKOUT_RECOVERY_PREFIX
            .split('/')
            .skip(1)
            .filter(|name| !name.is_empty())
        {
            for mask in 0..(1usize << reserved.len()) {
                let spelling: String = reserved
                    .bytes()
                    .enumerate()
                    .map(|(index, byte)| {
                        char::from(if mask & (1 << index) == 0 {
                            byte
                        } else {
                            byte.to_ascii_uppercase()
                        })
                    })
                    .collect();
                assert!(!valid_component(&spelling), "{spelling}");
                assert!(valid_component(&format!("{spelling}-volume")));
            }
        }
    }
}
