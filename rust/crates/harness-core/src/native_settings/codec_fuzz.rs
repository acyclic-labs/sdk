//! Arbitrary inputs enter the same native settings parser and hook importer.
//! This does not execute commands or qualify host trust and file precedence.
use super::*;
use crate::{hooks::Hooks, tool::ToolRegistry};

pub fn check(bytes: &[u8]) -> Result<()> {
    for toml in [false, true] {
        let mut imported = Hooks::default();
        let admitted = if toml {
            std::str::from_utf8(bytes)
                .map_err(|error| Error::Invalid(error.to_string()))
                .and_then(|document| imported.add_toml(document))
        } else {
            imported.add_json(bytes)
        };
        if admitted.is_ok() {
            check_rules(&imported, bytes)?;
        } else if !imported.is_empty() {
            return Err(Error::Invalid(
                "refused settings retained partial hook rules".into(),
            ));
        }
        let Ok(mut document) = parse::<BTreeMap<String, Value>>(bytes, toml) else {
            continue;
        };
        let encoded =
            serde_json::to_vec(&document).map_err(|error| Error::Invalid(error.to_string()))?;
        if parse::<BTreeMap<String, Value>>(&encoded, false)? != document {
            return Err(Error::Invalid(
                "settings projection changed original values".into(),
            ));
        }
        let Some(declarations) = document.remove("hooks") else {
            continue;
        };
        let Ok(hooks) = serde_json::from_value(declarations) else {
            continue;
        };
        let mut original = Hooks::default();
        if original
            .add_document(crate::hooks::imports::Document { hooks })
            .is_err()
        {
            if !original.is_empty() {
                return Err(Error::Invalid(
                    "refused settings retained partial hook rules".into(),
                ));
            }
            continue;
        }
        if admitted.is_ok()
            && serde_json::to_value(&original).map_err(|error| Error::Invalid(error.to_string()))?
                != serde_json::to_value(&imported)
                    .map_err(|error| Error::Invalid(error.to_string()))?
        {
            return Err(Error::Invalid(
                "native and public settings imports changed admitted rules".into(),
            ));
        }
        check_rules(&original, bytes)?;
    }
    Ok(())
}

fn check_rules(original: &Hooks, bytes: &[u8]) -> Result<()> {
    let normalized =
        serde_json::to_vec(original).map_err(|error| Error::Invalid(error.to_string()))?;
    let restored: Hooks =
        serde_json::from_slice(&normalized).map_err(|error| Error::Invalid(error.to_string()))?;
    let encoded = restored.encode(normalized.len() as u64)?;
    if encoded != normalized {
        return Err(Error::Invalid(
            "normalized settings changed original rules".into(),
        ));
    }
    let registry = ToolRegistry::default();
    for subject in ["shell", "Read", std::str::from_utf8(bytes).unwrap_or("")] {
        let input = serde_json::json!({"tool_name": subject, "source": subject});
        for event in crate::hooks::HookEvent::ALL {
            let selected = |rules: &Hooks| -> Result<Vec<crate::hooks::HookSpec>> {
                Ok(rules
                    .commands(event, &input, &registry)?
                    .map(|hook| hook.as_ref().clone())
                    .collect())
            };
            if selected(original)? != selected(&restored)? {
                return Err(Error::Invalid(
                    "normalized settings changed event selection".into(),
                ));
            }
        }
    }
    Ok(())
}
