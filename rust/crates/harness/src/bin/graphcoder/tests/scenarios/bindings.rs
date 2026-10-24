use super::TestResult;
use serde_json::Value;

// Select what the model actually sees, without duplicating the provider's
// private canonical-name encoding. Descriptions are declared by the fixture.
pub fn tool<'a>(request: &'a Value, description: &str) -> TestResult<&'a str> {
    select(request, |tool| tool["description"] == description)
}

pub fn pinned<'a>(request: &'a Value, canonical: &str) -> TestResult<&'a str> {
    select(request, |tool| {
        tool["description"]
            .as_str()
            .and_then(|description| description.split_once('\n'))
            .is_some_and(|(binding, _)| binding == canonical)
    })
}

fn select(request: &Value, matches: impl Fn(&Value) -> bool) -> TestResult<&str> {
    let mut matches = request["tools"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|tool| matches(tool));
    let definition = matches
        .next()
        .ok_or("advertised tool description missing")?;
    if matches.next().is_some() {
        return Err("advertised tool description is ambiguous".into());
    }
    definition["name"]
        .as_str()
        .ok_or_else(|| "advertised tool name missing".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use serde_json::json;

    proptest! {
        #[test]
        fn advertised_names_are_selected_independently_of_alias_spelling(
            name in "[a-zA-Z_][a-zA-Z0-9_]{0,63}", description in ".{1,64}",
        ) {
            let mut request = json!({"tools":[{"name":name,"description":description}]});
            let decoded: Value = serde_json::from_slice(&serde_json::to_vec(&request)?)?;
            prop_assert_eq!(tool(&decoded, &description).unwrap(), name.as_str());
            let duplicate = request["tools"][0].clone();
            request["tools"].as_array_mut().unwrap().push(duplicate);
            prop_assert!(tool(&request, &description).is_err(), "first-match control accepted ambiguous tool");
            request["tools"] = json!([]);
            prop_assert!(tool(&request, &description).is_err(), "missing tool passed");
        }

        #[test]
        fn pinned_calls_use_only_the_advertised_revision_alias(
            alias in "[a-zA-Z_][a-zA-Z0-9_]{0,63}", revision in "[a-z0-9-]{1,32}",
        ) {
            let canonical = format!("read@{revision}");
            let mut request = json!({"tools":[{"name":alias,"description":format!("{canonical}\nRead bytes")}]});
            let decoded: Value = serde_json::from_slice(&serde_json::to_vec(&request)?)?;
            prop_assert_eq!(pinned(&decoded, &canonical).unwrap(), alias.as_str());
            prop_assert!(pinned(&decoded, "read@ungranted.revision").is_err());
            let duplicate = request["tools"][0].clone();
            request["tools"].as_array_mut().unwrap().push(duplicate);
            prop_assert!(pinned(&request, &canonical).is_err());
            request["tools"] = json!([]);
            prop_assert!(pinned(&request, &canonical).is_err());
        }
    }
}
