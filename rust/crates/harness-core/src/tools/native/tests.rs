use super::*;
use acyclic_model_render::{Family, ToolDeclarations};

#[test]
fn pinned_family_tool_lists_preserve_public_contract_bytes() -> Result<()> {
    let originals = crate::tools::defaults()?;
    for (profile, family, golden) in [
        (
            NativeToolProfile::Anthropic,
            Family::Anthropic,
            include_str!("golden/anthropic.json"),
        ),
        (
            NativeToolProfile::Openai,
            Family::Responses,
            include_str!("golden/openai.json"),
        ),
        (
            NativeToolProfile::Google,
            Family::Gemini,
            include_str!("golden/google.json"),
        ),
    ] {
        let revisions = profile.tools()?;
        let mut contracts = NativeDeclarations::new(profile, None)?;
        let mut tools = Vec::new();
        for revision in &revisions {
            let core = match revision.capability.as_str() {
                "glob" | "search" => "shell",
                name => name,
            };
            let original = originals
                .get_shared(core, None, None)?
                .expect("stock capability")
                .clone();
            tools.push(contracts.bind_variant(revision, original)?.0);
        }
        let render = |tools: &[Tool]| -> Result<Vec<u8>> {
            let declarations = tools
                .iter()
                .map(|tool| {
                    tool.definition
                        .model_declaration(std::borrow::Cow::Borrowed(&tool.definition.name))
                })
                .collect();
            let declarations = ToolDeclarations::new(family, declarations)
                .map_err(|error| Error::Invalid(error.to_string()))?;
            serde_json::to_vec(&declarations).map_err(|error| Error::Invalid(error.to_string()))
        };
        let actual = render(&tools)?;
        assert_eq!(
            actual,
            golden.trim_end_matches('\n').as_bytes(),
            "{profile:?}"
        );
        // Ordering is part of the cache prefix and signature-bearing request.
        tools.reverse();
        assert_ne!(render(&tools)?, actual);
    }
    Ok(())
}
