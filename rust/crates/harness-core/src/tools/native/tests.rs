use super::*;
use acyclic_model_render::{Family, ToolDeclarations};

fn render(family: Family, tools: &[ToolDefinition]) -> Result<Vec<u8>> {
    let declarations = tools
        .iter()
        .map(|tool| tool.model_declaration(std::borrow::Cow::Borrowed(tool.name.as_str())))
        .collect();
    let declarations = ToolDeclarations::new(family, declarations)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    serde_json::to_vec(&declarations).map_err(|error| Error::Invalid(error.to_string()))
}

#[test]
fn pinned_family_tool_lists_preserve_public_contract_bytes() -> Result<()> {
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
        let mut tools = definitions(profile)?;
        let actual = render(family, &tools)?;
        assert_eq!(
            actual,
            golden.trim_end_matches('\n').as_bytes(),
            "{profile:?}"
        );
        // Ordering is part of the cache prefix and signature-bearing request.
        tools.reverse();
        assert_ne!(render(family, &tools)?, actual);
    }
    Ok(())
}
