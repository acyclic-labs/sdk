use super::*;
use acyclic_model_render::{Family, ToolDeclarations};

#[test]
fn pinned_family_tool_lists_preserve_public_contract_bytes() -> Result<()> {
    let mut originals = crate::tools::defaults()?;
    let selected_shell = originals.get_shared("shell", None, None)?.unwrap().clone();
    let selected_patch = originals
        .get_shared("apply_patch", None, None)?
        .unwrap()
        .clone();
    register_profiles(
        &mut originals,
        [
            NativeToolProfile::Google,
            NativeToolProfile::Anthropic,
            NativeToolProfile::Openai,
            NativeToolProfile::Anthropic,
        ],
    )?;
    assert!(Arc::ptr_eq(
        &selected_shell,
        originals.get_shared("shell", None, None)?.unwrap(),
    ));
    assert!(Arc::ptr_eq(
        &selected_patch,
        originals.get_shared("apply_patch", None, None)?.unwrap(),
    ));
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
        let mut tools = Vec::new();
        for revision in &revisions {
            tools.push(
                originals
                    .get_shared(&revision.variant, Some(&revision.revision), None)?
                    .expect("registered native capability")
                    .clone(),
            );
        }
        let render = |tools: &[Arc<Tool>]| -> Result<Vec<u8>> {
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
