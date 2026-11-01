use super::*;
use crate::tool::ToolRegistry;

/// Adds each catalog-selected family's revisions during ordinary cold registry
/// assembly. Existing selections are preserved; exact model selection, grants
/// and role availability remain separate admissions. Native retained activation
/// uses the original funded constructor and registration boundary instead.
pub fn register_profiles(
    registry: &mut ToolRegistry,
    profiles: impl IntoIterator<Item = NativeToolProfile>,
) -> Result<()> {
    let mut selected = [false; 3];
    for profile in profiles {
        selected[index(profile)] = true;
    }
    if selected == [false; 3] {
        return Ok(());
    }
    // This fixed set is independent of model, agent and revision counts. No
    // definition or executor payload is cloned by these original Arc references.
    let originals = ["read", "write", "edit", "shell", "apply_patch"].map(|name| {
        registry
            .get_shared(name, None, None)
            .map(|tool| tool.cloned())
    });
    let [read, write, edit, shell, patch] = originals;
    let originals = [read?, write?, edit?, shell?, patch?];
    for profile in [
        NativeToolProfile::Anthropic,
        NativeToolProfile::Openai,
        NativeToolProfile::Google,
    ] {
        if !selected[index(profile)] {
            continue;
        }
        let mut declarations = NativeDeclarations::new(profile, None)?;
        for revision in profile.tools()? {
            let original = match revision.capability.as_str() {
                "read" => &originals[0],
                "write" => &originals[1],
                "edit" if profile == NativeToolProfile::Openai => &originals[4],
                "edit" => &originals[2],
                "shell" | "glob" | "search" => &originals[3],
                _ => return Err(Error::Invalid("unknown native profile capability".into())),
            }
            .as_ref()
            .ok_or_else(|| Error::Invalid(format!(
                "native capability {} has no original executor; recovery: bind the capability before admitting its model profile",
                revision.capability
            )))?;
            let previous = registry.get_shared(&revision.variant, None, None)?.cloned();
            let tool = declarations.bind_variant(&revision, Arc::clone(original))?;
            registry.register(tool, &mut None, None)?;
            if let Some(previous) = previous {
                registry.select_model_version(
                    &previous.definition.name,
                    &previous.definition.revision,
                )?;
            }
        }
    }
    Ok(())
}

const fn index(profile: NativeToolProfile) -> usize {
    match profile {
        NativeToolProfile::Anthropic => 0,
        NativeToolProfile::Openai => 1,
        NativeToolProfile::Google => 2,
    }
}
