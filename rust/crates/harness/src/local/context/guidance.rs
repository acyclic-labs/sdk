use super::*;
mod selection;
use selection::selection;
#[cfg(test)]
#[path = "../../../fuzz-context/fuzz_targets/file_guidance_input.rs"]
mod fuzz_input;

impl LocalHarness {
    pub(super) async fn file_guidance(&self, prompt: &str) -> Result<(bool, bool)> {
        let requested = selection(prompt, false, false);
        if requested == (true, true) {
            return Ok(requested);
        }
        // Both observations use the same canonical head. No goal body read,
        // metadata decode or content allocation is needed to select guidance.
        let workspace = crate::filesystem::workspace_ref(
            self.scratch.provider().clone(),
            &self.scratch.storage_name(&mut None)?,
        )?;
        let generation = self.files.resolve(&workspace, None).await?.generation;
        let mut present = [requested.0, requested.1];
        for (slot, (path, kind)) in present.iter_mut().zip([
            ("/GOAL.md", acyclic_fs::kernel::FileKind::Regular),
            ("/plans", acyclic_fs::kernel::FileKind::Directory),
        ]) {
            if *slot {
                continue;
            }
            match self.files.stat(&workspace, Some(&generation), path).await {
                Ok(stat) => *slot = stat.kind == kind,
                Err(Error::NotFound(_)) => {}
                Err(error) => return Err(error),
            }
        }
        Ok((present[0], present[1]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn file_guidance_corpus() -> Result<()> {
        let corpus =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("fuzz-context/corpus/file_guidance");
        for entry in std::fs::read_dir(corpus).map_err(storage)? {
            fuzz_input::check(&std::fs::read(entry.map_err(storage)?.path()).map_err(storage)?);
        }
        Ok(())
    }

    proptest! {
        // Arbitrary Unicode and mixed-case markers: file presence is monotonic,
        // and neither convention can enable the other.
        #[test]
        fn file_guidance_selection(prompt in ".{0,128}", goal in any::<bool>(), plans in any::<bool>()) {
            let lower = prompt.to_ascii_lowercase();
            let expected = (goal || lower.contains("goal.md") || lower.contains("goal mode"),
                plans || lower.contains("plans/") || lower.contains("plan mode"));
            prop_assert_eq!(selection(&prompt, goal, plans), expected);
            let goal_prompt = format!("{prompt} GOAL MODE");
            let plan_prompt = format!("{prompt} pLaNs/a.md");
            prop_assert!(selection(&goal_prompt, false, plans).0);
            prop_assert!(selection(&plan_prompt, goal, false).1);
        }
    }

    #[test]
    fn file_guidance_all_modes_and_controls() -> Result<()> {
        for (prompt, requested) in [
            ("hello", (false, false)),
            ("my goal is speed", (false, false)),
            ("GOAL MODE", (true, false)),
            ("Goal.md", (true, false)),
            ("PLAN MODE", (false, true)),
            ("PLANS/a.md", (false, true)),
            ("goal.md plans/a.md", (true, true)),
        ] {
            for goal in [false, true] {
                for plans in [false, true] {
                    let expected = (goal || requested.0, plans || requested.1);
                    let selected = selection(prompt, goal, plans);
                    assert_eq!(selected, expected);
                    let manifest = ContextManifest::for_files(
                        DiscoveredContext::default(),
                        Vec::new(),
                        &ToolRegistry::default(),
                        selected.0,
                        selected.1,
                    )?;
                    let actual = (
                        manifest
                            .conventions
                            .iter()
                            .any(|s| s.starts_with("Keep the current goal")),
                        manifest
                            .conventions
                            .iter()
                            .any(|s| s.starts_with("Keep each Markdown plan")),
                    );
                    assert_eq!(actual, expected);
                    // The previous shipping route coupled both conventions to
                    // goal presence. Run that route through the real constructor.
                    let mut legacy = ContextManifest::new(
                        DiscoveredContext::default(),
                        Vec::new(),
                        &ToolRegistry::default(),
                    )?;
                    if !selected.0 {
                        legacy.conventions.clear();
                    }
                    if expected.0 != expected.1 {
                        assert_ne!(legacy.conventions, manifest.conventions);
                    }
                    // Mutate the actual production constructor's inputs. The same
                    // detector rejects always-on guidance and goal-coupled plans.
                    for mutant in [(true, true), (selected.0, selected.0), (false, false)] {
                        if mutant == expected {
                            continue;
                        }
                        let broken = ContextManifest::for_files(
                            DiscoveredContext::default(),
                            Vec::new(),
                            &ToolRegistry::default(),
                            mutant.0,
                            mutant.1,
                        )?;
                        assert_ne!(broken.conventions, manifest.conventions);
                    }
                }
            }
        }
        Ok(())
    }
}
