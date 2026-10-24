use super::*;
use proptest::prelude::*;
use std::collections::VecDeque;

struct Pages(VecDeque<DiscoveryPage<u8, u8>>);
impl DiscoverySource for Pages {
    type Revision = u8;
    type Cursor = u8;
    fn page<'a>(
        &'a mut self,
        _: &'a str,
        _: Option<&'a u8>,
        _: Option<u8>,
        _: u32,
    ) -> BoxProviderFuture<'a, Result<DiscoveryPage<u8, u8>>> {
        Box::pin(async {
            self.0
                .pop_front()
                .ok_or_else(|| Error::Storage("page script exhausted".into()))
        })
    }
}
fn page(revision: u8, names: &[&str], next: Option<u8>) -> DiscoveryPage<u8, u8> {
    DiscoveryPage {
        revision,
        scanned: u32::try_from(names.len()).unwrap(),
        entries: names
            .iter()
            .map(|name| DiscoveryEntry {
                name: (*name).into(),
                kind: DiscoveryKind::File,
            })
            .collect(),
        next,
    }
}

#[tokio::test]
async fn directory_adapters_share_order_revision_duplicate_and_progress_fences() -> Result<()> {
    // Lazy sources may return a source phase followed by an authored phase,
    // including an empty page. The one walk orders the final combined result.
    let mut source = Pages(
        [
            page(1, &["z"], Some(1)),
            page(1, &[], Some(2)),
            page(1, &["a"], None),
        ]
        .into(),
    );
    let (_, entries) = DiscoveryWork::default()
        .list_source(&mut source, "", None)
        .await?;
    assert_eq!(
        entries.iter().map(|e| e.name.as_str()).collect::<Vec<_>>(),
        ["a", "z"]
    );
    for pages in [
        vec![page(1, &["a"], Some(1)), page(2, &["b"], None)],
        vec![page(1, &["a"], Some(1)), page(1, &["a"], None)],
        vec![page(1, &["../outside"], None)],
    ] {
        assert!(
            DiscoveryWork::default()
                .list_source(&mut Pages(pages.into()), "", None)
                .await
                .is_err()
        );
    }
    let mut work = DiscoveryWork {
        limits: ContextDiscoveryLimits {
            entries: 2,
            ..Default::default()
        },
        ..Default::default()
    };
    let mut no_progress = Pages(
        [
            page(1, &[], Some(1)),
            page(1, &[], Some(2)),
            page(1, &[], None),
        ]
        .into(),
    );
    assert!(work.list_source(&mut no_progress, "", None).await.is_err());
    assert_eq!(no_progress.0.len(), 1, "the third page must not be read");
    let mut omitted = page(1, &[], None);
    omitted.scanned = 2;
    let mut work = DiscoveryWork::default();
    work.list_source(&mut Pages([omitted].into()), "", None)
        .await?;
    assert_eq!(
        work.entries, 2,
        "opaque native names still consume the shared quota"
    );
    let mut understated = page(1, &["AGENTS.md"], None);
    understated.scanned = 0;
    assert!(
        DiscoveryWork::default()
            .list_source(&mut Pages([understated].into()), "", None)
            .await
            .is_err()
    );
    Ok(())
}

proptest! {
    #[test]
    fn cached_discovery_cost_obeys_the_same_combined_budget(
        a in 0u32..128, b in 0u32..128, bound in 1u32..128,
    ) {
        let mut work = DiscoveryWork { entries: a, limits: ContextDiscoveryLimits { entries: bound, ..Default::default() }, ..Default::default() };
        let used = DiscoveryWork { entries: b, ..Default::default() };
        let catalog = SkillCatalog::capture(Vec::new(), used, DiscoveryWork::default())?;
        let result = catalog.reuse(&mut work);
        prop_assert_eq!(result.is_ok(), a + b <= bound);
        if result.is_ok() {
            prop_assert_eq!(work.entries, a + b);
            let delta = work.since(DiscoveryWork { entries: a, ..Default::default() })?;
            prop_assert_eq!(delta.entries, b);
        } else {
            prop_assert_eq!(work.entries, a, "rejection must not partially charge");
        }
    }
}

#[test]
fn cached_discovery_cannot_bypass_invalid_limits() {
    for limits in [
        ContextDiscoveryLimits {
            entries: 0,
            ..Default::default()
        },
        ContextDiscoveryLimits {
            directories: 0,
            ..Default::default()
        },
        ContextDiscoveryLimits {
            header_bytes: 0,
            ..Default::default()
        },
        ContextDiscoveryLimits {
            instruction_bytes: 0,
            ..Default::default()
        },
    ] {
        let mut work = DiscoveryWork {
            limits,
            ..Default::default()
        };
        assert!(work.include(DiscoveryWork::default()).is_err());
        assert_eq!((work.entries, work.directories), (0, 0));
    }
}

impl InstructionReader for Pages {
    fn instruction<'a>(
        &'a mut self,
        _: &'a str,
        _: &'a u8,
        _: u64,
    ) -> BoxProviderFuture<'a, Result<crate::model::ProjectInstruction>> {
        panic!("non-file instruction must be rejected before a body read")
    }
}
impl SkillReader for Pages {
    fn skill_prefix<'a>(
        &'a mut self,
        _: &'a str,
        _: &'a u8,
        _: u32,
    ) -> BoxProviderFuture<'a, Result<(Vec<u8>, PinnedContextPath)>> {
        panic!("non-file skill must be rejected before a prefix read")
    }
}
#[tokio::test]
async fn unsupported_source_kinds_cannot_become_instructions_or_skills() {
    let entry = |name: &str, kind| DiscoveryPage {
        revision: 1,
        scanned: 1,
        entries: vec![DiscoveryEntry {
            name: name.into(),
            kind,
        }],
        next: None,
    };
    let mut instruction = Pages([entry("AGENTS.md", DiscoveryKind::Other)].into());
    assert!(matches!(
        capture_instructions(
            &mut instruction,
            "",
            "",
            &["AGENTS.md".into()],
            &mut DiscoveryWork::default(),
            None
        )
        .await,
        Err(Error::Invalid(_))
    ));
    let mut skill = Pages(
        [
            entry("inspect", DiscoveryKind::Directory),
            entry("SKILL.md", DiscoveryKind::Other),
        ]
        .into(),
    );
    assert!(matches!(
        discover_skills_from(
            &mut skill,
            "",
            "SKILL.md",
            &mut DiscoveryWork::default(),
            None,
            false
        )
        .await,
        Err(Error::Invalid(_))
    ));
}
