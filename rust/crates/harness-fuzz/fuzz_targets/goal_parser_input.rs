use acyclic_harness::{
    AgentId, Result,
    conversation::{FileDescriptor, FileRef, VolumeClass, VolumeOwner, VolumeRef},
    goal::{GoalStatus, GoalView},
    resources::ProviderRef,
};

const LIMIT: u64 = 8192;

fn file(bytes: &[u8]) -> Result<FileRef> {
    FileRef::new(
        VolumeRef::new(
            ProviderRef::new("fuzz".into(), "filesystem".into(), "1".into(), &mut None)?,
            "private".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(AgentId::from_bytes([1; 16])),
            &mut None,
        )?,
        "GOAL.md".into(),
        "v1".into(),
        FileDescriptor::from_bytes(bytes, "text/markdown", &mut None)?,
        "GOAL.md".into(),
        &mut None,
    )
}

fn round_trip(view: &GoalView, reason: Option<&str>) -> Result<()> {
    for status in [GoalStatus::Active, GoalStatus::Blocked, GoalStatus::Done] {
        let bytes = view.status_contents(status, reason)?;
        assert!(bytes.len() as u64 <= LIMIT);
        let rewritten = GoalView::from_file(file(bytes.as_bytes())?, bytes.as_bytes(), LIMIT)?;
        assert_eq!(rewritten.status, status);
        assert_eq!(rewritten.markdown, view.markdown);
        assert_eq!(rewritten.reason.as_deref(), reason);
    }
    Ok(())
}

pub fn check(data: &[u8]) -> Result<()> {
    if data.len() as u64 > LIMIT {
        return Ok(());
    }
    if let Ok(view) = GoalView::from_file(file(data)?, data, LIMIT) {
        round_trip(&view, view.reason.as_deref())?;
    }
    // Even malformed frontmatter exercises arbitrary Unicode reason encoding.
    if let Ok(text) = std::str::from_utf8(data) {
        let reason: String = text.chars().take(12).collect();
        let view = GoalView::from_file(file(b"Goal")?, b"Goal", LIMIT)?;
        round_trip(&view, Some(&reason))?;
    }
    Ok(())
}
