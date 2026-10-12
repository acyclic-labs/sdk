pub(super) const HELP: &str = "graphcoder --session CONFIG.json [--headless]\n\
    Actual local Filesystem/Stream task runtime; explicit deterministic model.\n\
    Native session requires terminal,filesystem-local; full fixture/writeback adds native-execution.\n\
    graphcoder --prepare-scenario ABSOLUTE_DIRECTORY  Create genuine local qualification providers\n\
    graphcoder --fixture wire [--headless]  Explicit presentation playback only.\n\
    TEXT        Queue a stock root/selected-parent task, without starting it\n\
    /parent TASK|root  Select an actual retained task as parent, or root\n\
    /queue FILE Queue a typed WorkflowCommand JSON with fresh admission identity\n\
    /command FILE Run/reconcile an explicitly configured approved native effect\n\
    /context FILE Capture discovery and replace typed attributes for later requests\n\
    /fork-selection FILE Select full/fresh/summary from an authenticated original cut\n\
    /prepare FILE Admit exact typed fork ResourcePreparationRequest JSON under its caller\n\
    /publish-fork FILE Publish actual report/admit exact child: caller,lease,preparation\n\
    /inspect FILE Inspect authorized source allocations: caller,lease,source\n\
    /history FILE Page owned history: caller,lease,cursor (16 events/64 KiB)\n\
    /inherited-history FILE Page verified inherited slice: caller,lease,seed,after_sequence\n\
    /checkpoint FILE Observe real machine checkpoint: caller,lease\n\
    /run TASK   Drive one finite ordinary worker turn for this exact task\n\
    /claim TASK Retain/start an actual original owner lease without model dispatch\n\
    /resume FILE Reconcile the original WorkLease JSON; never guess a new lease\n\
    /reconcile OPERATION  Observe a lost admission acknowledgement, without resend\n\
    /outcome TASK Observe the durable task result and distinct admission identity\n\
    /children FILE  Page direct children from task/revision/after JSON (max 16)\n\
    /mail FILE  Send one scoped pinned file under an actual caller lease\n\
    /inbox FILE Observe one finite inbox page; never claim model consumption\n\
    /queue-mailbox FILE Queue ordinary receive/checkpoint/model workflow\n\
    /write FILE Stage bounded content in an explicit caller-owned volume\n\
    /read FILE  Read exact pinned content: caller,lease,file (max 64 KiB)\n\
    /read-path FILE Read granted head/explicit cut: caller,lease,volume,path,generation? (max 64 KiB)\n\
    /publication-open FILE Prepare original SDK plan: operation_id,caller_lease\n\
    /publication-observe FILE Observe retained publication/approval journal\n\
    /publication-apply FILE Publish only the actual approved original plan\n\
    /publication-reconcile FILE Reconcile the original publication, never replan\n\
    /next       Display one bounded activity delivery\n\
    /cursor FILE Select a protobuf Resume ClientFrame for bounded archival paging\n\
    /back N     Show a retained display page (0 newest; 64 pages/256 KiB max)\n\
    /file FILE  Display scoped immutable content: caller,lease,file (max 64 KiB)\n\
    /diff FILE  Display scoped exact refs: caller,lease,before,after (max 32 KiB each)\n\
    /request FILE Dispatch one scoped protobuf ClientFrame; no automatic retries\n\
    /input FILE Dispatch a prepared conversation-message command\n\
    /approve FILE Dispatch an exact scoped interaction resolution command\n\
    /cancel FILE Dispatch an exact scoped protobuf Cancel ClientFrame\n\
    /cancel-task TASK  Request cancellation through the actual typed task host\n\
    /reopen     Drop/reopen actual local providers; do not execute queued work\n\
    /reset      Reset fixture display only (unavailable for native sessions)\n\
    /help       Display these commands\n\
    /quit       Exit without claiming pending work completed or cancelled\n\
    Live models and optional remote capabilities require explicit adapters.\n";

pub(super) enum Selection {
    Fixture,
    Native(std::path::PathBuf),
    Scenario(std::path::PathBuf),
}

pub(super) fn parse() -> acyclic_harness::Result<Option<(Selection, bool)>> {
    use acyclic_harness::Error;
    let mut selection = None;
    let mut headless = false;
    let mut arguments = std::env::args_os().skip(1);
    while let Some(argument) = arguments.next() {
        match argument.to_str() {
            Some("--help" | "-h") => {
                print!("{HELP}");
                return Ok(None);
            }
            Some("--headless") if !headless => headless = true,
            Some("--fixture") if selection.is_none() => {
                if arguments.next().as_deref() != Some(std::ffi::OsStr::new("wire")) {
                    return Err(Error::Invalid(
                        "only the explicit wire fixture is available".into(),
                    ));
                }
                selection = Some(Selection::Fixture);
            }
            Some("--session") if selection.is_none() => {
                let path = arguments
                    .next()
                    .ok_or_else(|| Error::Invalid("session config path missing".into()))?;
                selection = Some(Selection::Native(path.into()));
            }
            Some("--prepare-scenario") if selection.is_none() => {
                let path = arguments
                    .next()
                    .ok_or_else(|| Error::Invalid("scenario directory missing".into()))?;
                selection = Some(Selection::Scenario(path.into()));
            }
            _ => {
                return Err(Error::Invalid(
                    "unknown or duplicate terminal option".into(),
                ));
            }
        }
    }
    let selection = selection.ok_or_else(|| Error::Unsupported(
        "durable terminal host is unavailable without explicit --session CONFIG; use --fixture wire for UI playback only".into()))?;
    Ok(Some((selection, headless)))
}
