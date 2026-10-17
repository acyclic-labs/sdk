//! Saved history remains a bounded, maximal recent suffix across reopen.
use acyclic_harness::{
    OperationId,
    client::{CommandFileRoot, CommandSession, PromptFiles},
    task_volume::TaskVolume,
    tui::{PromptHistory, PromptInput, Screen},
};
use acyclic_stream::{MemoryStream, StreamPath};
use proptest::prelude::*;
use std::sync::Arc;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(12))]
    // Property: serialized bytes stay within the real session limit; retained
    // history is the maximal suffix, remains identical after reopen and another
    // save, and can still be recalled and rendered through TestBackend.
    #[test]
    fn saved_history_keeps_the_maximal_suffix_and_remains_usable(
        lengths in prop::collection::vec(3000usize..40000, 6..10),
        atom in prop::sample::select(vec!["a", "界", "\"", "\\", "\n"]),
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async {
            const LIMIT: usize = 65_536;
            let volume = Arc::new(TaskVolume::open(Arc::new(MemoryStream::default()), StreamPath::new("history-bound").unwrap()).await.unwrap());
            let id = OperationId::new().to_string().parse().unwrap();
            let mut files = CommandSession::open(volume.clone(), id, LIMIT as u64, 128).await.unwrap();
            let budget = files.state_byte_limit(CommandFileRoot::Config, "history.json").unwrap();
            prop_assert!(budget <= LIMIT);
            let mut projection = PromptFiles::default();
            let mut input = PromptInput::default();
            projection.refresh(&files, &mut input).unwrap();
            let mut history = PromptHistory::default();
            for (index, length) in lengths.iter().enumerate() {
                history.push(format!("entry {index}: {}", atom.repeat(*length)));
            }
            let all = history.entries().to_vec();
            input.replace_history(history);
            projection.save(&mut files, &mut input).await.unwrap();
            let saved = files.state_file(CommandFileRoot::Config, "history.json").unwrap().unwrap();
            prop_assert!(saved.bytes.len() <= budget);
            let retained: Vec<String> = serde_json::from_slice(&saved.bytes).unwrap();
            let start = all.len() - retained.len();
            prop_assert_eq!(&retained, &all[start..]);
            if start > 0 {
                prop_assert!(serde_json::to_vec(&all[start - 1..]).unwrap().len() > budget);
            }
            prop_assert_eq!(input.history().entries(), retained.as_slice());
            projection.save(&mut files, &mut input).await.unwrap();
            let mut reopened = CommandSession::open(volume, id, LIMIT as u64, 128).await.unwrap();
            let mut replay = PromptInput::default();
            let mut replay_files = PromptFiles::default();
            replay_files.refresh(&reopened, &mut replay).unwrap();
            prop_assert_eq!(replay.history().entries(), retained.as_slice());
            let mut next = PromptHistory::default();
            for text in replay.history().entries() { next.push(text.clone()); }
            next.push("newest submission".into());
            replay.replace_history(next);
            replay_files.save(&mut reopened, &mut replay).await.unwrap();
            let saved = reopened.state_file(CommandFileRoot::Config, "history.json").unwrap().unwrap();
            prop_assert!(saved.bytes.len() <= budget);
            prop_assert_eq!(replay.history().entries().last().map(String::as_str), Some("newest submission"));
            let mut navigation = replay.history().clone();
            replay.editor.set_text(&navigation.previous("").unwrap(), 0);
            let mut terminal = super::Terminal::new(super::TestBackend::new(80, 20)).unwrap();
            terminal.draw(|frame| { Screen::default().render(frame, &replay, &mut super::Viewport::default(), 0, 0); }).unwrap();
            prop_assert!(super::screen_text(&terminal).contains("newest submission"));
            Ok(())
        })?;
    }
}
