use acyclic_harness::{
    task_volume::{FileKey, Query, TaskVolume, Write},
    tui::{
        Editor, PromptInput, Screen, Viewport,
        ratatui::{Terminal, backend::TestBackend},
    },
    tui_files::CompletionFiles,
};
use acyclic_stream::{MemoryStream, StreamPath};
use futures::FutureExt;
use std::{collections::BTreeSet, sync::Arc};

fn command(name: &str) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!([{"name":name}])).unwrap()
}

fn painted(input: &PromptInput) -> String {
    let mut terminal = Terminal::new(TestBackend::new(80, 20)).unwrap();
    terminal
        .draw(|frame| {
            Screen::default().render(frame, input, &mut Viewport::default(), 0, 0);
        })
        .unwrap();
    super::screen_text(&terminal)
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(24))]
    // Property: each publication updates the complete candidate set and cursor
    // atomically. A malformed later file preserves the painted popup and cut;
    // repairing that same publication yields its complete expected candidates.
    #[test]
    fn bulk_completion_publications_are_atomic_and_idempotent(
        sequence in proptest::collection::vec((0u8..10, proptest::bool::ANY, proptest::bool::ANY, proptest::bool::ANY), 1..5)
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async {
            let volume = TaskVolume::open(Arc::new(MemoryStream::default()), StreamPath::new("completion-batch").unwrap()).await.unwrap();
            let old = FileKey::parse("commands/old.json").unwrap();
            let current = FileKey::parse("commands/current.json").unwrap();
            let extra = FileKey::parse("commands/extra.json").unwrap();
            let cut = volume.snapshot(&Query::Inputs, 8).await.unwrap().generation;
            let mut generation = volume.append(cut, &Write::new(old.clone(), command("before"))).await.unwrap();
            let mut input = PromptInput::new(Editor::new("/"));
            let mut files = CompletionFiles::observe(&volume, &mut input, None, 8).await.unwrap();
            let mut old_present = true;
            // Replay every generated write immediately: an identical append is
            // not a publication and must neither wait nor mutate the projection.
            for (value, remove, malformed, primary) in sequence.into_iter().flat_map(|step| [step, step]) {
                let name = format!("item{value}");
                let extra_name = format!("extra{value}");
                let before = painted(&input);
                let cursor = files.cursor.clone();
                let previous = generation.clone();
                generation = volume.append(generation, &Write::new(current.clone(), command(&name))).await.unwrap();
                // Poll the accepted in-memory cut once, independently of the
                // enclosing task's cooperative budget; never wait for absent data.
                let ready = tokio::task::unconstrained(files.read_change()).now_or_never();
                if generation == previous {
                    proptest::prop_assert!(ready.is_none());
                    proptest::prop_assert_eq!(&files.cursor, &cursor);
                    proptest::prop_assert_eq!(painted(&input), before);
                    continue;
                }
                let mut change = ready.expect("an accepted in-memory publication must be immediately observable").unwrap();
                if !primary {
                    change.files.push(change.file.take().unwrap());
                }
                if remove { change.removed.push(old.clone()); }
                let valid = Write::new(extra.clone(), command(&extra_name));
                change.files.push(if malformed { Write::new(extra.clone(), b"[".to_vec()) } else { valid.clone() });
                let accepted = change.cursor.clone();
                if malformed {
                    proptest::prop_assert!(files.apply_change(change, &mut input).is_err());
                    proptest::prop_assert_eq!(painted(&input), before);
                    proptest::prop_assert_eq!(&files.cursor, &cursor);
                    change = acyclic_harness::task_volume::Change {
                        file: Some(Write::new(current.clone(), command(&name))),
                        files: vec![valid],
                        removed: if remove { vec![old.clone()] } else { vec![] },
                        cursor: accepted.clone(),
                    };
                }
                files.apply_change(change, &mut input).unwrap();
                proptest::prop_assert_eq!(&files.cursor, &accepted);
                proptest::prop_assert_ne!(&files.cursor, &cursor);
                old_present &= !remove;
                let mut expected = BTreeSet::from([name.clone(), extra_name.clone()]);
                if old_present { expected.insert("before".into()); }
                let actual: BTreeSet<_> = input.completion.items().map(|item| item.value.clone()).collect();
                proptest::prop_assert_eq!(actual, expected.clone());
                let picture = painted(&input);
                for name in expected { proptest::prop_assert!(picture.contains(&name)); }
            }
            Ok(())
        })?;
    }
}
