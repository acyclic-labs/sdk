//! Help is derived from the same followed files as dispatch and completion.
use super::{InputKeymap, ratatui::text::Line};
use crate::{
    Error, Result,
    client::{CommandFileRoot, CommandSession},
    task_volume::FileValue,
};
use acyclic_stream::StreamProvider;

#[derive(Default)]
pub(super) struct Help {
    source: Option<[Option<FileValue>; 2]>,
    pub(super) lines: Vec<Line<'static>>,
}

impl Help {
    pub(super) fn refresh<P: StreamProvider + ?Sized>(
        &mut self,
        files: &CommandSession<P>,
    ) -> Result<()> {
        let catalog = files.state_file(CommandFileRoot::Config, "commands.json")?;
        let keymap = files.state_file(CommandFileRoot::Config, "keymap.json")?;
        if self
            .source
            .as_ref()
            .is_some_and(|old| old[0].as_ref() == catalog && old[1].as_ref() == keymap)
        {
            return Ok(());
        }
        let map = keymap
            .map(|file| {
                let text = std::str::from_utf8(&file.bytes)
                    .map_err(|error| Error::Invalid(error.to_string()))?;
                InputKeymap::parse(text).map_err(|errors| Error::Invalid(format!("{errors:?}")))
            })
            .transpose()?
            .unwrap_or_default();
        let mut lines = vec![Line::raw("Commands")];
        for command in files.catalog().commands() {
            let aliases = command
                .aliases
                .iter()
                .map(|alias| format!(" /{alias}"))
                .collect::<String>();
            lines.push(Line::raw(
                format!("/{}{} · {}", command.name, aliases, command.description)
                    .escape_debug()
                    .to_string(),
            ));
        }
        lines.push(Line::raw("Keys"));
        lines.extend(
            map.help()
                .lines()
                .map(|line| Line::raw(line.replace('\t', " · "))),
        );
        self.lines = lines;
        self.source = Some([catalog.cloned(), keymap.cloned()]);
        Ok(())
    }
}
