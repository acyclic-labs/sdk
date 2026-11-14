//! Paged hook names from original layers; declarations remain demand-only.
use super::*;
use crate::conversation::PrivateDirectoryEntryKind;

/// Continuation in the fixed semantic layer stack, never a hook-count limit.
/// The journaled operation retains every selected generation; this position
/// grants no read authority and carries the producer's opaque directory token.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HookCatalogueCursor {
    version: u32,
    source: u8,
    after: Option<Vec<u8>>,
}

/// Only visible directory names and the next bounded source query.
pub struct HookCataloguePage<'a> {
    /// Names may occur in several layers. Their effective program is resolved
    /// only when selected, through the same highest-precedence module reader.
    names: Vec<String>,
    /// Next original source position; absence proves the layer stack is done.
    next: Option<HookCatalogueCursor>,
    _metadata: Option<crate::filesystem::PreparationCustody>,
    _original: Option<ContentHold<'a>>,
    _cut: crate::layers::RetainedContentCut<'a>,
}

impl HookCataloguePage<'_> {
    /// Borrow visible names while their original producer credit remains live.
    pub fn names(&self) -> &[String] {
        &self.names
    }
    /// Serialize this position into the consuming actor's own checkpoint before
    /// retiring the page. Its opaque token cannot outlive its producer credit.
    pub fn next(&self) -> Option<&HookCatalogueCursor> {
        self.next.as_ref()
    }
}

impl ProgramFiles {
    /// List names only, retaining the original home/issuer generations before
    /// the first page. `operation` belongs to the catalogue's original source
    /// event. A later event starts a new operation; recovery keeps this cut.
    pub fn catalogue_page<'a, 'io: 'a>(
        &'a self,
        journal: &'a dyn crate::executor::ExecutionJournal,
        operation: crate::OperationId,
        cursor: Option<&'a HookCatalogueCursor>,
        maximum_entries: u32,
        io: Option<&'io PreparationIo>,
    ) -> Result<BoundedContentFuture<'a, HookCataloguePage<'io>>> {
        BoundedContentFuture::new(io, || async move {
            if maximum_entries == 0
                || cursor.is_some_and(|cursor| cursor.version != 1 || cursor.source > 5)
            {
                return Err(crate::executor::stock_boundary(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "invalid hook catalogue page",
                ));
            }
            let cut = crate::layers::ContentSources {
                reader: self.reader.as_ref(),
                seed: None,
                home: &self.home,
                configuration: &self.configuration,
                maximum_bytes: self.maximum_bytes,
                io,
            }
            .retain(journal, operation, "program:hooks:catalogue", "hooks")
            .await?;
            let attempt = io.map(PreparationIo::scoped).transpose()?;
            let temporary = attempt.as_ref();
            let mut source = cursor.map_or(0, |cursor| cursor.source);
            let mut after = cursor.and_then(|cursor| cursor.after.as_deref());
            let mut names = Vec::new();
            let mut original = None;
            let next = loop {
                if source == 5 {
                    // The built-in seed is a sorted immutable file table. Seek
                    // past one whole child subtree rather than scanning files.
                    let after = after.map(std::str::from_utf8).transpose().map_err(|_| {
                        crate::executor::stock_boundary(
                            io,
                            crate::wire::ErrorCode::Invalid,
                            "invalid seed catalogue continuation",
                        )
                    })?;
                    if let Some(after) = after {
                        crate::executor::with_history_io(io, |work| {
                            crate::conversation::validate_content_path(after, work)
                        })?;
                        if after.contains('/') {
                            return Err(crate::executor::stock_boundary(
                                io,
                                crate::wire::ErrorCode::Invalid,
                                "seed catalogue continuation is not one child name",
                            ));
                        }
                    }
                    let files = crate::seed::tree();
                    let mut at = files.partition_point(|(path, _)| *path < "hooks/");
                    let mut more = false;
                    while let Some((path, _)) =
                        files.get(at).filter(|(path, _)| path.starts_with("hooks/"))
                    {
                        let Some((name, _)) = path[6..].split_once('/') else {
                            at += 1;
                            continue;
                        };
                        if after.is_none_or(|after| name > after)
                            && names.len() == maximum_entries as usize
                        {
                            more = true;
                            break;
                        }
                        if after.is_none_or(|after| name > after) {
                            let selected = crate::executor::stock_string_copy(name, temporary)?;
                            crate::executor::stock_vec_push(&mut names, selected, temporary)?;
                        }
                        at += files[at..].partition_point(|(path, _)| {
                            path.strip_prefix("hooks/")
                                .and_then(|path| path.split_once('/'))
                                .is_some_and(|(child, _)| child == name)
                        });
                    }
                    let next = if more {
                        let mut token = Vec::new();
                        for byte in names
                            .last()
                            .ok_or_else(|| Error::Invalid("empty catalogue continuation".into()))?
                            .as_bytes()
                        {
                            crate::executor::stock_vec_push(&mut token, *byte, temporary)?;
                        }
                        Some(HookCatalogueCursor {
                            version: 1,
                            source,
                            after: Some(token),
                        })
                    } else {
                        None
                    };
                    break next;
                }
                let selected = if source == 0 {
                    Some((&cut.selection.home.0, &cut.selection.home.1))
                } else {
                    cut.selection.configuration[4 - usize::from(source)]
                        .as_ref()
                        .map(|layer| (&layer.volume, &layer.generation))
                };
                let Some((volume, generation)) = selected else {
                    source += 1;
                    after = None;
                    continue;
                };
                let page = self
                    .reader
                    .list_private_directory(
                        volume,
                        "",
                        "hooks",
                        Some(generation),
                        after,
                        maximum_entries,
                        None,
                        io,
                    )?
                    .await?;
                let Some((mut page, hold)) = page else {
                    if after.is_some() {
                        return Err(crate::executor::stock_boundary(
                            io,
                            crate::wire::ErrorCode::Conflict,
                            "hook catalogue continuation lost its original directory",
                        ));
                    }
                    source += 1;
                    after = None;
                    continue;
                };
                page.validate_with_io(io)?;
                if &page.generation != generation || page.entries.len() > maximum_entries as usize {
                    return Err(crate::executor::stock_boundary(
                        io,
                        crate::wire::ErrorCode::Conflict,
                        "hook directory page changed its selected source",
                    ));
                }
                let next = match page.next.take() {
                    Some(after) => Some(HookCatalogueCursor {
                        version: 1,
                        source,
                        after: Some(after),
                    }),
                    None => Some(HookCatalogueCursor {
                        version: 1,
                        source: source + 1,
                        after: None,
                    }),
                };
                for entry in page.entries {
                    if entry.kind == PrivateDirectoryEntryKind::Directory {
                        crate::executor::stock_vec_push(&mut names, entry.name, temporary)?;
                    }
                }
                original = hold;
                break next;
            };
            let metadata = temporary
                .map(PreparationIo::take_all_owned_custody)
                .transpose()?;
            Ok(HookCataloguePage {
                names,
                next,
                _metadata: metadata,
                _original: original,
                _cut: cut,
            })
        })
    }
}
