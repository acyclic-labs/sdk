//! Capacity-bounded rolling summaries over complete dependency groups.
use super::*;

#[derive(serde::Deserialize, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
struct ManagerRewrite {
    version: u32,
    drop_through: u32,
    summary: String,
}

impl PreparationRecord for ManagerRewrite {}

#[derive(Serialize, Deserialize, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
struct ManagerWindow {
    version: u32,
    source: [u8; 32],
    start: u32,
    end: u32,
}
impl PreparationRecord for ManagerWindow {}

impl StockExecution<'_> {
    pub(super) fn manager_material(
        &self,
        context: &crate::context::Context,
        range: std::ops::Range<usize>,
        previous: Option<&crate::context::ContextSummary>,
        instruction: Option<&ModelContent>,
    ) -> Result<(crate::context::Context, ModelContent)> {
        let start = range.start;
        let (mut source, _) = with_history_io(self.io, |work| {
            crate::context::clone_context_range(context, range, work)
        })?;
        if let Some(previous) = previous {
            let file = crate::model::clone_file(&previous.output, self.io)?;
            stock_vec_reserve(&mut source.messages, 1, self.io)?;
            source.messages.insert(
                0,
                ModelMessage {
                    role: ModelRole::Assistant,
                    content: ModelContent::Part(crate::model::ModelContentPart::File {
                        file,
                        policy: crate::model::FileProjectionPolicy::BoundedFull,
                    }),
                },
            );
            source.current_input_index = source.current_input_index.map(|index| index + 1);
        }
        if let Some(instruction) = instruction {
            let instruction = with_history_io(self.io, |work| {
                crate::model::clone_content(instruction, work).map(|(content, _)| content)
            })?;
            return Ok((source, instruction));
        }
        let instruction = with_history_io(self.io, |work| {
            preparation_format(
                format_args!(
                    "{}\nSource count: {}.\nThe first {} message is the rolling summary of original messages [0,{start}); preserve its useful facts when incorporating the following complete source groups.",
                    crate::seed::CONTEXT_MANAGER,
                    source.messages.len(),
                    usize::from(previous.is_some()),
                ),
                work,
            )
        })?;
        Ok((source, ModelContent::Text(instruction)))
    }

    pub(super) async fn manager_window(
        &self,
        journal: &dyn ExecutionJournal,
        turn: (&TurnInput, u32),
        context: &crate::context::Context,
        selection: (usize, Option<&ModelContent>),
        progress: (u64, Option<&crate::context::ContextSummary>, [u8; 32]),
    ) -> Result<usize> {
        let (input, step) = turn;
        let (eligible, instruction) = selection;
        let (part, previous, digest) = progress;
        let start = previous.map_or(0, |summary| summary.source_messages as usize);
        let key = with_history_io(self.io, |work| {
            preparation_format(format_args!("manager:{step}:part:{part}:window"), work)
        })?;
        let located = journal
            .staged_ref(input.operation_id, &key, self.io)?
            .await?;
        match located {
            Some(file) => {
                let selected =
                    retain_preparation(load_json::<ManagerWindow>(journal, &file, self.io).await?)?;
                if selected.version != 1
                    || selected.source != digest
                    || selected.start as usize != start
                    || selected.end as usize > eligible
                    || (selected.end as usize) < start
                {
                    return Err(stock_boundary(
                        self.io,
                        crate::wire::ErrorCode::Conflict,
                        "context manager window differs from its original source",
                    ));
                }
                return Ok(selected.end as usize);
            }
            None => {}
        }
        let purpose = if part == 0 {
            ModelPurpose::Summary
        } else {
            ModelPurpose::SummaryPart(part)
        };
        let mut end = eligible;
        loop {
            // Probes leave no retained request or source allocation. Only the
            // selected immutable window escapes this preparation scope.
            let scope = crate::filesystem::PreparationScope::new(self.io)?;
            let session = self.preparing(scope.io());
            let material = session.manager_material(context, start..end, previous, instruction)?;
            let admitted = session
                .admit_summary_window(journal, turn, material.0, material.1, purpose)
                .await;
            drop(session);
            scope.finish(0)?;
            match admitted {
                Ok(()) => break,
                Err(Error::ModelContextOverflow) if end > start => {
                    let policy = crate::context::CapacityCompaction::default();
                    let first = crate::context::plan_compaction_budget(
                        &policy,
                        context,
                        None,
                        start + 1,
                        self.io,
                    )?
                    .0;
                    if first >= end {
                        if previous.is_some() {
                            end = start;
                            continue;
                        }
                        return Err(Error::ModelContextOverflow);
                    }
                    let middle = start + (end - start) / 2;
                    let selected = crate::context::plan_compaction_budget(
                        &policy,
                        context,
                        None,
                        middle.max(start + 1),
                        self.io,
                    )?
                    .0;
                    end = if selected < end { selected } else { first };
                }
                Err(error) => return Err(error),
            }
        }
        let selected = ManagerWindow {
            version: 1,
            source: digest,
            start: u32::try_from(start).map_err(|_| Error::ModelContextOverflow)?,
            end: u32::try_from(end).map_err(|_| Error::ModelContextOverflow)?,
        };
        stage_json_once(journal, input.operation_id, &key, &selected, self.io).await?;
        Ok(end)
    }

    pub(super) async fn managed_summary(
        &self,
        journal: &dyn ExecutionJournal,
        turn: (&TurnInput, u32),
        context: &crate::context::Context,
        policy: &crate::context::CapacityCompaction,
        selection: (usize, Option<usize>),
        progress: (u64, Option<&crate::context::ContextSummary>, [u8; 32]),
    ) -> Result<(crate::context::ContextSummary, usize, bool)> {
        let (input, step) = turn;
        let (eligible, message_limit) = selection;
        let (part, previous, _) = progress;
        let start = previous.map_or(0, |summary| summary.source_messages as usize);
        let end = self
            .manager_window(journal, turn, context, (eligible, None), progress)
            .await?;
        let scope = crate::filesystem::PreparationScope::new(self.io)?;
        let session = self.preparing(scope.io());
        let (source, instruction) =
            session.manager_material(context, start..end, previous, None)?;
        let supplied = source.messages.len();
        let purpose = if part == 0 {
            ModelPurpose::Summary
        } else {
            ModelPurpose::SummaryPart(part)
        };
        let mut phase = DispatchPhase::new();
        let (_, text) = session
            .summarize_part(
                journal,
                Some(input),
                input.operation_id,
                step,
                (source, instruction),
                &mut phase,
                purpose,
            )
            .await?;
        let invalid = |reason| stock_boundary(session.io, crate::wire::ErrorCode::Invalid, reason);
        let rewrite: ManagerRewrite = match session.io {
            Some(io) => retain_preparation({
                let (value, hold) = io.decode(
                    std::marker::PhantomData,
                    text.as_bytes(),
                    crate::wire::ErrorCode::Invalid,
                )?;
                (value, Some(hold))
            })?,
            None => crate::contract::json_from_slice(text.as_bytes())
                .map_err(|_| invalid("context manager returned an invalid rewrite"))?,
        };
        let selected = rewrite.drop_through as usize;
        if rewrite.version != 1 || selected == 0 || selected > supplied {
            return Err(invalid("context manager selected an invalid source range"));
        }
        let through = start + selected - usize::from(previous.is_some());
        if through == start
            && previous.is_none_or(|old| {
                rewrite.summary.len() as u64 >= old.output.descriptor().byte_length()
            })
        {
            return Err(invalid("context manager rewrite makes no progress"));
        }
        let (closed, maximum) = crate::context::plan_compaction_budget(
            policy,
            context,
            message_limit,
            through,
            session.io,
        )?;
        if closed != through {
            return Err(invalid("context manager cut splits a dependency group"));
        }
        let source_digest = crate::context::prefix_digest(context, through, session.io)?;
        let key = with_history_io(session.io, |work| {
            preparation_format(format_args!("summary:{step}:part:{part}:rewrite"), work)
        })?;
        let summary = session
            .publish_summary(
                journal,
                (input.operation_id, step),
                (through as u32, source_digest),
                (&key, rewrite.summary.as_bytes()),
            )
            .await?;
        drop(rewrite);
        drop(text);
        drop(key);
        drop(session);
        scope.finish(summary.output.logical_owned_bytes()?)?;
        Ok((summary, maximum, end < eligible))
    }
}
