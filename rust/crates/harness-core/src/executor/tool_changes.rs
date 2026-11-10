//! Shared dynamic tool announcements for every tool source.
use super::*;

impl StockExecution<'_> {
    pub(super) fn tools_notice(
        &self,
        context: &mut crate::context::Context,
        tools: &[crate::tool::ToolDefinition],
        retained_tables: bool,
    ) -> Result<()> {
        if !retained_tables
            && !self.stock.tools.has_sources()
            && !self.code_composition(tools)?
            && !context.messages.iter().any(|message| {
                message.content.notice_parts().is_some_and(|(notices, _)| {
                    notices
                        .iter()
                        .any(|notice| notice.source == "tools" && notice.kind == "tools.changed")
                })
            })
        {
            return Ok(());
        }
        crate::model::charge_items(self.io, tools.len() as u64)?;
        // Native declarations already expose these exact call schemas. Only
        // dynamic tools use the appended surface for every tool source.
        struct DynamicTools<'a>(&'a [crate::tool::ToolDefinition]);
        impl Serialize for DynamicTools<'_> {
            fn serialize<S: serde::Serializer>(
                &self,
                serializer: S,
            ) -> std::result::Result<S::Ok, S::Error> {
                use serde::ser::SerializeSeq;
                let mut sequence = serializer.serialize_seq(None)?;
                for tool in self.0 {
                    if !crate::tools::catalog::is_native_declaration(&tool.name) {
                        sequence.serialize_element(tool)?;
                    }
                }
                sequence.end()
            }
        }
        let has_dynamic = tools
            .iter()
            .any(|tool| !crate::tools::catalog::is_native_declaration(&tool.name));
        let tools = DynamicTools(tools);
        #[derive(Serialize)]
        struct Body<'a> {
            offset: usize,
            json: &'a str,
            complete: bool,
            removed: &'a [String],
        }
        #[derive(Serialize)]
        struct Notice<'a> {
            source: &'static str,
            kind: &'static str,
            body: Body<'a>,
            wake: bool,
            coalesce_key: Option<&'static str>,
        }
        #[derive(Serialize)]
        struct Content<'a> {
            kind: &'static str,
            notices: [Notice<'a>; 1],
        }
        fn content<'a>(
            json: &'a str,
            offset: usize,
            complete: bool,
            removed: &'a [String],
        ) -> Content<'a> {
            Content {
                kind: "notices",
                notices: [Notice {
                    source: "tools",
                    kind: "tools.changed",
                    body: Body {
                        offset,
                        json,
                        complete,
                        removed,
                    },
                    wake: false,
                    coalesce_key: None,
                }],
            }
        }
        let invalid = |reason| stock_boundary(self.io, crate::wire::ErrorCode::Invalid, reason);
        let table = if has_dynamic {
            #[cfg(feature = "filesystem")]
            let funded = self
                .io
                .map(|io| io.encode(&tools, u64::MAX, false))
                .transpose()?;
            #[cfg(not(feature = "filesystem"))]
            let funded: Option<acyclic_stream::OwnedBytes> = None;
            Some(match funded {
                Some(bytes) => bytes,
                None => serde_json::to_vec(&tools)
                    .map(Into::into)
                    .map_err(|_| invalid("tool table encoding failed"))?,
            })
        } else {
            None
        };
        #[cfg(feature = "filesystem")]
        let _table_hold = self
            .io
            .zip(table.as_ref())
            .map(|(io, table)| io.guard_bytes(table))
            .transpose()?;
        // An empty update revokes the last dynamic table without inspecting
        // history. Its borrowed bytes avoid encoding native definitions.
        let table = match table.as_deref() {
            Some(table) => {
                std::str::from_utf8(table).map_err(|_| invalid("tool table is not UTF-8"))?
            }
            None => "[]",
        };
        let mut previous = String::new();
        for message in &context.messages {
            if let Some((notices, _)) = message.content.notice_parts() {
                for notice in notices {
                    if notice.kind == "tools.changed" {
                        if notice.body.get("offset").and_then(Value::as_u64) == Some(0) {
                            previous.clear();
                        }
                        if let Some(fragment) = notice.body.get("json").and_then(Value::as_str) {
                            stock_string_push(
                                &mut previous,
                                fragment,
                                u64::MAX,
                                "prior tool table size overflow",
                                self.io,
                            )?;
                        }
                    }
                }
            }
        }
        if previous == table {
            return Ok(());
        }
        let decoded;
        let previous = if previous.is_empty() {
            self.stock
                .epoch_tools
                .as_deref()
                .map(Vec::as_slice)
                .unwrap_or_default()
        } else {
            #[cfg(feature = "filesystem")]
            let funded = self
                .io
                .map(|io| {
                    io.decode(
                        std::marker::PhantomData::<Vec<crate::tool::ToolDefinition>>,
                        previous.as_bytes(),
                        crate::wire::ErrorCode::Invalid,
                    )
                })
                .transpose()?;
            #[cfg(not(feature = "filesystem"))]
            let funded: Option<(
                Vec<crate::tool::ToolDefinition>,
                crate::conversation::ContentHold<'_>,
            )> = None;
            decoded = match funded {
                Some((table, hold)) => {
                    hold.into_retained()?;
                    table
                }
                None => serde_json::from_str(&previous)
                    .map_err(|_| invalid("invalid prior tool notice"))?,
            };
            decoded.as_slice()
        };
        let mut removed = Vec::new();
        for old in previous {
            if !tools.0.iter().any(|tool| tool.name == old.name) {
                stock_vec_push(
                    &mut removed,
                    stock_string_copy(&old.name, self.io)?,
                    self.io,
                )?;
            }
        }
        crate::model::charge_items(self.io, table.len() as u64)?;
        // Charge actual JSON escaping, with worst-case offset and completion
        // framing. A schema larger than one part is split without truncation.
        let overhead = with_history_io(self.io, |work| {
            json::json_byte_length(&content("", usize::MAX, false, &[]), u64::MAX, work)
        })?;
        let maximum = self
            .stock
            .limits
            .render_bytes
            .checked_sub(overhead)
            .ok_or_else(|| invalid("render allowance cannot frame a tool notice"))?;
        let index = context.messages.len();
        let mut pending = Vec::new();
        let mut offset = 0;
        let mut removed_offset = 0;
        while offset < table.len() || removed_offset < removed.len() {
            let remaining = &table[offset..];
            let mut bytes = 0;
            let removed_start = removed_offset;
            while let Some(name) = removed.get(removed_offset) {
                let size =
                    with_history_io(self.io, |work| json::json_byte_length(name, u64::MAX, work))?
                        + u64::from(removed_offset != removed_start);
                if size > maximum - bytes {
                    break;
                }
                bytes += size;
                removed_offset += 1;
            }
            let mut end = 0;
            for (index, character) in remaining.char_indices() {
                let size = match character {
                    '"' | '\\' | '\n' | '\r' | '\t' | '\u{08}' | '\u{0c}' => 2,
                    character if character < '\u{20}' => 6,
                    character => character.len_utf8() as u64,
                };
                if size > maximum - bytes {
                    break;
                }
                bytes += size;
                end = index + character.len_utf8();
            }
            if end == 0 && removed_offset == removed_start {
                return Err(invalid("render allowance cannot advance a tool notice"));
            }
            let notice = content(
                &remaining[..end],
                offset,
                offset + end == table.len() && removed_offset == removed.len(),
                &removed[removed_start..removed_offset],
            );
            #[cfg(feature = "filesystem")]
            let funded = self
                .io
                .map(|io| io.encode(&notice, u64::MAX, false))
                .transpose()?;
            #[cfg(not(feature = "filesystem"))]
            let funded: Option<acyclic_stream::OwnedBytes> = None;
            let bytes = match funded {
                Some(bytes) => bytes,
                None => serde_json::to_vec(&notice)
                    .map(Into::into)
                    .map_err(|_| invalid("tool notice encoding failed"))?,
            };
            #[cfg(feature = "filesystem")]
            let _bytes_hold = self.io.map(|io| io.guard_bytes(&bytes)).transpose()?;
            #[cfg(feature = "filesystem")]
            let decoded = self
                .io
                .map(|io| {
                    io.decode(
                        std::marker::PhantomData::<ModelContent>,
                        &bytes,
                        crate::wire::ErrorCode::Invalid,
                    )
                })
                .transpose()?;
            #[cfg(not(feature = "filesystem"))]
            let decoded: Option<(ModelContent, crate::conversation::ContentHold<'_>)> = None;
            let decoded = match decoded {
                Some((message, hold)) => (message, Some(hold)),
                None => (
                    serde_json::from_slice(&bytes)
                        .map_err(|_| invalid("tool notice decoding failed"))?,
                    None,
                ),
            };
            let decoded = (
                ModelMessage {
                    role: ModelRole::User,
                    content: decoded.0,
                },
                decoded.1,
            );
            stock_vec_push(&mut pending, retain_preparation(decoded)?, self.io)?;
            offset += end;
        }
        if pending.is_empty() {
            return Ok(());
        }
        // Append one batch independently of part count.
        crate::context::ContextInsertion::insert_batch(
            stock_into_iter(pending, self.io)?,
            index,
            context,
            0,
            self.io,
        )
    }
}
