//! Original-position tool bindings for every tool source.
use super::*;

impl StockExecution<'_> {
    pub(super) fn tools_notice(
        &self,
        context: &mut crate::context::Context,
        tools: &[crate::tool::ToolDefinition],
    ) -> Result<()> {
        let compose = self.code_composition(tools)?;
        let mut pending = self.tool_table_notice(context, tools, compose, true)?;
        let dynamic = self.tool_table_notice(context, tools, compose, false)?;
        for message in stock_into_iter(dynamic, self.io)? {
            stock_vec_push(&mut pending, message, self.io)?;
        }
        if pending.is_empty() {
            return Ok(());
        }
        // Both tables publish as one preparation, even when framing fails.
        crate::context::ContextInsertion::insert_batch(
            stock_into_iter(pending, self.io)?,
            context.messages.len(),
            context,
            0,
            self.io,
        )
    }

    fn tool_table_notice(
        &self,
        context: &crate::context::Context,
        tools: &[crate::tool::ToolDefinition],
        compose: bool,
        native: bool,
    ) -> Result<Vec<ModelMessage>> {
        crate::model::charge_items(self.io, tools.len() as u64)?;
        // Native bindings are private validation metadata, never another copy
        // of the provider header in the prompt. Dynamic tables remain visible.
        let kind = if native {
            "tools.bound"
        } else {
            "tools.changed"
        };
        let selected = |tool: &&crate::tool::ToolDefinition| {
            native == (!compose || crate::tools::catalog::is_native_declaration(&tool.name))
        };
        struct Table<'a> {
            tools: &'a [crate::tool::ToolDefinition],
            compose: bool,
            native: bool,
        }
        impl Serialize for Table<'_> {
            fn serialize<S: serde::Serializer>(
                &self,
                serializer: S,
            ) -> std::result::Result<S::Ok, S::Error> {
                use serde::ser::SerializeSeq;
                let mut sequence = serializer.serialize_seq(None)?;
                for tool in self.tools {
                    if self.native
                        == (!self.compose
                            || crate::tools::catalog::is_native_declaration(&tool.name))
                    {
                        sequence.serialize_element(tool)?;
                    }
                }
                sequence.end()
            }
        }
        let has_tools = tools.iter().any(|tool| selected(&tool));
        let table_tools = Table {
            tools,
            compose,
            native,
        };
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
            kind: &'static str,
            json: &'a str,
            offset: usize,
            complete: bool,
            removed: &'a [String],
        ) -> Content<'a> {
            Content {
                kind: "notices",
                notices: [Notice {
                    source: "tools",
                    kind,
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
        let table = if has_tools {
            #[cfg(not(target_arch = "wasm32"))]
            let funded = self
                .io
                .map(|io| io.encode(&table_tools, u64::MAX, false))
                .transpose()?;
            #[cfg(target_arch = "wasm32")]
            let funded: Option<acyclic_stream::OwnedBytes> = None;
            Some(match funded {
                Some(bytes) => bytes,
                None => serde_json::to_vec(&table_tools)
                    .map(Into::into)
                    .map_err(|_| invalid("tool table encoding failed"))?,
            })
        } else {
            None
        };
        #[cfg(not(target_arch = "wasm32"))]
        let _table_hold = self
            .io
            .zip(table.as_ref())
            .map(|(io, table)| io.guard_bytes(table))
            .transpose()?;
        // An empty update revokes this table's historical bindings.
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
                    if notice.source == "tools" && notice.kind == kind {
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
            return Ok(Vec::new());
        }
        if !has_tools
            && previous.is_empty()
            && self
                .stock
                .epoch_tools
                .as_deref()
                .is_none_or(|tools| !tools.iter().any(|tool| selected(&tool)))
        {
            return Ok(Vec::new());
        }
        let decoded;
        let previous = if previous.is_empty() {
            self.stock
                .epoch_tools
                .as_deref()
                .map(Vec::as_slice)
                .unwrap_or_default()
        } else {
            #[cfg(not(target_arch = "wasm32"))]
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
            #[cfg(target_arch = "wasm32")]
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
        for old in previous.iter().filter(selected) {
            if !tools
                .iter()
                .filter(selected)
                .any(|tool| tool.name == old.name)
            {
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
            json::json_byte_length(&content(kind, "", usize::MAX, false, &[]), u64::MAX, work)
        })?;
        let maximum = self
            .stock
            .limits
            .render_bytes
            .checked_sub(overhead)
            .ok_or_else(|| invalid("render allowance cannot frame a tool notice"))?;
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
                kind,
                &remaining[..end],
                offset,
                offset + end == table.len() && removed_offset == removed.len(),
                &removed[removed_start..removed_offset],
            );
            #[cfg(not(target_arch = "wasm32"))]
            let funded = self
                .io
                .map(|io| io.encode(&notice, u64::MAX, false))
                .transpose()?;
            #[cfg(target_arch = "wasm32")]
            let funded: Option<acyclic_stream::OwnedBytes> = None;
            let bytes = match funded {
                Some(bytes) => bytes,
                None => serde_json::to_vec(&notice)
                    .map(Into::into)
                    .map_err(|_| invalid("tool notice encoding failed"))?,
            };
            #[cfg(not(target_arch = "wasm32"))]
            let _bytes_hold = self.io.map(|io| io.guard_bytes(&bytes)).transpose()?;
            #[cfg(not(target_arch = "wasm32"))]
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
            #[cfg(target_arch = "wasm32")]
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
        Ok(pending)
    }
}
