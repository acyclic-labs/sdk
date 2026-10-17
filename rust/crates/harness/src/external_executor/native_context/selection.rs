use super::*;

impl<'a> SessionFile<'a> {
    /// Selects the exact branch prefix before the invocation's entire assistant.
    /// Its sibling calls and later results never enter the child file.
    pub fn cut(&self, head: Option<&str>, invocation: &str) -> Result<ContextCut> {
        if !text(invocation) {
            return Err(invalid());
        }
        let branch = self.branch(head)?;
        let mut owner = None;
        let mut tool = None;
        for (position, index) in branch.iter().enumerate() {
            if let Some(name) = self.invocation_tool(*index, invocation)? {
                if owner.replace(position).is_some() {
                    return Err(invalid());
                }
                tool = Some(name);
            }
        }
        let selected = branch
            .get(..owner.ok_or_else(invalid)?)
            .ok_or_else(invalid)?;
        self.complete_exchanges(selected)?;
        let mut hash = blake3::Hasher::new();
        hash.update(b"acyclic.native-context-cut/v1\0");
        hash.update(&(self.header_bytes.len() as u64).to_le_bytes());
        hash.update(self.header_bytes);
        // The selection omits the invoking assistant, but its original bytes
        // remain part of the guard: changing its arguments cannot reuse a cut.
        for index in &branch {
            let row = self.rows.get(*index).ok_or_else(invalid)?;
            hash.update(&(row.bytes.len() as u64).to_le_bytes());
            hash.update(row.bytes);
        }
        Ok(ContextCut {
            session: self.header.id.clone(),
            workspace: self.header.cwd.clone(),
            head: head.map(str::to_owned),
            through: selected
                .last()
                .map(|index| {
                    self.rows
                        .get(*index)
                        .map(|row| row.meta.id.to_string())
                        .ok_or_else(invalid)
                })
                .transpose()?,
            invocation: invocation.into(),
            tool: tool.ok_or_else(invalid)?,
            digest: *hash.finalize().as_bytes(),
        })
    }

    /// Revalidates against the current published leaf before returning raw rows.
    /// Unselected later branches may exist; they cannot change the selected cut.
    pub fn require_cut(
        &self,
        current_head: Option<&str>,
        cut: &ContextCut,
    ) -> Result<Vec<&'a [u8]>> {
        if self.cut(current_head, &cut.invocation)? != *cut {
            return Err(invalid());
        }
        self.branch(cut.through.as_deref())?
            .into_iter()
            .map(|index| {
                self.rows
                    .get(index)
                    .map(|row| row.bytes)
                    .ok_or_else(invalid)
            })
            .collect()
    }

    /// Builds a child file from its SDK-generated fresh header and exact source
    /// records. The cut is the separate provenance manifest for this selection.
    pub fn child_file(
        &self,
        current_head: Option<&str>,
        cut: &ContextCut,
        child_header: &[u8],
        cwd: &std::path::Path,
    ) -> Result<Vec<u8>> {
        let header: header::Header = decode(child_header, acyclic_stream::MAX_RECORD_BYTES as u64)?;
        header::fresh(child_header, &header.id)?;
        if header.id == self.header.id {
            return Err(invalid());
        }
        let selected = self.require_cut(current_head, cut)?;
        let mut bytes = header::seed(child_header, cwd)?;
        for row in selected {
            bytes.extend_from_slice(row);
            bytes.push(b'\n');
        }
        Ok(bytes)
    }
}
