use super::*;

/// The original request remains the authority for all calls in its response.
pub(super) struct AdmittedModelStep<'a> {
    pub(super) events: Vec<ModelEvent>,
    pub(super) request: crate::model::PreparedModelRequest,
    pub(super) prior: usize,
    pub(super) hold: Option<crate::conversation::ContentHold<'a>>,
}

impl<'a> AdmittedModelStep<'a> {
    pub(super) fn new(
        events: Vec<ModelEvent>,
        request: crate::model::PreparedModelRequest,
        prior: usize,
        io: Option<&'a PreparationIo>,
    ) -> Result<Self> {
        let hold = io
            .map(|io| io.guard_owned(request.preparation_owned_bytes(io)?))
            .transpose()?;
        Ok(Self {
            events,
            request,
            prior,
            hold,
        })
    }
}

/// A borrowed index over one admitted catalog, shared by direct and code calls.
pub(super) struct ToolSelection<'a> {
    definitions: &'a [crate::tool::ToolDefinition],
    index: once_cell::sync::OnceCell<(Vec<usize>, Option<crate::conversation::ContentHold<'a>>)>,
    io: Option<&'a PreparationIo>,
}

impl<'a> ToolSelection<'a> {
    pub(super) fn new(
        definitions: &'a [crate::tool::ToolDefinition],
        io: Option<&'a PreparationIo>,
    ) -> Self {
        Self {
            definitions,
            index: Default::default(),
            io,
        }
    }

    pub(super) fn get(&self, name: &str) -> Result<Option<&crate::tool::ToolDefinition>> {
        let (indices, _) = self.index.get_or_try_init(|| {
            let bytes = (self.definitions.len() as u64)
                .checked_mul(std::mem::size_of::<usize>() as u64)
                .ok_or_else(|| {
                    stock_boundary(
                        self.io,
                        crate::wire::ErrorCode::Invalid,
                        "tool index overflows",
                    )
                })?;
            let hold = self.io.map(|io| io.guard_owned(bytes)).transpose()?;
            crate::model::charge_items(self.io, self.definitions.len() as u64)?;
            #[cfg(test)]
            acyclic_test_support::differential::record((
                "admitted catalog entries",
                self.definitions.len() as u64,
            ));
            let mut indices: Vec<_> = (0..self.definitions.len()).collect();
            indices.sort_unstable_by(|&left, &right| {
                self.definitions[left]
                    .name
                    .cmp(&self.definitions[right].name)
            });
            Ok::<_, Error>((indices, hold))
        })?;
        let (name, revision) = name
            .rsplit_once('@')
            .map_or((name, None), |(name, revision)| (name, Some(revision)));
        let Ok(index) =
            indices.binary_search_by(|&index| self.definitions[index].name.as_str().cmp(name))
        else {
            return Ok(None);
        };
        let definition = &self.definitions[indices[index]];
        Ok(revision
            .is_none_or(|revision| revision == definition.revision)
            .then_some(definition))
    }
}
