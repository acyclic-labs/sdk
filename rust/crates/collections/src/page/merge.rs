//! Ordered leaf rewrites over original values, mutations and allocation custody.

use super::Format;

/// Original value ownership and mutation semantics for a structural leaf merge.
pub trait Work<F: Format, M> {
    /// One original owned optional value, retaining its allocation until drop.
    type Current;
    /// The original admitted output container.
    type Output;
    /// Original admission, semantic or input-contract refusal.
    type Error;

    /// Borrows the original mutation key without cloning or validation.
    fn mutation_key(mutation: &M) -> &F::Key;
    /// Clones the original value, or creates an empty original value owner.
    fn current(&mut self, value: Option<&F::Value>) -> Result<Self::Current, Self::Error>;
    /// Applies one original mutation before the next mutation is charged.
    fn apply(&mut self, mutation: &M, current: &mut Self::Current) -> Result<(), Self::Error>;
    /// Transfers a present value to output or retires its empty original owner.
    fn append(
        &mut self,
        output: &mut Self::Output,
        current: Self::Current,
    ) -> Result<(), Self::Error>;
    /// Charges original comparison or mutation work before performing it.
    fn charge_items(&mut self, count: u64) -> Result<(), Self::Error>;
    /// Reports an invalid original input position without another read.
    fn invalid_input() -> Self::Error;
}

/// Merges one ordered leaf and its original ordered mutation references.
///
/// Each step consumes an entry or mutation. Equal-key mutations retain their
/// original order and operate on one current value. Admission and replacement
/// remain interleaved, so a refusal cannot precharge later mutations. The core
/// performs no IO, allocation or value copy of its own.
pub fn apply<F: Format, M, W: Work<F, M>>(
    work: &mut W,
    entries: &[F::Value],
    mutations: &[(usize, &M)],
    output: &mut W::Output,
) -> Result<(), W::Error> {
    let mut entry_index = 0;
    let mut mutation_index = 0;
    while entry_index < entries.len() || mutation_index < mutations.len() {
        let Some((_, mutation)) = mutations.get(mutation_index) else {
            work.charge_items(1)?;
            let entry = entries.get(entry_index).ok_or_else(W::invalid_input)?;
            let cloned = work.current(Some(entry))?;
            work.append(output, cloned)?;
            entry_index += 1;
            continue;
        };
        let entry = entries.get(entry_index);
        let ordering = entry.map(|entry| F::key(entry).cmp(W::mutation_key(mutation)));
        work.charge_items(u64::from(ordering.is_some()))?;
        if matches!(ordering, Some(std::cmp::Ordering::Less)) {
            let cloned = work.current(Some(entry.ok_or_else(W::invalid_input)?))?;
            work.append(output, cloned)?;
            entry_index += 1;
            continue;
        }
        let mut current = if matches!(ordering, Some(std::cmp::Ordering::Equal)) {
            entry_index += 1;
            work.current(Some(entry.ok_or_else(W::invalid_input)?))?
        } else {
            work.current(None)?
        };
        let key = W::mutation_key(mutation);
        while let Some((_, mutation)) = mutations
            .get(mutation_index)
            .filter(|(_, mutation)| W::mutation_key(mutation) == key)
        {
            work.charge_items(1)?;
            work.apply(mutation, &mut current)?;
            mutation_index += 1;
        }
        work.append(output, current)?;
    }
    Ok(())
}
