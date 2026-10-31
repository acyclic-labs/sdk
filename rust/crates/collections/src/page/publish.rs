//! Immutable page publication through the caller's original IO and custody.

use super::{Child, Format, PackingError, PackingLimits, PageChunks, PageRef};

/// The original encoder, allocation owners and physical page publisher.
pub trait Work<F: Format> {
    /// Original allocation owner for a cloned first key.
    type First;
    /// Original admitted unfinished leaf tail.
    type LeafCarry;
    /// Original admitted child-summary container.
    type Output;
    /// Original admission, encoding or physical IO refusal.
    type Error;

    /// Borrows the original page bounds without changing their meaning.
    fn limits(&self) -> Result<PackingLimits, Self::Error>;
    /// Measures one original leaf entry.
    fn leaf_length(value: &F::Value) -> Result<usize, Self::Error>;
    /// Measures one original complete child reference and its bound.
    fn child_length(child: &Child<F::Key, F::Reference>) -> Result<usize, Self::Error>;
    /// Maps the shared packing refusal to the original error.
    fn packing_error(error: PackingError<Self::Error>) -> Self::Error;
    /// Reports an invalid original page position.
    fn invalid_input() -> Self::Error;
    /// Stable format-defined boundary hash; never an identity lookup.
    fn key_hash(key: &F::Key) -> u64;
    /// Retains the unfinished leaf in its original allocation owner.
    fn carry_leaf(&mut self, entries: &[F::Value]) -> Result<Self::LeafCarry, Self::Error>;
    /// Retains unfinished child summaries in their original allocation owner.
    fn carry_children(
        &mut self,
        children: &[Child<F::Key, F::Reference>],
    ) -> Result<Self::Output, Self::Error>;
    /// Charges the original examined entries before their next operation.
    fn charge_items(&mut self, count: u64) -> Result<(), Self::Error>;
    /// Allocates the original output once with its original capacity.
    fn allocate(&mut self, capacity: usize) -> Result<Self::Output, Self::Error>;
    /// Preserves the original growing leaf-output admission before page IO.
    fn reserve(&mut self, output: &mut Self::Output, maximum: usize) -> Result<(), Self::Error>;
    /// Clones a bound through the caller's original allocation owner.
    fn first(&mut self, key: &F::Key) -> Result<Self::First, Self::Error>;
    /// Transfers that original owner and complete page identity into output.
    fn append(
        &mut self,
        output: &mut Self::Output,
        first: Self::First,
        page: F::Reference,
    ) -> Result<(), Self::Error>;
    /// Publishes one original borrowed page. Its physical producer owns the
    /// original admission and settles a lost producer with its typed error.
    fn write(
        &mut self,
        page: &PageRef<'_, F>,
    ) -> impl Future<Output = Result<F::Reference, Self::Error>>;
}

/// One level's original item and encoder, selected at compile time.
pub trait Level<F: Format> {
    /// Original leaf value or child summary.
    type Item;
    /// The caller's typed unfinished tail owner.
    type Pending<W: Work<F>>;
    /// Copies only an unfinished tail under original admission.
    fn carry<W: Work<F>>(work: &mut W, items: &[Self::Item]) -> Result<Self::Pending<W>, W::Error>;
    /// Internal pages must prove that the next level reduces before any IO.
    const INTERNAL: bool;
    /// Borrows the first key without cloning it.
    fn key(item: &Self::Item) -> &F::Key;
    /// Measures the original item through the original encoder.
    fn length<W: Work<F>>(item: &Self::Item) -> Result<usize, W::Error>;
    /// Borrows an original contiguous partition for its encoder.
    fn page(items: &[Self::Item]) -> PageRef<'_, F>;
}

/// Ordered leaf values.
pub enum Leaf {}

impl<F: Format> Level<F> for Leaf {
    type Item = F::Value;
    type Pending<W: Work<F>> = W::LeafCarry;
    fn carry<W: Work<F>>(work: &mut W, items: &[Self::Item]) -> Result<Self::Pending<W>, W::Error> {
        work.carry_leaf(items)
    }
    const INTERNAL: bool = false;

    fn key(item: &Self::Item) -> &F::Key {
        F::key(item)
    }

    fn length<W: Work<F>>(item: &Self::Item) -> Result<usize, W::Error> {
        W::leaf_length(item)
    }

    fn page(items: &[Self::Item]) -> PageRef<'_, F> {
        PageRef::Leaf(items)
    }
}

/// Ordered child summaries with complete references.
pub enum Internal {}

impl<F: Format> Level<F> for Internal {
    type Item = Child<F::Key, F::Reference>;
    type Pending<W: Work<F>> = W::Output;
    fn carry<W: Work<F>>(work: &mut W, items: &[Self::Item]) -> Result<Self::Pending<W>, W::Error> {
        work.carry_children(items)
    }
    const INTERNAL: bool = true;

    fn key(item: &Self::Item) -> &F::Key {
        &item.first
    }

    fn length<W: Work<F>>(item: &Self::Item) -> Result<usize, W::Error> {
        W::child_length(item)
    }

    fn page(items: &[Self::Item]) -> PageRef<'_, F> {
        PageRef::Internal(items)
    }
}

/// Publishes completed content-defined chunks and returns only the unfinished
/// right tail to the original owner. The physical page producer is the sole
/// wait; no new allocation, task, reference codec or durability cut lives here.
pub async fn write<F, K, W, const HEADER_BYTES: usize, const TARGET_BYTES: usize>(
    work: &mut W,
    items: &[K::Item],
    level: u16,
    close: bool,
) -> Result<(W::Output, Option<K::Pending<W>>), W::Error>
where
    F: Format,
    K: Level<F>,
    W: Work<F>,
{
    let limits = work.limits()?;
    let mut chunks = PageChunks::<_, _, _, HEADER_BYTES, TARGET_BYTES>::new(
        items,
        limits,
        level,
        K::length::<W>,
        |item| W::key_hash(K::key(item)),
    )
    .map_err(W::packing_error)?;
    let mut output = work.allocate(0)?;
    while let Some((chunk, examined, closed)) = chunks.next_page().map_err(W::packing_error)? {
        work.charge_items(examined)?;
        if !closed && !close {
            return Ok((output, Some(K::carry(work, chunk)?)));
        }
        let first = work.first(K::key(chunk.first().ok_or_else(W::invalid_input)?))?;
        work.reserve(&mut output, items.len())?;
        let page = work.write(&K::page(chunk)).await?;
        work.append(&mut output, first, page)?;
    }
    Ok((output, None))
}
