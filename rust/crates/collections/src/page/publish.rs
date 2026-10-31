//! Immutable page publication through the caller's original IO and custody.

use super::{Child, Format, PackingError, PackingLimits, PageChunks, PageRef};

/// The original encoder, allocation owners and physical page publisher.
pub trait Work<F: Format> {
    /// Original allocation owner for a cloned first key.
    type First;
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
    /// Refuses a parent level that cannot reduce its child count.
    fn nonreducing() -> Self::Error;
    /// Checks the original admitted height before constructing another level.
    fn next_height(&self, height: u16) -> Result<u16, Self::Error>;
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
    /// Borrows the original owned summaries without copying them.
    fn children(output: &Self::Output) -> &[Child<F::Key, F::Reference>];
    /// Retires the original summary allocation after its final use.
    fn retire(&mut self, output: Self::Output) -> Result<(), Self::Error>;
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

/// Packs and publishes one level using its original admission and IO order.
///
/// Leaf output grows under the original reservation immediately before each
/// write. Internal output is counted and its reduction proved before any write.
/// Each step consumes a nonempty partition; the only wait is its original page
/// producer. This core owns no allocation, queue, payload copy or durability cut.
pub async fn write<F, K, W, const HEADER_BYTES: usize, const TARGET_BYTES: usize>(
    work: &mut W,
    items: &[K::Item],
) -> Result<W::Output, W::Error>
where
    F: Format,
    K: Level<F>,
    W: Work<F>,
{
    let limits = work.limits()?;
    let (mut chunks, examined) =
        PageChunks::<_, _, HEADER_BYTES, TARGET_BYTES>::new(items, limits, K::length::<W>)
            .map_err(W::packing_error)?;
    work.charge_items(examined)?;
    let mut capacity = 0;
    if K::INTERNAL {
        while let Some((_, examined)) = chunks.next_page().map_err(W::packing_error)? {
            work.charge_items(examined)?;
            capacity += 1;
        }
        if items.len() > 1 && capacity >= items.len() {
            return Err(W::nonreducing());
        }
        let (next, examined) =
            PageChunks::<_, _, HEADER_BYTES, TARGET_BYTES>::new(items, limits, K::length::<W>)
                .map_err(W::packing_error)?;
        work.charge_items(examined)?;
        chunks = next;
    }
    let mut output = work.allocate(capacity)?;
    while let Some((chunk, examined)) = chunks.next_page().map_err(W::packing_error)? {
        work.charge_items(examined)?;
        let first = work.first(K::key(chunk.first().ok_or_else(W::invalid_input)?))?;
        if !K::INTERNAL {
            work.reserve(&mut output, items.len())?;
        }
        let page = work.write(&K::page(chunk)).await?;
        work.append(&mut output, first, page)?;
    }
    Ok(output)
}

/// Publishes reducing parent levels, retiring each original completed level.
/// The original height bound refuses malformed nontermination; an empty root
/// publishes the original empty leaf rather than inventing a reference.
pub async fn root<F, W, const HEADER_BYTES: usize, const TARGET_BYTES: usize>(
    work: &mut W,
    mut summaries: W::Output,
    mut height: u16,
) -> Result<F::Reference, W::Error>
where
    F: Format,
    W: Work<F>,
{
    if W::children(&summaries).is_empty() {
        work.retire(summaries)?;
        return work.write(&PageRef::Leaf(&[])).await;
    }
    while W::children(&summaries).len() > 1 {
        height = work.next_height(height)?;
        let next =
            write::<F, Internal, W, HEADER_BYTES, TARGET_BYTES>(work, W::children(&summaries))
                .await?;
        work.retire(summaries)?;
        summaries = next;
    }
    let page = W::children(&summaries)
        .first()
        .ok_or_else(W::invalid_input)?
        .page;
    work.retire(summaries)?;
    Ok(page)
}
