//! Fallible, linear-space text merge. Every owned backing enters through [`Work`].
//! Bidirectional Myers search adapted from diffy 0.5.2 (MIT, see LICENSE-MIT).

use std::collections::TryReserveError;
use std::hash::{Hash, Hasher};
use std::ops::Range;

/// Admission on the original invocation. Claims must retire with their backing.
pub trait Work {
    /// Owned credit, released on drop after the corresponding physical buffer.
    type Allocation;
    /// Original typed admission or cancellation failure.
    type Error;
    /// Admits one positive allocation before reaching the allocator.
    fn allocate(&mut self, bytes: usize) -> std::result::Result<Self::Allocation, Self::Error>;
    /// Admits copied bytes before copying them.
    fn copy(&mut self, bytes: usize) -> std::result::Result<(), Self::Error>;
    /// Admits one examined merge item, also observing cancellation.
    fn examine(&mut self) -> std::result::Result<(), Self::Error>;
}

/// Typed failure without an allocated diagnostic wrapper.
#[derive(Debug)]
pub enum Error<E> {
    /// Original admission or cancellation failure.
    Work(E),
    /// Actual allocator refusal.
    Allocation(TryReserveError),
    /// A requested capacity or position is not representable.
    Overflow,
}

/// The output and the exact claim funding its `String` capacity.
pub struct Output<A> {
    /// Whether the output contains no generated conflict blocks.
    pub clean: bool,
    /// Complete merged UTF-8 text.
    pub content: String,
    /// Retires after the content, including during unwind.
    pub allocation: A,
}

struct Buffer<T, A> {
    values: Vec<T>,
    allocation: A,
}

type Result<T, W> = std::result::Result<T, Error<<W as Work>::Error>>;

impl<T, A> Buffer<T, A> {
    fn new<W: Work<Allocation = A>>(capacity: usize, work: &mut W) -> Result<Self, W> {
        let bytes = capacity
            .checked_mul(std::mem::size_of::<T>())
            .ok_or(Error::Overflow)?;
        let allocation = work.allocate(bytes).map_err(Error::Work)?;
        let mut values = Vec::new();
        values
            .try_reserve_exact(capacity)
            .map_err(Error::Allocation)?;
        Ok(Self { values, allocation })
    }

    fn reserve<W: Work<Allocation = A>>(&mut self, extra: usize, work: &mut W) -> Result<(), W> {
        let length = self
            .values
            .len()
            .checked_add(extra)
            .ok_or(Error::Overflow)?;
        if length > self.values.capacity() {
            let capacity = length.max(self.values.capacity().saturating_mul(2));
            let bytes = self
                .values
                .len()
                .checked_mul(std::mem::size_of::<T>())
                .ok_or(Error::Overflow)?;
            let mut replacement = Self::new(capacity, work)?;
            work.copy(bytes).map_err(Error::Work)?;
            replacement.values.append(&mut self.values);
            // Both backings remain funded through movement. Drop the old
            // physical allocation before returning its original credit.
            *self = replacement;
        }
        Ok(())
    }

    fn push<W: Work<Allocation = A>>(&mut self, value: T, work: &mut W) -> Result<(), W> {
        self.reserve(1, work)?;
        self.values.push(value);
        Ok(())
    }

    fn insert<W: Work<Allocation = A>>(
        &mut self,
        index: usize,
        value: T,
        work: &mut W,
    ) -> Result<(), W> {
        self.reserve(1, work)?;
        work.copy(
            (self.values.len() - index)
                .checked_mul(std::mem::size_of::<T>())
                .ok_or(Error::Overflow)?,
        )
        .map_err(Error::Work)?;
        self.values.insert(index, value);
        Ok(())
    }

    fn remove<W: Work<Allocation = A>>(&mut self, index: usize, work: &mut W) -> Result<T, W> {
        work.copy(
            (self.values.len() - index - 1)
                .checked_mul(std::mem::size_of::<T>())
                .ok_or(Error::Overflow)?,
        )
        .map_err(Error::Work)?;
        Ok(self.values.remove(index))
    }
}

impl<A> Buffer<u8, A> {
    fn append<W: Work<Allocation = A>>(&mut self, value: &[u8], work: &mut W) -> Result<(), W> {
        self.reserve(value.len(), work)?;
        work.copy(value.len()).map_err(Error::Work)?;
        self.values.extend_from_slice(value);
        Ok(())
    }
}

#[derive(Eq)]
struct Line<'a> {
    text: &'a str,
    hash: u64,
}

impl PartialEq for Line<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash && normalized(self.text) == normalized(other.text)
    }
}

fn normalized(text: &str) -> &str {
    text.strip_suffix('\n').unwrap_or(text)
}

fn lines<'a, W: Work>(text: &'a str, work: &mut W) -> Result<Buffer<Line<'a>, W::Allocation>, W> {
    let mut result = Buffer::new(0, work)?;
    for text in text.split_inclusive('\n') {
        work.examine().map_err(Error::Work)?;
        let mut hash = std::collections::hash_map::DefaultHasher::new();
        normalized(text).hash(&mut hash);
        result.push(
            Line {
                text,
                hash: hash.finish(),
            },
            work,
        )?;
    }
    Ok(result)
}

#[derive(Clone)]
struct Edit {
    base: Range<usize>,
    side: Range<usize>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Equal,
    Delete,
    Insert,
}

#[derive(Clone)]
struct Span {
    kind: Kind,
    base: Range<usize>,
    side: Range<usize>,
}

impl Span {
    fn length(&self) -> usize {
        if self.kind == Kind::Insert {
            self.side.len()
        } else {
            self.base.len()
        }
    }

    fn grow(&mut self, length: usize, up: bool) {
        for range in [&mut self.base, &mut self.side] {
            if up {
                range.start -= length;
            } else {
                range.end += length;
            }
        }
    }

    fn shrink(&mut self, length: usize, front: bool) {
        for range in [&mut self.base, &mut self.side] {
            if front {
                range.start += length;
            } else {
                range.end -= length;
            }
        }
    }
}

fn common<W: Work>(a: &[Line<'_>], b: &[Line<'_>], suffix: bool, work: &mut W) -> Result<usize, W> {
    let mut length = 0;
    while length < a.len().min(b.len()) {
        work.examine().map_err(Error::Work)?;
        let (i, j) = if suffix {
            (a.len() - length - 1, b.len() - length - 1)
        } else {
            (length, length)
        };
        if a.get(i) != b.get(j) {
            break;
        }
        length += 1;
    }
    Ok(length)
}

fn shift_equal<W: Work>(
    spans: &mut Buffer<Span, W::Allocation>,
    mut pointer: usize,
    up: bool,
    base: &[Line<'_>],
    side: &[Line<'_>],
    work: &mut W,
) -> Result<Option<usize>, W> {
    let neighbor = if up { pointer - 1 } else { pointer + 1 };
    let this = spans.values.get(pointer).ok_or(Error::Overflow)?.clone();
    let next = spans.values.get(neighbor).ok_or(Error::Overflow)?.clone();
    let (a, b) = if this.kind == Kind::Delete {
        (base.get(this.base.clone()), side.get(next.side.clone()))
    } else {
        (side.get(this.side.clone()), base.get(next.base.clone()))
    };
    let amount = common(
        a.ok_or(Error::Overflow)?,
        b.ok_or(Error::Overflow)?,
        up,
        work,
    )?;
    if amount == 0 {
        return Ok(None);
    }
    let mut equal = next;
    if this.kind == Kind::Delete {
        equal.base = this.base;
    } else {
        equal.side = this.side;
    }
    for range in [&mut equal.base, &mut equal.side] {
        if up {
            range.start = range.end - amount;
        } else {
            range.end = range.start + amount;
        }
    }
    let beyond = if up {
        pointer.checked_add(1)
    } else {
        pointer.checked_sub(1)
    };
    if let Some(index) = beyond.filter(|index| {
        spans
            .values
            .get(*index)
            .is_some_and(|span| span.kind == Kind::Equal)
    }) {
        spans
            .values
            .get_mut(index)
            .ok_or(Error::Overflow)?
            .grow(amount, up);
    } else {
        spans.insert(if up { pointer + 1 } else { pointer }, equal, work)?;
        if !up {
            pointer += 1;
        }
    }
    let this = spans.values.get_mut(pointer).ok_or(Error::Overflow)?;
    let range = if this.kind == Kind::Delete {
        &mut this.base
    } else {
        &mut this.side
    };
    if up {
        range.start -= amount;
        range.end -= amount;
    } else {
        range.start += amount;
        range.end += amount;
    }
    let neighbor = if up { pointer - 1 } else { pointer + 1 };
    let next = spans.values.get_mut(neighbor).ok_or(Error::Overflow)?;
    next.shrink(amount, !up);
    if next.length() == 0 {
        spans.remove(neighbor, work)?;
        if up {
            pointer -= 1;
        }
    }
    Ok(Some(pointer))
}

// Compact deletions, then insertions, in the same order as the previous
// shared diffy merger. Repeated lines must not change hunk alignment merely
// because storage admission became explicit.
fn shift<W: Work>(
    spans: &mut Buffer<Span, W::Allocation>,
    mut pointer: usize,
    up: bool,
    base: &[Line<'_>],
    side: &[Line<'_>],
    work: &mut W,
) -> Result<usize, W> {
    loop {
        work.examine().map_err(Error::Work)?;
        let neighbor = if up {
            pointer.checked_sub(1)
        } else {
            pointer.checked_add(1)
        };
        let Some(neighbor) = neighbor.filter(|index| *index < spans.values.len()) else {
            break;
        };
        let this = spans.values.get(pointer).ok_or(Error::Overflow)?.clone();
        let next = spans.values.get(neighbor).ok_or(Error::Overflow)?.clone();
        if next.kind == Kind::Equal {
            let Some(shifted) = shift_equal(spans, pointer, up, base, side, work)? else {
                break;
            };
            pointer = shifted;
        } else if next.kind == this.kind {
            let (keep, remove) = if up {
                (neighbor, pointer)
            } else {
                (pointer, neighbor)
            };
            let length = if up { this.length() } else { next.length() };
            let span = spans.values.get_mut(keep).ok_or(Error::Overflow)?;
            if span.kind == Kind::Delete {
                span.base.end += length;
            } else {
                span.side.end += length;
            }
            spans.remove(remove, work)?;
            pointer = keep;
        } else {
            spans.values.swap(pointer, neighbor);
            pointer = neighbor;
        }
    }
    Ok(pointer)
}

fn compact<W: Work>(
    base: &[Line<'_>],
    side: &[Line<'_>],
    edits: Buffer<Edit, W::Allocation>,
    work: &mut W,
) -> Result<Buffer<Edit, W::Allocation>, W> {
    let mut spans = Buffer::new(0, work)?;
    let (mut a, mut b) = (0, 0);
    for edit in &edits.values {
        if edit.base.start > a {
            spans.push(
                Span {
                    kind: Kind::Equal,
                    base: a..edit.base.start,
                    side: b..edit.side.start,
                },
                work,
            )?;
        }
        if !edit.base.is_empty() {
            spans.push(
                Span {
                    kind: Kind::Delete,
                    base: edit.base.clone(),
                    side: 0..0,
                },
                work,
            )?;
        }
        if !edit.side.is_empty() {
            spans.push(
                Span {
                    kind: Kind::Insert,
                    base: 0..0,
                    side: edit.side.clone(),
                },
                work,
            )?;
        }
        (a, b) = (edit.base.end, edit.side.end);
    }
    if a < base.len() {
        spans.push(
            Span {
                kind: Kind::Equal,
                base: a..base.len(),
                side: b..side.len(),
            },
            work,
        )?;
    }
    drop(edits);
    for kind in [Kind::Delete, Kind::Insert] {
        let mut pointer = 0;
        while let Some(span) = spans.values.get(pointer) {
            if span.kind == kind {
                pointer = shift(&mut spans, pointer, true, base, side, work)?;
                pointer = shift(&mut spans, pointer, false, base, side, work)?;
            }
            pointer += 1;
        }
    }
    let mut output: Buffer<Edit, _> = Buffer::new(0, work)?;
    let (mut a, mut b) = (0, 0);
    let mut pending = None;
    for span in &spans.values {
        work.examine().map_err(Error::Work)?;
        if span.kind == Kind::Equal {
            if let Some(edit) = pending.take() {
                output.push(edit, work)?;
            }
            a += span.length();
            b += span.length();
        } else {
            let edit = pending.get_or_insert(Edit {
                base: a..a,
                side: b..b,
            });
            if span.kind == Kind::Delete {
                a += span.length();
                edit.base.end = a;
            } else {
                b += span.length();
                edit.side.end = b;
            }
        }
    }
    if let Some(edit) = pending {
        output.push(edit, work)?;
    }
    Ok(output)
}

struct Frontier<A> {
    forward: Buffer<usize, A>,
    backward: Buffer<usize, A>,
    offset: isize,
}

fn frontier_index<E>(offset: isize, diagonal: isize) -> std::result::Result<usize, Error<E>> {
    usize::try_from(offset.checked_add(diagonal).ok_or(Error::Overflow)?)
        .map_err(|_| Error::Overflow)
}

fn endpoint<E, A>(
    buffer: &Buffer<usize, A>,
    offset: isize,
    diagonal: isize,
) -> std::result::Result<usize, Error<E>> {
    buffer
        .values
        .get(frontier_index(offset, diagonal)?)
        .copied()
        .ok_or(Error::Overflow)
}

// Bidirectional Myers search. Only two linear frontiers survive a split;
// the explicit job stack avoids input-dependent native call-stack growth.
fn search_step<W: Work>(
    base: &[Line<'_>],
    side: &[Line<'_>],
    frontier: &mut Frontier<W::Allocation>,
    work: &mut W,
    search: (isize, isize, bool),
) -> Result<Option<(usize, usize)>, W> {
    let (distance, diagonal, reverse) = search;
    let (n, m, offset) = (base.len(), side.len(), frontier.offset);
    let delta = isize::try_from(n).map_err(|_| Error::Overflow)?
        - isize::try_from(m).map_err(|_| Error::Overflow)?;
    work.examine().map_err(Error::Work)?;
    let buffer = if reverse {
        &frontier.backward
    } else {
        &frontier.forward
    };
    let mut x = if diagonal == -distance
        || (diagonal != distance
            && endpoint(buffer, offset, diagonal - 1)? < endpoint(buffer, offset, diagonal + 1)?)
    {
        endpoint(buffer, offset, diagonal + 1)?
    } else {
        endpoint(buffer, offset, diagonal - 1)?
            .checked_add(1)
            .ok_or(Error::Overflow)?
    };
    let mut y = usize::try_from(isize::try_from(x).map_err(|_| Error::Overflow)? - diagonal)
        .map_err(|_| Error::Overflow)?;
    let start = (x, y);
    while x < n && y < m {
        work.examine().map_err(Error::Work)?;
        let (a, b) = if reverse {
            (n - x - 1, m - y - 1)
        } else {
            (x, y)
        };
        if base.get(a) != side.get(b) {
            break;
        }
        x += 1;
        y += 1;
    }
    let buffer = if reverse {
        &mut frontier.backward
    } else {
        &mut frontier.forward
    };
    *buffer
        .values
        .get_mut(frontier_index(offset, diagonal)?)
        .ok_or(Error::Overflow)? = x;
    let reciprocal = diagonal - delta;
    let odd = delta & 1 != 0;
    let connects = if reverse {
        !odd && reciprocal.abs() <= distance
    } else {
        odd && reciprocal.abs() < distance
    };
    if connects {
        let other = if reverse {
            &frontier.forward
        } else {
            &frontier.backward
        };
        if x.checked_add(endpoint(other, offset, -reciprocal)?)
            .ok_or(Error::Overflow)?
            >= n
        {
            return Ok(Some(if reverse { (n - x, m - y) } else { start }));
        }
    }
    Ok(None)
}

fn middle<W: Work>(
    base: &[Line<'_>],
    side: &[Line<'_>],
    frontier: &mut Frontier<W::Allocation>,
    work: &mut W,
) -> Result<(usize, usize), W> {
    let n = base.len();
    let m = side.len();
    let maximum = n
        .checked_add(m)
        .ok_or(Error::Overflow)?
        .div_ceil(2)
        .checked_add(1)
        .ok_or(Error::Overflow)?;
    let offset = frontier.offset;
    for buffer in [&mut frontier.forward, &mut frontier.backward] {
        *buffer
            .values
            .get_mut(frontier_index(offset, 1)?)
            .ok_or(Error::Overflow)? = 0;
    }
    for distance in 0..isize::try_from(maximum).map_err(|_| Error::Overflow)? {
        for reverse in [false, true] {
            for diagonal in (-distance..=distance).rev().step_by(2) {
                if let Some(split) =
                    search_step(base, side, frontier, work, (distance, diagonal, reverse))?
                {
                    return Ok(split);
                }
            }
        }
    }
    Err(Error::Overflow)
}

fn edits<W: Work>(
    base: &[Line<'_>],
    side: &[Line<'_>],
    frontier: &mut Frontier<W::Allocation>,
    work: &mut W,
) -> Result<Buffer<Edit, W::Allocation>, W> {
    let mut output = Buffer::new(0, work)?;
    let mut jobs = Buffer::new(1, work)?;
    jobs.values.push((0..base.len(), 0..side.len()));
    while let Some((mut a, mut b)) = jobs.values.pop() {
        while !a.is_empty() && !b.is_empty() {
            work.examine().map_err(Error::Work)?;
            if base.get(a.start) != side.get(b.start) {
                break;
            }
            a.start += 1;
            b.start += 1;
        }
        while !a.is_empty() && !b.is_empty() {
            work.examine().map_err(Error::Work)?;
            if base.get(a.end - 1) != side.get(b.end - 1) {
                break;
            }
            a.end -= 1;
            b.end -= 1;
        }
        if a.is_empty() || b.is_empty() {
            if !a.is_empty() || !b.is_empty() {
                output.push(Edit { base: a, side: b }, work)?;
            }
            continue;
        }
        let (x, y) = middle(
            base.get(a.clone()).ok_or(Error::Overflow)?,
            side.get(b.clone()).ok_or(Error::Overflow)?,
            frontier,
            work,
        )?;
        if (x == 0 && y == 0) || (x == a.len() && y == b.len()) {
            output.push(Edit { base: a, side: b }, work)?;
        } else {
            let x = a.start.checked_add(x).ok_or(Error::Overflow)?;
            let y = b.start.checked_add(y).ok_or(Error::Overflow)?;
            jobs.push((x..a.end, y..b.end), work)?;
            jobs.push((a.start..x, b.start..y), work)?;
        }
    }
    compact(base, side, output, work)
}

fn append_lines<W: Work>(
    output: &mut Buffer<u8, W::Allocation>,
    lines: &[Line<'_>],
    work: &mut W,
) -> Result<(), W> {
    for line in lines {
        work.examine().map_err(Error::Work)?;
        output.append(line.text.as_bytes(), work)?;
        if !line.text.ends_with('\n') {
            output.append(b"\n", work)?;
        }
    }
    Ok(())
}

fn side_end<E>(
    start: usize,
    length: usize,
    edits: &[Edit],
) -> std::result::Result<usize, Error<E>> {
    edits.iter().try_fold(
        start.checked_add(length).ok_or(Error::Overflow)?,
        |end, edit| {
            end.checked_sub(edit.base.len())
                .and_then(|end| end.checked_add(edit.side.len()))
                .ok_or(Error::Overflow)
        },
    )
}

fn project<W: Work>(
    base: &[Line<'_>],
    ours: &[Line<'_>],
    theirs: &[Line<'_>],
    changes: (&[Edit], &[Edit]),
    labels: (&str, &str),
    work: &mut W,
) -> Result<(bool, Buffer<u8, W::Allocation>), W> {
    let (a, b) = changes;
    let (mut i, mut j, mut position, mut our_position, mut their_position) = (0, 0, 0, 0, 0);
    let mut clean = true;
    let mut output = Buffer::new(0, work)?;
    while i < a.len() || j < b.len() {
        work.examine().map_err(Error::Work)?;
        let start = match (a.get(i), b.get(j)) {
            (Some(a), Some(b)) => a.base.start.min(b.base.start),
            (Some(a), None) => a.base.start,
            (None, Some(b)) => b.base.start,
            (None, None) => break,
        };
        append_lines(
            &mut output,
            base.get(position..start).ok_or(Error::Overflow)?,
            work,
        )?;
        let our_start = our_position + (start - position);
        let their_start = their_position + (start - position);
        let (first_i, first_j) = (i, j);
        let mut end = start;
        loop {
            let previous = (i, j);
            while let Some(edit) = a.get(i).filter(|edit| edit.base.start <= end) {
                end = end.max(edit.base.end);
                i += 1;
            }
            while let Some(edit) = b.get(j).filter(|edit| edit.base.start <= end) {
                end = end.max(edit.base.end);
                j += 1;
            }
            if previous == (i, j) {
                break;
            }
        }
        let our_end = side_end(
            our_start,
            end - start,
            a.get(first_i..i).ok_or(Error::Overflow)?,
        )?;
        let their_end = side_end(
            their_start,
            end - start,
            b.get(first_j..j).ok_or(Error::Overflow)?,
        )?;
        let our_lines = ours.get(our_start..our_end).ok_or(Error::Overflow)?;
        let their_lines = theirs.get(their_start..their_end).ok_or(Error::Overflow)?;
        let base_lines = base.get(start..end).ok_or(Error::Overflow)?;
        if i == first_i || our_lines == base_lines {
            append_lines(&mut output, their_lines, work)?;
        } else if j == first_j || our_lines == their_lines || their_lines == base_lines {
            append_lines(&mut output, our_lines, work)?;
        } else {
            clean = false;
            output.append(b"<<<<<<< ", work)?;
            output.append(labels.0.as_bytes(), work)?;
            output.append(b"\n", work)?;
            append_lines(&mut output, our_lines, work)?;
            output.append(b"||||||| original\n", work)?;
            append_lines(
                &mut output,
                base.get(start..end).ok_or(Error::Overflow)?,
                work,
            )?;
            output.append(b"=======\n", work)?;
            append_lines(&mut output, their_lines, work)?;
            output.append(b">>>>>>> ", work)?;
            output.append(labels.1.as_bytes(), work)?;
            output.append(b"\n", work)?;
        }
        position = end;
        our_position = our_end;
        their_position = their_end;
    }
    append_lines(
        &mut output,
        base.get(position..).ok_or(Error::Overflow)?,
        work,
    )?;
    Ok((clean, output))
}

/// Merge on the original work and allocation owner. Scratch is linear in input
/// lines, every growth admits simultaneous backings, and output keeps its guard.
pub fn merge<W: Work>(
    base: &str,
    ours: &str,
    theirs: &str,
    ours_label: &str,
    theirs_label: &str,
    work: &mut W,
) -> Result<Output<W::Allocation>, W> {
    let selected = if ours == theirs || base == theirs {
        Some(ours)
    } else if base == ours {
        Some(theirs)
    } else {
        None
    };
    if let Some(text) = selected {
        let mut output = Buffer::new(text.len(), work)?;
        output.append(text.as_bytes(), work)?;
        let Buffer { values, allocation } = output;
        return Ok(Output {
            clean: true,
            content: String::from_utf8(values).map_err(|_| Error::Overflow)?,
            allocation,
        });
    }
    let trailing = super::result_has_trailing_newline(base, ours, theirs);
    let base = lines(base, work)?;
    let ours = lines(ours, work)?;
    let theirs = lines(theirs, work)?;
    let maximum = base
        .values
        .len()
        .checked_add(ours.values.len().max(theirs.values.len()))
        .ok_or(Error::Overflow)?
        .div_ceil(2)
        .checked_add(1)
        .ok_or(Error::Overflow)?;
    let capacity = maximum.checked_mul(2).ok_or(Error::Overflow)?;
    let mut forward = Buffer::new(capacity, work)?;
    let mut backward = Buffer::new(capacity, work)?;
    forward.values.resize(capacity, 0);
    backward.values.resize(capacity, 0);
    let mut frontier = Frontier {
        forward,
        backward,
        offset: isize::try_from(maximum).map_err(|_| Error::Overflow)?,
    };
    let a = edits(&base.values, &ours.values, &mut frontier, work)?;
    let b = edits(&base.values, &theirs.values, &mut frontier, work)?;
    drop(frontier);
    let (clean, mut output) = project(
        &base.values,
        &ours.values,
        &theirs.values,
        (&a.values, &b.values),
        (ours_label, theirs_label),
        work,
    )?;
    if clean && !trailing {
        output.values.pop();
    }
    let Buffer { values, allocation } = output;
    let content = String::from_utf8(values).map_err(|_| Error::Overflow)?;
    Ok(Output {
        clean,
        content,
        allocation,
    })
}
