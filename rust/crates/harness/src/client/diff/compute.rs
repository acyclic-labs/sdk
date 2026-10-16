use super::{DiffHunk, Error, FileDiff, PatchText, Result, render_bound};
use std::fmt::Write as _;

pub(super) fn work_bound() -> Error {
    Error::Invalid("diff computation work bound exceeded".into())
}

pub(super) fn charge(work: &mut usize, units: usize) -> Result<()> {
    *work = work.checked_sub(units).ok_or_else(work_bound)?;
    Ok(())
}

#[derive(Clone, Copy)]
enum Edit<'a> {
    Keep(&'a str),
    Add(&'a str),
    Remove(&'a str),
}

fn lines<'a>(text: &'a str, work: &mut usize) -> Result<Vec<&'a str>> {
    let mut lines = Vec::new();
    for line in text.split_inclusive('\n') {
        charge(work, 1)?;
        lines.push(line);
    }
    Ok(lines)
}

// A frontier omits unreachable edge diagonals. Retaining earlier frontiers
// permits exact traceback; every candidate entry is charged before allocation.
struct Frontier {
    start: usize,
    values: Vec<isize>,
}

impl Frontier {
    fn get(&self, index: usize) -> Option<&isize> {
        self.values.get(index.checked_sub(self.start)?)
    }
}

fn step(previous: &Frontier, index: usize, degree: usize, n: isize, m: isize) -> (isize, bool) {
    let diagonal = 2 * index as isize - degree as isize;
    let down = previous.get(index).copied().unwrap_or(-1);
    let right = index
        .checked_sub(1)
        .and_then(|i| previous.get(i))
        .copied()
        .filter(|x| *x >= 0)
        .map_or(-1, |x| x + 1);
    let valid = |x: isize| x >= 0 && x <= n && x - diagonal >= 0 && x - diagonal <= m;
    if valid(down) && (!valid(right) || down >= right) {
        (down, true)
    } else if valid(right) {
        (right, false)
    } else {
        (-1, false)
    }
}

fn edits<'a>(old: &[&'a str], new: &[&'a str], work: &mut usize) -> Result<Vec<Edit<'a>>> {
    if old.is_empty() {
        charge(work, new.len())?;
        return Ok(new.iter().map(|line| Edit::Add(line)).collect());
    }
    if new.is_empty() {
        charge(work, old.len())?;
        return Ok(old.iter().map(|line| Edit::Remove(line)).collect());
    }
    let maximum = old.len().checked_add(new.len()).ok_or_else(work_bound)?;
    let n = isize::try_from(old.len()).map_err(|_| work_bound())?;
    let m = isize::try_from(new.len()).map_err(|_| work_bound())?;
    // Keep signed diagonal arithmetic representable, including twice an index.
    if maximum > isize::MAX as usize / 2 {
        return Err(work_bound());
    }
    let mut trace: Vec<Frontier> = Vec::new();
    for degree in 0..=maximum {
        // An index is the number of deletions; the other edits are insertions.
        // Neither count can exceed its input, even after matching a snake.
        let (start, end) = trace.last().map_or((0, 0), |previous| {
            (
                previous.start.max(degree.saturating_sub(new.len())),
                (previous.start + previous.values.len()).min(degree.min(old.len())),
            )
        });
        charge(work, end - start + 1)?;
        let mut frontier = Frontier {
            start,
            values: Vec::with_capacity(end - start + 1),
        };
        for index in start..=end {
            let mut x = match trace.last() {
                Some(previous) => step(previous, index, degree, n, m).0,
                None => 0,
            };
            let diagonal = 2 * index as isize - degree as isize;
            let mut y = x - diagonal;
            if x >= 0 {
                while x < n && y < m {
                    let before = old.get(x as usize).ok_or_else(work_bound)?;
                    let after = new.get(y as usize).ok_or_else(work_bound)?;
                    charge(work, before.len().min(after.len()) + 1)?;
                    if before != after {
                        break;
                    }
                    x += 1;
                    y += 1;
                }
            }
            if x < 0 && frontier.values.is_empty() {
                frontier.start += 1;
                continue;
            }
            frontier.values.push(x);
            if x == n && y == m {
                trace.push(frontier);
                return traceback(&trace, old, new, work);
            }
        }
        while frontier.values.last().is_some_and(|x| *x < 0) {
            frontier.values.pop();
        }
        if frontier.values.is_empty() {
            return Err(work_bound());
        }
        trace.push(frontier);
    }
    Err(work_bound())
}

fn traceback<'a>(
    trace: &[Frontier],
    old: &[&'a str],
    new: &[&'a str],
    work: &mut usize,
) -> Result<Vec<Edit<'a>>> {
    let mut result = Vec::new();
    let (mut x, mut y) = (old.len() as isize, new.len() as isize);
    for degree in (1..trace.len()).rev() {
        let previous = trace.get(degree - 1).ok_or_else(work_bound)?;
        let diagonal = x - y;
        let index = usize::try_from((diagonal + degree as isize) / 2).map_err(|_| work_bound())?;
        let (start, down) = step(
            previous,
            index,
            degree,
            old.len() as isize,
            new.len() as isize,
        );
        let px = start - if down { 0 } else { 1 };
        let py = px - (diagonal + if down { 1 } else { -1 });
        while x > px && y > py {
            charge(work, 1)?;
            result.push(Edit::Keep(
                old.get((x - 1) as usize).ok_or_else(work_bound)?,
            ));
            x -= 1;
            y -= 1;
        }
        charge(work, 1)?;
        if down {
            result.push(Edit::Add(new.get((y - 1) as usize).ok_or_else(work_bound)?));
            y -= 1;
        } else {
            result.push(Edit::Remove(
                old.get((x - 1) as usize).ok_or_else(work_bound)?,
            ));
            x -= 1;
        }
    }
    while x > 0 && y > 0 {
        charge(work, 1)?;
        result.push(Edit::Keep(
            old.get((x - 1) as usize).ok_or_else(work_bound)?,
        ));
        x -= 1;
        y -= 1;
    }
    result.reverse();
    Ok(result)
}

pub(super) fn text_diff(
    before: &str,
    after: &str,
    maximum: usize,
    work: &mut usize,
    result: &mut FileDiff,
) -> Result<()> {
    let old = lines(before, work)?;
    let new = lines(after, work)?;
    let edits = edits(&old, &new, work)?;
    let mut ranges: Vec<(usize, usize)> = Vec::new();
    for (index, edit) in edits.iter().enumerate() {
        charge(work, 1)?;
        if matches!(edit, Edit::Keep(_)) {
            continue;
        }
        let start = index.saturating_sub(3);
        let end = (index + 4).min(edits.len());
        match ranges.last_mut() {
            Some((_, previous_end)) if *previous_end >= start => *previous_end = end,
            _ => ranges.push((start, end)),
        }
    }
    let mut output = PatchText {
        text: String::new(),
        maximum,
    };
    let formatted = |_| render_bound();
    output
        .write_str("--- original\n+++ modified\n")
        .map_err(formatted)?;
    let (mut cursor, mut old_line, mut new_line) = (0, 1, 1);
    for (start, end) in ranges {
        for edit in edits.get(cursor..start).ok_or_else(work_bound)? {
            charge(work, 1)?;
            let (old, new) = advances(*edit);
            old_line += old;
            new_line += new;
        }
        let span = edits.get(start..end).ok_or_else(work_bound)?;
        let (mut old_lines, mut new_lines) = (0, 0);
        for edit in span {
            charge(work, 1)?;
            let (old, new) = advances(*edit);
            old_lines += old;
            new_lines += new;
        }
        let old_start = if old_lines == 0 {
            old_line - 1
        } else {
            old_line
        };
        let new_start = if new_lines == 0 {
            new_line - 1
        } else {
            new_line
        };
        write!(output, "@@ -{old_start}").map_err(formatted)?;
        if old_lines != 1 {
            write!(output, ",{old_lines}").map_err(formatted)?;
        }
        write!(output, " +{new_start}").map_err(formatted)?;
        if new_lines != 1 {
            write!(output, ",{new_lines}").map_err(formatted)?;
        }
        output.write_str(" @@\n").map_err(formatted)?;
        for edit in span {
            charge(work, 1)?;
            let (prefix, text) = match edit {
                Edit::Keep(text) => (' ', text),
                Edit::Add(text) => {
                    result.added += 1;
                    ('+', text)
                }
                Edit::Remove(text) => {
                    result.removed += 1;
                    ('-', text)
                }
            };
            output.write_char(prefix).map_err(formatted)?;
            output.write_str(text).map_err(formatted)?;
            if !text.ends_with('\n') {
                output
                    .write_str("\n\\ No newline at end of file\n")
                    .map_err(formatted)?;
            }
        }
        result.hunks.push(DiffHunk {
            old_start,
            old_lines,
            new_start,
            new_lines,
        });
        old_line += old_lines;
        new_line += new_lines;
        cursor = end;
    }
    result.patch = output.text;
    Ok(())
}

fn advances(edit: Edit<'_>) -> (usize, usize) {
    match edit {
        Edit::Keep(_) => (1, 1),
        Edit::Add(_) => (0, 1),
        Edit::Remove(_) => (1, 0),
    }
}
