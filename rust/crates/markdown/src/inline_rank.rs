use crate::inline_scan::forest::PAGE;

#[derive(Default)]
struct Word {
    bits: u64,
    prefix: usize,
    previous: Option<usize>,
}

#[derive(Default)]
pub(in crate::inline_scan) struct Rank {
    first: Word,
    pages: Vec<Box<[Word]>>,
    tail: Vec<Word>,
    length: usize,
    ones: usize,
}

impl Rank {
    fn word(&self, index: usize) -> Option<&Word> {
        if index >= self.length.div_ceil(64) {
            return None;
        }
        if index == 0 {
            return Some(&self.first);
        }
        let index = index - 1;
        self.pages
            .get(index / PAGE)
            .map_or(self.tail.as_slice(), AsRef::as_ref)
            .get(index % PAGE)
    }

    pub(in crate::inline_scan) fn push(&mut self, value: bool) {
        let bit = self.length % 64;
        let index = self.length / 64;
        #[cfg(test)]
        crate::work::scan(1);
        if bit == 0 && index != 0 {
            let previous = index.checked_sub(1).and_then(|before| {
                let word = self.word(before)?;
                if word.bits != 0 {
                    Some(before)
                } else {
                    word.previous
                }
            });
            #[cfg(test)]
            {
                crate::work::copy(std::mem::size_of::<Word>());
            }
            if self.tail.len() == PAGE {
                #[cfg(test)]
                if self.pages.len() == self.pages.capacity() {
                    crate::work::copy(self.pages.len() * std::mem::size_of::<Box<[Word]>>());
                }
                self.pages
                    .push(std::mem::take(&mut self.tail).into_boxed_slice());
            }
            if self.tail.capacity() == 0 {
                self.tail = Vec::with_capacity(PAGE);
            }
            self.tail.push(Word {
                bits: 0,
                prefix: self.ones,
                previous,
            });
        }
        if value {
            if index == 0 {
                self.first.bits |= 1u64 << bit;
            } else {
                let index = index - 1;
                let page = self
                    .pages
                    .get_mut(index / PAGE)
                    .map_or(self.tail.as_mut_slice(), AsMut::as_mut);
                if let Some(word) = page.get_mut(index % PAGE) {
                    word.bits |= 1u64 << bit;
                }
            }
            self.ones += 1;
        }
        self.length += 1;
    }

    pub(in crate::inline_scan) fn before(&self, position: usize) -> Option<usize> {
        if position > self.length {
            return None;
        }
        if position == self.length {
            return Some(self.ones);
        }
        let mask = (1u64 << (position % 64)) - 1;
        let word = self.word(position / 64)?;
        let bits = word.bits & mask;
        #[cfg(test)]
        if OMIT_PREFIX.get() {
            return Some(bits.count_ones() as usize);
        }
        Some(word.prefix + bits.count_ones() as usize)
    }

    pub(in crate::inline_scan) fn last_before(&self, position: usize) -> Option<usize> {
        #[cfg(test)]
        crate::work::scan(1);
        if position > self.length {
            return None;
        }
        let index = position.checked_sub(1)? / 64;
        let mask = u64::MAX >> (63 - (position - 1) % 64);
        let word = self.word(index)?.bits & mask;
        let (index, word) = if word == 0 {
            let previous = self.word(index)?.previous?;
            (previous, self.word(previous)?.bits)
        } else {
            (index, word)
        };
        Some(index * 64 + 63 - word.leading_zeros() as usize)
    }

    #[cfg(test)]
    pub(in crate::inline_scan) fn retained(&self) -> usize {
        self.pages.capacity() * std::mem::size_of::<Box<[Word]>>()
            + (self.pages.len() * PAGE + self.tail.capacity()) * std::mem::size_of::<Word>()
    }
}

#[cfg(test)]
thread_local! { static OMIT_PREFIX: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }

#[cfg(test)]
#[path = "../tests/inline_rank.rs"]
mod tests;
