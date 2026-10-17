//! Derived visibility bits. Each level summarizes groups of 64 child words.

#[derive(Default)]
pub(super) struct Visibility {
    levels: Vec<Vec<u64>>,
    pub(super) end: usize,
}

impl Visibility {
    pub(super) fn set(&mut self, index: usize, visible: bool) {
        let mut bit = index;
        let mut value = visible;
        let mut level = 0;
        loop {
            if self.levels.len() == level {
                let inherited = self
                    .levels
                    .last()
                    .and_then(|words| words.first())
                    .is_some_and(|word| *word != 0);
                self.levels.push(vec![u64::from(inherited)]);
                #[cfg(test)]
                crate::work::copy(std::mem::size_of::<u64>());
            }
            let Some(words) = self.levels.get_mut(level) else {
                break;
            };
            #[cfg(test)]
            crate::work::visibility(1);
            let position = bit / 64;
            if words.len() <= position {
                #[cfg(test)]
                crate::work::copy((position + 1 - words.len()) * std::mem::size_of::<u64>());
                words.resize(position + 1, 0);
            }
            if let Some(word) = words.get_mut(position) {
                let mask = 1u64 << (bit % 64);
                if value {
                    *word |= mask;
                } else {
                    *word &= !mask;
                }
                value = *word != 0;
            }
            if words.len() == 1 {
                break;
            }
            bit = position;
            level += 1;
        }
        if visible {
            self.end = self.end.max(index + 1);
        } else if self.end == index + 1 {
            self.end = self.last();
        }
    }

    #[cfg(test)]
    pub(super) fn retained(&self) -> usize {
        self.levels
            .iter()
            .map(|words| words.capacity() * std::mem::size_of::<u64>())
            .sum()
    }

    fn last(&self) -> usize {
        let mut position = 0;
        for words in self.levels.iter().rev() {
            #[cfg(test)]
            crate::work::visibility(1);
            let word = words.get(position).copied().unwrap_or_default();
            if word == 0 {
                return 0;
            }
            let highest = 63 - word.leading_zeros() as usize;
            position = position * 64 + highest;
        }
        position + 1
    }
}
