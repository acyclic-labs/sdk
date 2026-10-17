// Pulldown-cmark accepts at most 33 nested destination parentheses.
const LEVELS: usize = 34;

pub(super) struct Bare {
    slots: [Vec<usize>; LEVELS],
    head: usize,
    used: u64,
    escaped: bool,
    #[cfg(test)]
    pub(super) forget_escape: bool,
}

impl Default for Bare {
    fn default() -> Self {
        Self {
            slots: std::array::from_fn(|_| Vec::new()),
            head: 0,
            used: 0,
            escaped: false,
            #[cfg(test)]
            forget_escape: false,
        }
    }
}

impl Bare {
    pub(super) fn insert(&mut self, start: usize, depth: usize, escaped: bool) {
        // A newly entered bare destination follows '(' in the source, so its
        // escape parity agrees with all existing bare destinations.
        debug_assert!(self.used == 0 || self.escaped == escaped);
        self.escaped = escaped;
        let slot = (self.head + depth) % LEVELS;
        let Some(values) = self.slots.get_mut(slot) else {
            return;
        };
        #[cfg(test)]
        crate::work::copy(
            std::mem::size_of::<usize>()
                * (1 + if values.len() == values.capacity() {
                    values.len()
                } else {
                    0
                }),
        );
        values.push(start);
        self.used |= 1 << slot;
    }

    fn take(&mut self, depth: usize) -> Vec<usize> {
        let slot = (self.head + depth) % LEVELS;
        self.used &= !(1 << slot);
        self.slots
            .get_mut(slot)
            .map_or_else(Vec::new, std::mem::take)
    }

    fn clear(&mut self, slot: usize) {
        if let Some(values) = self.slots.get_mut(slot) {
            #[cfg(test)]
            crate::work::scan(1 + values.len());
            values.clear();
        }
        self.used &= !(1 << slot);
    }

    pub(super) fn advance(&mut self, character: char) -> (Vec<usize>, bool) {
        if self.used == 0 {
            return (Vec::new(), false);
        }
        #[cfg(test)]
        crate::work::scan(character.len_utf8());
        if std::mem::take(&mut self.escaped) && character.is_ascii_punctuation() {
            return (Vec::new(), false);
        }
        match character {
            '\\' => {
                #[cfg(test)]
                let escape = !self.forget_escape;
                #[cfg(not(test))]
                let escape = true;
                self.escaped = escape;
            }
            '(' => {
                self.clear((self.head + LEVELS - 1) % LEVELS);
                self.head = (self.head + LEVELS - 1) % LEVELS;
            }
            ')' => {
                let done = self.take(0);
                self.head = (self.head + 1) % LEVELS;
                return (done, false);
            }
            '\0'..='\u{20}' => {
                let done = self.take(0);
                while self.used != 0 {
                    self.clear(self.used.trailing_zeros() as usize);
                }
                return (done, true);
            }
            _ => {}
        }
        (Vec::new(), false)
    }

    #[cfg(test)]
    pub(super) fn retained(&self) -> usize {
        std::mem::size_of::<Self>()
            + self
                .slots
                .iter()
                .map(|slot| slot.capacity() * std::mem::size_of::<usize>())
                .sum::<usize>()
    }
}
