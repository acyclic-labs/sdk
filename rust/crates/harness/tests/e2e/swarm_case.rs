include!("../support/schema/swarm.rs");

impl Layout {
    pub fn rendered(&self, id: usize) -> bool {
        (self.visible[0]..self.visible[1]).contains(&id)
    }
}

impl SwarmCase {
    pub fn through(&self, phase: usize) -> u64 {
        self.turns[..=phase]
            .iter()
            .map(|turn| turn.len() as u64)
            .sum()
    }
}
