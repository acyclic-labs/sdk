use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SwarmCase {
    pub agents: usize,
    pub layouts: Vec<Layout>,
    pub turns: Vec<Vec<String>>,
    pub attention_every: usize,
    pub expect: Expectations,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Layout {
    pub visible: [usize; 2],
    pub prefetch: [usize; 2],
}

impl Layout {
    pub fn selected(&self) -> impl Iterator<Item = usize> + use<'_> {
        let [a, b] = self.visible;
        let [c, d] = self.prefetch;
        (a..b).chain((c..d).filter(move |id| !(a..b).contains(id)))
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Expectations {
    pub text: String,
    pub wire: serde_json::Value,
}

impl SwarmCase {
    pub fn validate(&self) -> Result<(), &'static str> {
        if !(1..=10_000).contains(&self.agents)
            || !(1..=4).contains(&self.layouts.len())
            || self.layouts.len() != self.turns.len()
            || self.attention_every == 0
            || self.layouts.iter().any(|layout| {
                [layout.visible, layout.prefetch]
                    .iter()
                    .any(|[start, end]| start > end || *end > self.agents)
                    || layout.selected().count() > 64
            })
            || self.turns.iter().enumerate().any(|(phase, turn)| {
                !(if phase == 0 { 3 } else { 2 }..=8).contains(&turn.len())
                    || turn.iter().any(|token| token.len() > 4096)
            })
        {
            return Err("invalid bounded swarm scenario");
        }
        Ok(())
    }
}
