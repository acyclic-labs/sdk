use super::*;

impl ViewLedger {
    pub(in crate::native_mount) fn check_change_since(
        &self,
        change: &ViewChange<'_>,
        stamp: ViewStamp,
        carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<(), MountSourceError> {
        if self.stamp() == stamp {
            return Ok(());
        }
        let unchanged = carrier.with_work(|work| match change {
            ViewChange::Node(file) => self.node_unchanged_since(*file, stamp, work),
            ViewChange::Bound(path) => self.unchanged_since(path, None, stamp, work),
            ViewChange::Entry(path, file) => self.unchanged_since(path, Some(*file), stamp, work),
            ViewChange::Moved {
                from,
                to,
                moved,
                replaced,
            } => Ok(self.unchanged_since(from, Some(*moved), stamp, work)?
                && self.unchanged_since(to, *replaced, stamp, work)?),
            ViewChange::Effect(effect) => self.effect_unchanged_since(effect, stamp, work),
            ViewChange::Everything | ViewChange::Unconfirmed => Ok(false),
        })?;
        if unchanged {
            Ok(())
        } else {
            Err(MountSourceError::Stale)
        }
    }

    fn effect_unchanged_since(
        &self,
        effect: &ViewEffect,
        stamp: ViewStamp,
        work: &mut crate::demand::DemandWork,
    ) -> Result<bool, MountSourceError> {
        work.charge(scan_work(2)).map_err(MountSourceError::from)?;
        if !stamp.precedes_none_of(&self.unconfirmed) || !stamp.precedes_none_of(&self.everything) {
            return Ok(false);
        }
        if effect.everything || !effect.subtrees.is_empty() {
            return Ok(false);
        }
        for node in &effect.nodes {
            if !self.node_unchanged_since(*node, stamp, work)? {
                return Ok(false);
            }
        }
        for (directory, name) in &effect.names {
            let names = || directory.components().iter().chain(std::iter::once(name));
            let unchanged = admitted_names(names(), work, || {
                stamp.precedes_none_of(&self.unconfirmed)
                    && stamp.precedes_none_of(&self.everything)
                    && storage::unchanged_since(&self.bindings, stamp, path_keys(names()))
            })?;
            if !unchanged {
                return Ok(false);
            }
        }
        for path in &effect.listings {
            if !self.listing_unchanged_since(path, stamp, work)? {
                return Ok(false);
            }
        }
        Ok(true)
    }
}
