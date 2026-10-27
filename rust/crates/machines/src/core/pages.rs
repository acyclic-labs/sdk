//! Page construction over the canonical projections on every platform.
use super::OperationRegistry;
use crate::*;

impl OperationRegistry {
    pub async fn machine_page(
        &self,
        after: Option<MachineId>,
        limit: u32,
    ) -> Result<MachinePage, ProviderError> {
        if limit == 0 || limit > MAX_PAGE_SIZE {
            return Err(ProviderError::Invalid(
                "machine page admission failed; recovery: request 1..256 machines".into(),
            ));
        }
        let mut machines = self
            .machines()
            .await?
            .into_iter()
            .filter(|value| after.is_none_or(|cursor| value.id > cursor))
            .take(limit as usize + 1)
            .collect::<Vec<_>>();
        let next = if machines.len() > limit as usize {
            machines.pop();
            machines.last().map(|value| value.id)
        } else {
            None
        };
        Ok(MachinePage { machines, next })
    }

    pub async fn projected_event_page(
        &self,
        machine: MachineId,
        after: Option<u64>,
        limit: u32,
    ) -> Result<EventPage, ProviderError> {
        if limit == 0 || limit > MAX_EVENT_PAGE_SIZE {
            return Err(ProviderError::Invalid(
                "Machines event page admission failed; recovery: request 1..1024 events".into(),
            ));
        }
        self.machine(machine)
            .await?
            .ok_or_else(|| ProviderError::NotFound(machine.to_string()))?;
        let (events, next_sequence) = self.events(machine, after, limit as usize).await?;
        Ok(EventPage {
            events,
            next_sequence,
        })
    }
}

/// One native page. An opaque cursor is transport data, not a public machine ID.
pub struct NativePage<T> {
    pub items: Vec<T>,
    pub next: Option<String>,
}

/// Collect native pages once, without retries. This retains the existing
/// 256-page bound and refuses a repeated cursor before issuing another request.
pub async fn collect_pages<T, F, O>(mut read: F) -> Result<Vec<T>, ProviderError>
where
    T: acyclic_stream::ProviderTask,
    F: FnMut(Option<String>) -> O + acyclic_stream::ProviderTask,
    O: std::future::Future<Output = Result<NativePage<T>, ProviderError>>
        + acyclic_stream::ProviderTask,
{
    let mut items = Vec::new();
    let mut cursor = None;
    let mut seen = BTreeSet::new();
    for _ in 0..256 {
        let page = read(cursor.take()).await?;
        items.extend(page.items);
        match page.next {
            None => return Ok(items),
            Some(next) if seen.insert(next.clone()) => cursor = Some(next),
            Some(_) => return Err(ProviderError::Rejected("native list cursor repeated; recovery: correct the provider pagination response, then call MachinesProvider::list_machines again".into())),
        }
    }
    Err(ProviderError::Rejected("native list exceeded 256 pages; recovery: narrow the native account filter, then call MachinesProvider::list_machines again".into()))
}
