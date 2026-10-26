use super::*;

impl<P: crate::distributed::AuthorityProvider + ?Sized> StreamAuthorityStore<P> {
    pub(crate) async fn quote_group_append(
        &self,
        request: &crate::GuardedAppend,
        growth: usize,
        work: &mut AuthorityWork<'_>,
    ) -> Result<Option<crate::AuthorityGroupCost>, AuthorityStoreError> {
        let actor = self.actor(request.authority_id, work)?;
        let mut journal = actor.lease(work).await?;
        let mut child = work.operation()?;
        let result = async {
            let projection = self
                .quotation_projection(
                    request.authority_id,
                    request
                        .commit
                        .publication
                        .as_ref()
                        .map(|seal| seal.volume()),
                    &mut journal,
                    &mut child,
                )
                .await?;
            let view = AppendView {
                cursor: Some(AppendCursor::Durable(projection.snapshot)),
                projection: Some(&projection),
                transaction: None,
            };
            let prepared = self
                .prepare_append(
                    request,
                    &view,
                    &mut child,
                    None::<&mut ()>,
                    Quotation { growth },
                )
                .await
                .map_err(|error| error.into_error(std::convert::identity))?;
            let Preparation::Ready(prepared) = prepared else {
                return Ok(None);
            };
            Ok(prepared)
        }
        .await;
        work.record(child.spent())?;
        result
    }
}

fn visit(work: &mut AuthorityWork<'_>) -> Result<(), AuthorityStoreError> {
    super::view::visit(work)
}

struct Quotation {
    growth: usize,
}

impl AppendPurpose for Quotation {
    type Output<'a> = Option<crate::AuthorityGroupCost>;
    const FRESH_LINEAGE: bool = true;

    fn finish<'a, P: crate::distributed::AuthorityProvider + ?Sized>(
        self,
        store: &StreamAuthorityStore<P>,
        prepared: PreparedAppend<'a>,
        work: &mut AuthorityWork<'_>,
    ) -> Result<Self::Output<'a>, AuthorityStoreError> {
        let authority_records = prepared.receipt.records;
        let request = store.with_policy_cuts(prepared.request, &[], work)?;
        let mut records = 0usize;
        for mutation in &request.mutations {
            visit(work)?;
            let CommitMutation::Append {
                path,
                records: rows,
                ..
            } = mutation
            else {
                return Err(AuthorityStoreError::RejectedStatic(
                    "authority quote contains a non-append mutation",
                ));
            };
            records = records
                .checked_add(rows.len())
                .ok_or(crate::WorkError::Overflow)?;
            for row in rows {
                visit(work)?;
                if path == &authority_records
                    && row
                        .payload_bytes()
                        .checked_add(self.growth)
                        .is_none_or(|bytes| bytes > acyclic_stream::MAX_RECORD_BYTES)
                {
                    return Ok(None);
                }
            }
        }
        // A later dense sequence or merged protobuf length can require at
        // most nine more bytes per integer field. No wire encoder is copied.
        let integers = request
            .conditions
            .len()
            .checked_mul(3)
            .and_then(|n| records.checked_mul(3).and_then(|rows| n.checked_add(rows)))
            .and_then(|n| {
                request
                    .mutations
                    .len()
                    .checked_mul(2)
                    .and_then(|mutations| n.checked_add(mutations))
            })
            .ok_or(crate::WorkError::Overflow)?;
        let padding = integers
            .checked_mul(9)
            .and_then(|n| n.checked_add(self.growth))
            .ok_or(crate::WorkError::Overflow)?;
        let bytes = acyclic_stream::request::commit_encoded_len(&request)
            .checked_add(padding)
            .ok_or(crate::WorkError::Overflow)?;
        for _ in &request.conditions {
            visit(work)?;
        }
        Ok(Some(crate::AuthorityGroupCost { bytes, records }))
    }
}
