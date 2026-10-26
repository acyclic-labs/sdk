//! Automatic approvers shared by all host compositions.
use super::{ApprovalReviewFile, Approver, RuleVerdict, request_id, review_key};
use crate::{
    Error, Result,
    conversation::Limits,
    interaction::Interaction,
    model::{Model, ModelProvider},
    task_volume::{FileValue, Query, TaskFiles},
};
use acyclic_stream::StreamProvider;
use std::{collections::BTreeMap, sync::Arc};

mod llm;

/// Evaluates one automatic stage using the composition's explicit model bindings.
/// Escalation is durable before another stage starts. A decisive review must be
/// settled by the authenticated approval owner; the review itself grants nothing.
pub async fn automatic_review<P: StreamProvider + ?Sized>(
    models: &BTreeMap<String, (Model, Arc<dyn ModelProvider>)>,
    files: &TaskFiles<P>,
    request: &FileValue,
    approver: &Approver,
    expected: ApprovalReviewFile,
    limits: Limits,
) -> Result<ApprovalReviewFile> {
    let (verdict, reason) = match approver {
        Approver::Rules {
            rules, fallback, ..
        } => {
            let interaction: Interaction =
                serde_json::from_slice(&request.bytes).map_err(storage)?;
            let verdict = match &interaction {
                Interaction::Approval { subject, .. } => {
                    super::ApprovalRule::evaluate_subject(rules, *fallback, subject)
                }
                _ => RuleVerdict::Ask,
            };
            (verdict, Some("configured approval rules".into()))
        }
        Approver::Llm { model, prompt, .. } => {
            let (model, provider) = models.get(model).ok_or_else(|| {
                Error::Unsupported("independent approver model is not bound".into())
            })?;
            llm::review_model(
                files,
                request,
                &expected.principal,
                (model, provider.as_ref()),
                prompt,
                limits,
            )
            .await?
        }
        Approver::Human { .. } | Approver::Parent => {
            return Err(Error::Invalid(
                "external approver requires a signed decision".into(),
            ));
        }
    };
    let review = ApprovalReviewFile {
        verdict,
        reason,
        ..expected
    };
    if review.verdict == RuleVerdict::Ask {
        crate::task_volume::publish_durable(
            &files.inner,
            &FileValue::new(
                review_key(request, &review.principal)?,
                serde_json::to_vec(&review).map_err(storage)?,
            ),
            None,
        )
        .await?;
    }
    Ok(review)
}

fn storage(error: impl std::fmt::Display) -> Error {
    Error::Storage(error.to_string())
}
