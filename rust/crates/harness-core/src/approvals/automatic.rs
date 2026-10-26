//! Automatic approvers shared by all host compositions.
use super::{ApprovalFiles, ApprovalRequest, ApprovalReviewFile, Approver, RuleVerdict};
use crate::{
    Error, Result,
    conversation::Limits,
    interaction::Interaction,
    model::{Model, ModelProvider},
};
use std::{collections::BTreeMap, sync::Arc};

mod llm;

/// Evaluates one automatic stage using the composition's explicit model bindings.
/// Escalation is durable before another stage starts. A decisive review must be
/// settled by the authenticated approval owner; the review itself grants nothing.
pub async fn automatic_review(
    models: &BTreeMap<String, (Model, Arc<dyn ModelProvider>)>,
    files: &dyn ApprovalFiles,
    request: &ApprovalRequest<'_>,
    approver: &Approver,
    expected: ApprovalReviewFile,
    limits: Limits,
) -> Result<ApprovalReviewFile> {
    let principal = match approver {
        Approver::Rules { principal, .. } | Approver::Llm { principal, .. } => principal,
        _ => return Err(Error::Invalid("external approver requires a signed decision".into())),
    };
    if expected.principal != *principal
        || expected.request_sha256 != super::chain::fingerprint(request.bytes())?
    {
        return Err(Error::Conflict("automatic approval stage binding differs".into()));
    }
    let (verdict, reason) = match approver {
        Approver::Rules {
            rules, fallback, ..
        } => {
            let interaction: Interaction =
                serde_json::from_slice(request.bytes()).map_err(storage)?;
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
    // Retain every review, including a decisive one, before the owner settles
    // or escalates. A racing writer must agree on the exact bound review.
    super::files::create_exact(
        files,
        &request.review_path(&review.principal),
        serde_json::to_vec(&review).map_err(storage)?,
        request.maximum_bytes(),
    )
    .await?;
    Ok(review)
}

fn storage(error: impl std::fmt::Display) -> Error {
    Error::Storage(error.to_string())
}
