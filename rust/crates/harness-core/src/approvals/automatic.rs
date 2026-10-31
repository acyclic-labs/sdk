//! Automatic approvers shared by all host compositions.
use super::{ApprovalFiles, ApprovalRequest, ApprovalReviewFile, Approver, RuleVerdict};
use crate::{
    Result,
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
    automatic_review_with_io(models, files, request, approver, expected, limits, None).await
}

pub(super) async fn automatic_review_with_io(
    models: &BTreeMap<String, (Model, Arc<dyn ModelProvider>)>,
    files: &dyn ApprovalFiles,
    request: &ApprovalRequest<'_>,
    approver: &Approver,
    expected: ApprovalReviewFile,
    limits: Limits,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<ApprovalReviewFile> {
    super::io::step(io, 1)?;
    let principal = match approver {
        Approver::Rules { principal, .. } | Approver::Llm { principal, .. } => principal,
        _ => {
            return Err(super::io::error(
                io,
                crate::wire::ErrorCode::Invalid,
                "external approver requires a signed decision",
            ));
        }
    };
    if expected.principal != *principal
        || expected.request_sha256 != super::chain::fingerprint_with_io(request.bytes(), io)?
    {
        return Err(super::io::error(
            io,
            crate::wire::ErrorCode::Conflict,
            "automatic approval stage binding differs",
        ));
    }
    let (verdict, reason) = match approver {
        Approver::Rules {
            rules, fallback, ..
        } => {
            let interaction: Interaction = super::io::decode(request.bytes(), io)?;
            let verdict = match &interaction {
                Interaction::Approval { subject, .. } => {
                    super::ApprovalRule::evaluate_subject_with_io(rules, *fallback, subject, io)?
                }
                _ => RuleVerdict::Ask,
            };
            (
                verdict,
                Some(super::io::copy("configured approval rules", io)?),
            )
        }
        Approver::Llm { model, prompt, .. } => {
            let (model, provider) = models.get(model).ok_or_else(|| {
                super::io::error(
                    io,
                    crate::wire::ErrorCode::Unsupported,
                    "independent approver model is not bound",
                )
            })?;
            llm::review_model(
                files,
                request,
                &expected.principal,
                (model, provider.as_ref()),
                prompt,
                limits,
                io,
            )
            .await?
        }
        Approver::Human { .. } | Approver::Parent => {
            return Err(super::io::error(
                io,
                crate::wire::ErrorCode::Invalid,
                "external approver requires a signed decision",
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
    let bytes = super::io::encode(&review, request.maximum_bytes(), false, io)?;
    super::files::create_exact_with_io(
        files,
        &request.review_path_with_io(&review.principal, io)?,
        &bytes,
        request.maximum_bytes(),
        io,
    )
    .await?;
    Ok(review)
}
