use super::*;

/// One principal's verdict about one exact approval request.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalVerdict {
    /// The exact bound action may run.
    Approve,
    /// The exact bound action must not run.
    Deny,
}

/// Immutable `approvals/<id>/decisions/<principal>.json` body.
///
/// The interaction owner stages these exact bytes as the approval resolution's
/// detail, and admission binds them to the request, version and resolving scope.
/// The task file is a verbatim copy of that detail, never a second record.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovalDecisionFile {
    /// Authenticated scope identity that resolved the request.
    pub principal: String,
    /// Lowercase hex SHA-256 of the exact admitted request bytes.
    pub request_sha256: String,
    /// Interaction version this decision resolved.
    pub expected_version: u64,
    /// The principal's verdict.
    pub verdict: ApprovalVerdict,
    /// Participant explanation.
    pub reason: Option<String>,
}

impl ApprovalDecisionFile {
    /// Builds the decision that `principal` gives for a request digest.
    pub fn new(
        principal: &str,
        request_sha256: &[u8; 32],
        expected_version: u64,
        approved: bool,
        reason: Option<String>,
    ) -> Result<Self> {
        let value = Self {
            principal: principal.into(),
            request_sha256: sha256_hex(request_sha256),
            expected_version,
            verdict: if approved {
                ApprovalVerdict::Approve
            } else {
                ApprovalVerdict::Deny
            },
            reason,
        };
        value.validate()?;
        Ok(value)
    }

    /// Decodes a client-written body before its owner admits it.
    pub fn decode(bytes: &[u8], maximum_bytes: u64) -> Result<Self> {
        if bytes.len() as u64 > maximum_bytes {
            return Err(Error::Invalid(
                "approval decision exceeds its file bound".into(),
            ));
        }
        let value: Self = crate::contract::json_from_slice(bytes)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        value.validate()?;
        Ok(value)
    }

    /// Rejects bodies that name no exact request, version or principal.
    pub fn validate(&self) -> Result<()> {
        if self.expected_version == 0
            || self.principal.is_empty()
            || self.principal.len() > 255
            || self.request_sha256.len() != 64
            || !self
                .request_sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            || self.request_sha256.bytes().all(|byte| byte == b'0')
        {
            return Err(Error::Invalid(
                "approval decision has no exact request binding".into(),
            ));
        }
        Ok(())
    }

    /// Whether this decision approves its request.
    #[must_use]
    pub const fn approved(&self) -> bool {
        matches!(self.verdict, ApprovalVerdict::Approve)
    }

    /// Whether this body decides the request with these exact bytes and version.
    #[must_use]
    pub fn binds(&self, request_sha256: &[u8; 32], expected_version: u64) -> bool {
        self.expected_version == expected_version
            && self.request_sha256 == sha256_hex(request_sha256)
    }

    /// Canonical file component naming this principal's decision.
    #[must_use]
    pub fn principal_file(principal: &str) -> String {
        principal_hex(&Self::principal_digest(principal))
    }

    pub(super) fn principal_digest(principal: &str) -> [u8; 16] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"harness/approval-principal\0");
        hasher.update(principal.as_bytes());
        let mut digest = [0; 16];
        digest.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
        digest
    }
}

/// Lowercase hex of a principal file digest.
pub(crate) fn principal_hex(digest: &[u8; 16]) -> String {
    hex(digest)
}

fn sha256_hex(bytes: &[u8; 32]) -> String {
    hex(bytes)
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    bytes
        .iter()
        .flat_map(|byte| {
            [
                DIGITS[usize::from(byte >> 4)],
                DIGITS[usize::from(byte & 15)],
            ]
        })
        .map(char::from)
        .collect()
}
