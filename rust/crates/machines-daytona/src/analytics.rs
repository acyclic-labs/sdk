//! Daytona's read-only per-sandbox Analytics API, separate from provisional
//! allocation receipts. Source: https://www.daytona.io/docs/analytics-openapi.json.
//! Observations can lag and change; they are not a final invoice or proof that an
//! absent period costs zero. Consumers retain the original bytes and reconcile
//! monetary settlement separately from physical execution completion.
use crate::DaytonaConfig;
use acyclic_machines::ProviderError;
use reqwest::{
    Client, Url,
    header::{AUTHORIZATION, HeaderValue},
};
use serde::{Deserialize, Serialize};
use serde_json::{Number, Value};
use sha2::{Digest as _, Sha256};
use std::{collections::BTreeMap, time::Duration};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

/// Official read-only analytics origin; API credentials never follow redirects.
pub const DEFAULT_ANALYTICS_URL: &str = "https://analytics.app.daytona.io/";
const MAX_RESPONSE_BYTES: usize = 16 * 1024 * 1024;

/// Provider-reported usage period. All fields are required: incomplete records
/// must not become zero-valued allocation or monetary observations.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UsagePeriod {
    /// Provider's exact interval start, RFC3339.
    pub start_at: String,
    /// Provider's exact interval end, RFC3339.
    pub end_at: String,
    /// Reserved CPU quantity reported by the provider.
    pub cpu: u64,
    /// Reserved RAM quantity, GB, reported by the provider.
    #[serde(rename = "ramGB")]
    pub ram_gb: u64,
    /// Reserved disk quantity, GB, reported by the provider.
    #[serde(rename = "diskGB")]
    pub disk_gb: u64,
    /// GPU quantity reported by the provider.
    pub gpu: u64,
    /// Provider-reported price; no local tariff or allocation calculation is used.
    /// Monetary consumers must parse the retained raw bytes losslessly rather
    /// than perform binary floating-point arithmetic on this projection.
    pub price: Number,
    /// Preserve provider extensions without inventing billing interpretation.
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// One exact provider observation for a pinned organization, sandbox and query.
/// This type deliberately has no `final` or `settled` flag.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UsageObservation {
    /// Organization fixed when the protected client was installed.
    pub organization_id: String,
    /// Exact sandbox selected from the consumer's retained original receipt.
    pub sandbox_id: String,
    /// Exact requested RFC3339 lower bound.
    pub from: String,
    /// Exact requested RFC3339 upper bound.
    pub to: String,
    /// Actual HTTP request URL, including its encoded interval selectors.
    pub request_url: String,
    /// Actual successful HTTP status.
    pub status: u16,
    /// Selected provider provenance headers, never authorization material.
    pub headers: BTreeMap<String, String>,
    /// Exact response bytes, including the provider's original price lexemes.
    pub body: Vec<u8>,
    /// SHA-256 of those exact response bytes.
    pub body_sha256: [u8; 32],
    /// Parsed required records. Empty means no records observed, NOT zero cost.
    pub periods: Vec<UsagePeriod>,
}

/// Client bound to one configured organization and the official analytics host.
/// The consumer must independently pin sandbox ownership to its original create
/// receipt; arbitrary guest sandbox IDs must not reach this client.
pub struct AnalyticsApi {
    http: Client,
    base: Url,
    organization_id: String,
    authorization: HeaderValue,
}
fn invalid(message: &str) -> ProviderError {
    ProviderError::Invalid(message.to_owned())
}
fn rfc3339(value: &str) -> Result<OffsetDateTime, ProviderError> {
    OffsetDateTime::parse(value, &Rfc3339)
        .map_err(|_| invalid("analytics interval must be RFC3339"))
}
fn selector(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}
impl AnalyticsApi {
    /// Install a bounded, redirect-refusing client. Organization selection is
    /// protected configuration, not a request header or guest argument.
    ///
    /// # Errors
    /// Refuses absent organization, malformed credentials, or zero timeout.
    pub fn new(config: &DaytonaConfig) -> Result<Self, ProviderError> {
        let organization = config
            .organization_id
            .as_deref()
            .ok_or_else(|| invalid("analytics organization required"))?;
        Self::install(
            DEFAULT_ANALYTICS_URL,
            &config.api_key,
            organization,
            config.request_timeout,
        )
    }
    fn install(
        base: &str,
        key: &str,
        organization: &str,
        timeout: Duration,
    ) -> Result<Self, ProviderError> {
        if !selector(organization) || key.is_empty() || timeout.is_zero() {
            return Err(invalid("invalid analytics client configuration"));
        }
        let mut authorization = HeaderValue::from_str(&format!("Bearer {key}"))
            .map_err(|_| invalid("invalid analytics credential"))?;
        authorization.set_sensitive(true);
        let http = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(timeout)
            .build()
            .map_err(|_| ProviderError::Unavailable)?;
        let base = Url::parse(base).map_err(|_| invalid("invalid analytics origin"))?;
        Ok(Self {
            http,
            base,
            organization_id: organization.to_owned(),
            authorization,
        })
    }

    /// `GET /organization/{organizationId}/sandbox/{sandboxId}/usage?from=...&to=...`.
    /// Every observation retains exact provider bytes. There is no automatic
    /// redispatch, finality inference, aggregate fallback or guessed zero cost.
    ///
    /// # Errors
    /// Rejects invalid selectors/intervals, non-200 status, incomplete usage
    /// records, negative prices, overlapping/reversed periods or oversized data.
    pub async fn sandbox_usage(
        &self,
        sandbox_id: &str,
        from: &str,
        to: &str,
    ) -> Result<UsageObservation, ProviderError> {
        if !selector(sandbox_id) {
            return Err(invalid("invalid analytics sandbox selector"));
        }
        let start = rfc3339(from)?;
        let end = rfc3339(to)?;
        if start >= end {
            return Err(invalid("analytics interval must increase"));
        }
        let mut url = self
            .base
            .join(&format!(
                "organization/{}/sandbox/{sandbox_id}/usage",
                self.organization_id
            ))
            .map_err(|_| invalid("invalid analytics selector"))?;
        url.query_pairs_mut()
            .append_pair("from", from)
            .append_pair("to", to);
        let request_url = url.to_string();
        let mut response = self
            .http
            .get(url)
            .header(AUTHORIZATION, self.authorization.clone())
            .send()
            .await
            .map_err(|_| ProviderError::Unavailable)?;
        if response.status().as_u16() != 200 {
            return Err(match response.status().as_u16() {
                404 => ProviderError::NotFound("Daytona analytics sandbox usage not found".into()),
                429 | 500..=599 => ProviderError::Unavailable,
                _ => ProviderError::Rejected(format!(
                    "Daytona analytics HTTP {}",
                    response.status().as_u16()
                )),
            });
        }
        if response
            .content_length()
            .is_some_and(|bytes| bytes > MAX_RESPONSE_BYTES as u64)
        {
            return Err(ProviderError::Rejected(
                "Daytona analytics response too large".into(),
            ));
        }
        let mut headers = BTreeMap::new();
        for name in ["content-type", "date", "etag", "x-request-id"] {
            if let Some(value) = response.headers().get(name) {
                headers.insert(
                    name.to_owned(),
                    value
                        .to_str()
                        .map_err(|_| {
                            ProviderError::Rejected("invalid analytics response header".into())
                        })?
                        .to_owned(),
                );
            }
        }
        let mut body = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| ProviderError::Unavailable)?
        {
            if chunk.len() > MAX_RESPONSE_BYTES - body.len() {
                return Err(ProviderError::Rejected(
                    "Daytona analytics response too large".into(),
                ));
            }
            body.extend_from_slice(&chunk);
        }
        let periods: Vec<UsagePeriod> = serde_json::from_slice(&body).map_err(|_| {
            ProviderError::Rejected("invalid Daytona analytics usage records".into())
        })?;
        validate_periods(&periods, start, end)?;
        Ok(UsageObservation {
            organization_id: self.organization_id.clone(),
            sandbox_id: sandbox_id.to_owned(),
            from: from.to_owned(),
            to: to.to_owned(),
            request_url,
            status: 200,
            headers,
            body_sha256: Sha256::digest(&body).into(),
            body,
            periods,
        })
    }
}
fn validate_periods(
    periods: &[UsagePeriod],
    from: OffsetDateTime,
    to: OffsetDateTime,
) -> Result<(), ProviderError> {
    let mut intervals = Vec::with_capacity(periods.len());
    for period in periods {
        let start = rfc3339(&period.start_at)?;
        let end = rfc3339(&period.end_at)?;
        // Retain the provider's whole interval; never prorate a period straddling
        // a requested boundary. Non-intersecting records are selector failures.
        if start >= end || end <= from || start >= to || period.price.to_string().starts_with('-') {
            return Err(ProviderError::Rejected(
                "invalid Daytona analytics usage period".into(),
            ));
        }
        intervals.push((start, end));
    }
    intervals.sort_unstable();
    if intervals.windows(2).any(|pair| pair[0].1 > pair[1].0) {
        return Err(ProviderError::Rejected(
            "overlapping Daytona analytics usage periods".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::panic,
    reason = "deterministic analytics boundary tests"
)]
mod tests {
    use super::*;
    use crate::mock::Mock;
    const FROM: &str = "2026-10-10T00:00:00Z";
    const TO: &str = "2026-10-10T01:00:00Z";
    const PERIOD: &str = r#"[{"startAt":"2026-10-10T00:00:00Z","endAt":"2026-10-10T01:00:00Z","cpu":2,"ramGB":4,"diskGB":10,"gpu":0,"price":0.1234567890123456789}]"#;
    #[tokio::test]
    async fn retains_exact_sandbox_interval_and_provider_price_bytes() {
        let server = Mock::start(|request| async move {
            assert_eq!(request.method, "GET");
            assert_eq!(request.path, "/organization/org-1/sandbox/sandbox-1/usage");
            assert_eq!(request.authorization.as_deref(), Some("Bearer secret"));
            (200, PERIOD.to_owned())
        })
        .await;
        let api = AnalyticsApi::install(
            &format!("{}/", server.url),
            "secret",
            "org-1",
            Duration::from_secs(5),
        )
        .unwrap();
        let observation = api.sandbox_usage("sandbox-1", FROM, TO).await.unwrap();
        assert_eq!(observation.organization_id, "org-1");
        assert_eq!(observation.sandbox_id, "sandbox-1");
        assert_eq!(observation.from, FROM);
        assert_eq!(observation.to, TO);
        assert_eq!(
            observation.body_sha256,
            <[u8; 32]>::from(Sha256::digest(&observation.body))
        );
        assert_eq!(observation.body, PERIOD.as_bytes());
        assert_eq!(observation.periods[0].cpu, 2);
        let url = Url::parse(&observation.request_url).unwrap();
        assert_eq!(
            url.query_pairs()
                .collect::<BTreeMap<_, _>>()
                .get("from")
                .unwrap(),
            FROM
        );
        assert_eq!(
            url.query_pairs()
                .collect::<BTreeMap<_, _>>()
                .get("to")
                .unwrap(),
            TO
        );
    }
    #[tokio::test]
    async fn missing_usage_does_not_synthesize_zero_charge() {
        let server = Mock::start(|_| async { (200, "[]".to_owned()) }).await;
        let api = AnalyticsApi::install(
            &format!("{}/", server.url),
            "secret",
            "org-1",
            Duration::from_secs(5),
        )
        .unwrap();
        let observation = api.sandbox_usage("sandbox-1", FROM, TO).await.unwrap();
        assert_eq!(observation.body, b"[]");
        assert!(observation.periods.is_empty());
        let error = Mock::start(|_| async { (404, "{}".to_owned()) }).await;
        let api = AnalyticsApi::install(
            &format!("{}/", error.url),
            "secret",
            "org-1",
            Duration::from_secs(5),
        )
        .unwrap();
        assert!(matches!(
            api.sandbox_usage("sandbox-1", FROM, TO).await,
            Err(ProviderError::NotFound(_))
        ));
    }
    #[test]
    fn incomplete_and_overlapping_records_are_not_billable() {
        assert!(serde_json::from_str::<UsagePeriod>(r#"{"cpu":2,"price":0}"#).is_err());
        let mut periods: Vec<UsagePeriod> = serde_json::from_str(PERIOD).unwrap();
        periods.push(periods[0].clone());
        assert!(validate_periods(&periods, rfc3339(FROM).unwrap(), rfc3339(TO).unwrap()).is_err());
        periods.truncate(1);
        periods[0].price = serde_json::from_str("-1").unwrap();
        assert!(validate_periods(&periods, rfc3339(FROM).unwrap(), rfc3339(TO).unwrap()).is_err());
    }
}
