//! Cloudflare-style Worker route grammar and selection, shared with Edge vectors.
use super::{CanonicalHost, DnsError, validate_dns_label};

/// Malformed route declaration, request URL or an ambiguous route table.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RouteError {
    /// The route contains an unsupported wildcard, scheme, query or host.
    #[error("invalid Worker route pattern")]
    Pattern,
    /// Routing accepts absolute HTTP(S) URLs without credentials or fragments.
    #[error("invalid Worker request URL")]
    Url,
    /// Two equally specific matches name different targets.
    #[error("ambiguous Worker routes")]
    Ambiguous,
    /// A route target must be a valid named Worker; only None means exclusion.
    #[error("invalid Worker route target")]
    Target,
}
impl From<DnsError> for RouteError {
    fn from(_: DnsError) -> Self {
        Self::Pattern
    }
}

/// Parsed route: optional scheme, leading host wildcard and trailing path wildcard.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RoutePattern {
    canonical: String,
    scheme: Option<String>,
    host: String,
    host_wildcard: bool,
    path: String,
    path_wildcard: bool,
}
impl RoutePattern {
    /// Admit exactly the supported CF route grammar, normalizing only host case.
    pub fn parse(value: &str) -> Result<Self, RouteError> {
        if value.is_empty()
            || value.len() > 4096
            || value.contains(['?', '#', '\\'])
            || value.bytes().any(|b| b.is_ascii_control() || b == b' ')
        {
            return Err(RouteError::Pattern);
        }
        let (scheme, rest) = if let Some(rest) = value.strip_prefix("https://") {
            (Some("https"), rest)
        } else if let Some(rest) = value.strip_prefix("http://") {
            (Some("http"), rest)
        } else if value.contains("://") {
            return Err(RouteError::Pattern);
        } else {
            (None, value)
        };
        let (host, path) = rest
            .split_once('/')
            .map_or((rest, "/".to_owned()), |(host, path)| {
                (host, format!("/{path}"))
            });
        let host_wildcard = host.starts_with('*');
        let host = if host_wildcard {
            host.strip_prefix('*').ok_or(RouteError::Pattern)?
        } else {
            host
        };
        let host = host.to_ascii_lowercase();
        if host.contains('*') || (!host_wildcard && host.is_empty()) {
            return Err(RouteError::Pattern);
        }
        if !host.is_empty() {
            let label_host = host.strip_prefix('.').unwrap_or(&host);
            if label_host.is_empty() || (!host_wildcard && host.starts_with('.')) {
                return Err(RouteError::Pattern);
            }
            CanonicalHost::parse(label_host)?;
        }
        let path_wildcard = path.ends_with('*');
        let path = if path_wildcard {
            path.strip_suffix('*')
                .ok_or(RouteError::Pattern)?
                .to_owned()
        } else {
            path
        };
        if path.contains('*') {
            return Err(RouteError::Pattern);
        }
        let canonical = format!(
            "{}{}{}{}{}",
            scheme.map_or(String::new(), |s| format!("{s}://")),
            if host_wildcard { "*" } else { "" },
            host,
            path,
            if path_wildcard { "*" } else { "" }
        );
        Ok(Self {
            canonical,
            scheme: scheme.map(str::to_owned),
            host,
            host_wildcard,
            path,
            path_wildcard,
        })
    }
    /// Canonical configuration spelling (host case is insignificant for routes).
    pub fn as_str(&self) -> &str {
        &self.canonical
    }
    /// Match path including its query string, without decoding or case folding it.
    pub fn matches(&self, value: &str) -> Result<bool, RouteError> {
        let url = url::Url::parse(value).map_err(|_| RouteError::Url)?;
        if !matches!(url.scheme(), "http" | "https")
            || !url.username().is_empty()
            || url.password().is_some()
            || url.fragment().is_some()
        {
            return Err(RouteError::Url);
        }
        let host = url.host_str().ok_or(RouteError::Url)?;
        let host_matches = if self.host_wildcard {
            host.ends_with(&self.host)
        } else {
            host == self.host
        };
        let path = format!(
            "{}{}",
            url.path(),
            url.query().map_or(String::new(), |q| format!("?{q}"))
        );
        Ok(host_matches
            && self.scheme.as_ref().is_none_or(|s| s == url.scheme())
            && if self.path_wildcard {
                path.starts_with(&self.path)
            } else {
                path == self.path
            })
    }
    fn specificity(&self) -> (bool, usize, usize, bool, bool) {
        (
            !self.host_wildcard,
            self.host.len(),
            self.path.len(),
            !self.path_wildcard,
            self.scheme.is_some(),
        )
    }
}

/// A route may deliberately suppress a broader Worker route by omitting target.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkerRoute {
    /// Admitted route grammar.
    pub pattern: RoutePattern,
    /// Named script, or no Worker for a route exclusion.
    pub target: Option<String>,
}
/// Select the most specific match; an exclusion is represented by Some(None).
pub fn select_route<'a>(
    routes: &'a [WorkerRoute],
    url: &str,
) -> Result<Option<Option<&'a str>>, RouteError> {
    let mut best: Option<&WorkerRoute> = None;
    let mut ambiguous = false;
    for route in routes {
        if let Some(target) = &route.target {
            validate_dns_label(target.as_bytes()).map_err(|_| RouteError::Target)?;
        }
        if !route.pattern.matches(url)? {
            continue;
        }
        match best {
            Some(old) if old.pattern.specificity() > route.pattern.specificity() => {}
            Some(old) if old.pattern.specificity() == route.pattern.specificity() => {
                ambiguous |= old.target != route.target;
            }
            _ => {
                best = Some(route);
                ambiguous = false;
            }
        }
    }
    if ambiguous {
        return Err(RouteError::Ambiguous);
    }
    Ok(best.map(|route| route.target.as_deref()))
}
