//! One child-page tail selector, shared by storage and reply authorization.
use crate::StreamPath;
use std::borrow::Cow;

/// Select a child's own stream or a relative stream within its subtree.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ChildTail {
    /// Project the child stream itself at the page's authority cut.
    Child,
    /// Project one canonical relative stream under the child.
    Relative(StreamPath),
}

impl ChildTail {
    /// Canonical optional-string wire selector. Presence with an empty string
    /// selects the child itself; a nonempty value selects a relative stream.
    pub fn as_str(&self) -> &str {
        match self {
            Self::Child => "",
            Self::Relative(path) => path.as_str(),
        }
    }

    /// Resolve once for both the provider's projection and its authorization.
    /// A relative path beyond the protective path bound cannot exist and has
    /// no tail; it never causes an otherwise valid child page to fail.
    pub fn target<'a>(&self, child: &'a StreamPath) -> Option<Cow<'a, str>> {
        match self {
            Self::Child => Some(Cow::Borrowed(child.as_str())),
            Self::Relative(relative) => {
                if child.as_str().len() + 1 + relative.as_str().len() > crate::MAX_PATH_BYTES {
                    return None;
                }
                // Both components already have canonical grammar. Joining them
                // needs only the combined protective length check above.
                Some(Cow::Owned(format!("{child}/{relative}")))
            }
        }
    }
}

impl From<StreamPath> for ChildTail {
    fn from(path: StreamPath) -> Self {
        Self::Relative(path)
    }
}

impl std::fmt::Display for ChildTail {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}
