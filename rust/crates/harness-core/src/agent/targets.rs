//! Execution targets: the machines an agent's `shell` commands run on.
//!
//! Every target is a Machine of one [`TargetClass`]. The model names one in
//! each `shell` call (`target`); an omitted target is the agent's home, and
//! `user:<name>` is that user's home device, the first one they attached.
//! There is never a fallback to another target: a call naming a target that
//! is unknown or offline is refused with [`TargetRefusal`], and the run goes
//! on. Whether a command runs on a listed target is decided by execution
//! policy alone (the approval subject carries the resolved device and its
//! owner), never by visibility.
//!
//! The model learns the targets from the [`Roster`]: a turn whose roster
//! digest differs from the one the agent last announced stages the roster as
//! a file on the agent's private volume and its conversation entry names it
//! (the agent's live `targets` file), so the system prompt and tool schemas
//! never change and an unchanged roster costs nothing.

use serde::{Deserialize, Serialize};
use std::fmt;

/// The `target` prefix that names a user's home device instead of a device.
pub const USER_PREFIX: &str = "user:";
/// Longest target name, in bytes.
pub const MAX_TARGET_NAME_BYTES: usize = 64;
/// Longest owner name, in bytes.
pub const MAX_OWNER_BYTES: usize = 255;

/// Who manages a target's lifecycle.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[serde(rename_all = "snake_case")]
pub enum TargetClass {
    /// A user's device: the harness only runs commands on it and observes
    /// its presence; destroying it detaches it.
    Attached,
    /// A cloud Machine the harness creates, suspends, wakes, forks and destroys.
    Provisioned,
}

/// How a target spells filesystem paths.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[serde(rename_all = "snake_case")]
pub enum PathStyle {
    /// `/`-separated paths (Linux, macOS).
    Posix,
    /// Drive letters and `\`-separated paths.
    Windows,
}

impl PathStyle {
    /// The path style of a shell contract's host.
    #[must_use]
    pub fn of(shell: &acyclic_host_platform::shell::ShellSpec) -> Self {
        if shell.windows_paths {
            Self::Windows
        } else {
            Self::Posix
        }
    }
}

/// What kind of machine a target is.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[serde(rename_all = "snake_case")]
pub enum DeviceKind {
    /// A user's workstation or laptop.
    Desktop,
    /// A server.
    Server,
    /// A virtual machine.
    VirtualMachine,
    /// A container.
    Container,
}

/// Whether a target takes commands now.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[serde(rename_all = "snake_case")]
pub enum TargetState {
    /// Connected and taking commands.
    Online,
    /// Listed, but not connected: calls naming it are refused.
    Offline,
}

/// One execution target as every agent of the tree sees it (v1).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[serde(deny_unknown_fields)]
pub struct TargetRecord {
    /// Stable name, unique within the agent tree; what `target` names.
    pub name: String,
    /// The user who owns the device and whose execution policy guards it.
    pub owner: String,
    /// Who manages its lifecycle.
    pub class: TargetClass,
    /// Operating system, as Rust's `std::env::consts::OS` names it.
    pub os: String,
    /// Command language of its `shell`: `bash` or `powershell`.
    pub shell: String,
    /// How its paths are spelled.
    pub path_style: PathStyle,
    /// What kind of machine it is.
    pub device_kind: DeviceKind,
    /// Whether it takes commands now.
    pub state: TargetState,
}

impl super::AgentRecord for TargetRecord {
    fn check(&self) -> Result<(), super::Error> {
        check_name(&self.name)?;
        if self.owner.is_empty()
            || self.owner.len() > MAX_OWNER_BYTES
            || self.owner.chars().any(char::is_control)
        {
            return Err(super::Error::Record("target owner is not a user name"));
        }
        if self.os.is_empty() || self.shell.is_empty() {
            return Err(super::Error::Record("target platform is incomplete"));
        }
        Ok(())
    }
}

/// Names are lowercase ASCII words, so `user:` can never name a device.
fn check_name(name: &str) -> Result<(), super::Error> {
    let mut bytes = name.bytes();
    if name.len() <= MAX_TARGET_NAME_BYTES
        && bytes
            .next()
            .is_some_and(|first| first.is_ascii_lowercase() || first.is_ascii_digit())
        && bytes.all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_' | b'.')
        })
    {
        Ok(())
    } else {
        Err(super::Error::Record("target name is not a lowercase word"))
    }
}

/// Every target visible to an agent, in attach order, and its home.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[serde(deny_unknown_fields)]
pub struct Roster {
    /// The target an omitted `target` names.
    pub home: String,
    /// Every target connected to the tree, each with its owner.
    pub targets: Vec<TargetRecord>,
}

/// Why a call's target cannot take it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Refused {
    /// No listed target has that name, or the user has no device.
    Unknown,
    /// The target is listed but not connected.
    Offline,
    /// The device declined to run the command, for this reason: its
    /// owner's policy refused it, or its grant did not verify there.
    Declined(String),
}

/// A call refused because of its target; the model reads exactly this text
/// after `tool execution failed: ` and may pick another target.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TargetRefusal {
    /// The target the call named, as it named it.
    pub target: String,
    /// Why it was refused.
    pub refused: Refused,
    /// The targets that are online, in roster order.
    pub online: Vec<String>,
}

impl std::error::Error for TargetRefusal {}

impl fmt::Display for TargetRefusal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = match &self.refused {
            Refused::Unknown => "unknown",
            Refused::Offline => "offline",
            Refused::Declined(reason) => {
                return write!(
                    formatter,
                    "target '{}' declined the command: {reason}",
                    self.target
                );
            }
        };
        write!(formatter, "target '{}' is {state}; online: ", self.target)?;
        if self.online.is_empty() {
            return formatter.write_str("none");
        }
        formatter.write_str(&self.online.join(", "))
    }
}

impl Roster {
    /// The roster of `targets` whose home is `home`: names are unique and
    /// the home is listed.
    ///
    /// # Errors
    /// When a record is malformed, a name repeats or the home is unlisted.
    pub fn new(home: &str, targets: Vec<TargetRecord>) -> Result<Self, super::Error> {
        let mut names = std::collections::BTreeSet::new();
        for target in &targets {
            super::AgentRecord::check(target)?;
            if !names.insert(target.name.as_str()) {
                return Err(super::Error::Record("target name repeats in the roster"));
            }
        }
        if !names.contains(home) {
            return Err(super::Error::Record("roster home is not a listed target"));
        }
        Ok(Self {
            home: home.to_owned(),
            targets,
        })
    }

    /// The roster of one in-process home target.
    ///
    /// # Errors
    /// When the record is malformed.
    pub fn home(target: TargetRecord) -> Result<Self, super::Error> {
        let home = target.name.clone();
        Self::new(&home, vec![target])
    }

    /// The canonical encoding an announcement stages, and its digest.
    ///
    /// # Errors
    /// When the roster does not encode.
    pub fn canonical(&self) -> Result<(Vec<u8>, [u8; 32]), super::Error> {
        let bytes = crate::contract::canonical_json_bytes(self)
            .map_err(|_| super::Error::Record("roster does not encode"))?;
        let mut hasher = blake3::Hasher::new_derive_key("acyclic.harness.targets.roster.v1");
        hasher.update(&bytes);
        let digest = *hasher.finalize().as_bytes();
        Ok((bytes, digest))
    }

    /// What a turn announces when its agent last announced `announced`: this
    /// roster's encoding and digest when they differ, else nothing.
    ///
    /// # Errors
    /// When the roster does not encode.
    pub fn announcement(
        &self,
        announced: Option<[u8; 32]>,
    ) -> Result<Option<(Vec<u8>, [u8; 32])>, super::Error> {
        let (bytes, digest) = self.canonical()?;
        Ok((announced != Some(digest)).then_some((bytes, digest)))
    }

    /// The target a call names: its home when `target` is omitted, the
    /// owner's first-attached device for `user:<owner>`, else the device of
    /// that name; refused when none matches or it is offline.
    ///
    /// # Errors
    /// The refusal the call settles with.
    pub fn select(&self, target: Option<&str>) -> Result<&TargetRecord, TargetRefusal> {
        let named = target.unwrap_or(&self.home);
        let found = match named.strip_prefix(USER_PREFIX) {
            Some(owner) => self.targets.iter().find(|record| record.owner == owner),
            None => self.targets.iter().find(|record| record.name == named),
        };
        let refused = match found {
            Some(record) if record.state == TargetState::Online => return Ok(record),
            Some(_) => Refused::Offline,
            None => Refused::Unknown,
        };
        Err(TargetRefusal {
            target: named.to_owned(),
            refused,
            online: self
                .targets
                .iter()
                .filter(|record| record.state == TargetState::Online)
                .map(|record| record.name.clone())
                .collect(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn device(name: &str, owner: &str, state: TargetState) -> TargetRecord {
        TargetRecord {
            name: name.into(),
            owner: owner.into(),
            class: TargetClass::Attached,
            os: "linux".into(),
            shell: "bash".into(),
            path_style: PathStyle::Posix,
            device_kind: DeviceKind::Desktop,
            state,
        }
    }

    fn roster() -> Roster {
        Roster::new(
            "laptop",
            vec![
                device("laptop", "ana", TargetState::Online),
                device("desk", "bo", TargetState::Offline),
                device("ci", "bo", TargetState::Online),
            ],
        )
        .expect("valid roster")
    }

    #[test]
    fn resolution_selects_home_names_and_user_homes_without_fallback() {
        let roster = roster();
        assert_eq!(roster.select(None).map(|t| &*t.name), Ok("laptop"));
        assert_eq!(roster.select(Some("ci")).map(|t| &*t.name), Ok("ci"));
        assert_eq!(
            roster.select(Some("user:ana")).map(|t| &*t.name),
            Ok("laptop")
        );
        // A user's home is their first-attached device; it is offline, and
        // their other online device is never substituted for it.
        let refused = roster.select(Some("user:bo")).expect_err("offline home");
        assert_eq!(
            refused.to_string(),
            "target 'user:bo' is offline; online: laptop, ci"
        );
        assert_eq!(
            roster.select(Some("pc")).expect_err("unknown").to_string(),
            "target 'pc' is unknown; online: laptop, ci"
        );
        assert_eq!(
            roster.select(Some("desk")).expect_err("offline").refused,
            Refused::Offline
        );
        let alone = Roster::home(device("pc", "ana", TargetState::Offline)).expect("valid");
        assert_eq!(
            alone.select(None).expect_err("offline").to_string(),
            "target 'pc' is offline; online: none"
        );
    }

    #[test]
    fn rosters_refuse_ambiguous_or_unlisted_names() {
        let twice = vec![
            device("pc", "ana", TargetState::Online),
            device("pc", "bo", TargetState::Online),
        ];
        assert!(Roster::new("pc", twice).is_err());
        assert!(Roster::new("pc", vec![device("ci", "ana", TargetState::Online)]).is_err());
        for name in [
            "",
            "user:ana",
            "PC",
            "-pc",
            &"a".repeat(MAX_TARGET_NAME_BYTES + 1),
        ] {
            assert!(
                Roster::home(device(name, "ana", TargetState::Online)).is_err(),
                "{name}"
            );
        }
    }

    fn digest(roster: &Roster) -> Option<[u8; 32]> {
        roster.canonical().ok().map(|(_, digest)| digest)
    }

    #[test]
    fn digest_changes_exactly_with_the_roster() {
        let roster = roster();
        assert_eq!(roster.canonical().ok(), roster.clone().canonical().ok());
        // An announced roster is not announced again; any other one is.
        let first = roster.announcement(None).ok().flatten().expect("announced");
        assert_eq!(roster.announcement(Some(first.1)).ok(), Some(None));
        assert!(roster.announcement(Some([0; 32])).ok().flatten().is_some());
        // Negative control: any visible change is a new announcement.
        let mut moved = roster.clone();
        moved.targets[1].state = TargetState::Online;
        assert_ne!(digest(&roster), digest(&moved));
        let mut rehomed = roster.clone();
        rehomed.home = "ci".into();
        assert_ne!(digest(&roster), digest(&rehomed));
    }
}
