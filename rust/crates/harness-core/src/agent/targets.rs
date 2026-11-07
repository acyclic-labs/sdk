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
//! The model learns the targets from the [`Roster`]. When its selected
//! history lacks the current roster, a turn announces it by an immutable file
//! on the agent's private volume, named by its conversation entry. History
//! windows and checkpoints preserve this by appending a new announcement
//! when needed; retained messages, system prompts and tool schemas stay fixed.

use serde::{Deserialize, Serialize};
use std::{fmt, sync::Arc};

use acyclic_collections::{FixedIndex, IndexReadWork, binary_key};

type NamePositions = FixedIndex<usize, crate::NativeCustody, { MAX_TARGET_NAME_BYTES + 8 }>;
type OwnerPositions = FixedIndex<usize, crate::NativeCustody, { MAX_OWNER_BYTES + 8 }>;

struct RosterPositions {
    names: NamePositions,
    owners: OwnerPositions,
}

impl RosterPositions {
    fn find<W: IndexReadWork>(&self, named: &str, work: &mut W) -> Result<Option<usize>, W::Error> {
        fn exact<W: IndexReadWork, const N: usize>(
            index: &FixedIndex<usize, crate::NativeCustody, N>,
            text: &str,
            work: &mut W,
        ) -> Result<Option<usize>, W::Error> {
            match binary_key(text.as_bytes(), work) {
                Ok(key) => index.get(key, work).map(|position| position.copied()),
                Err(acyclic_collections::BinaryKeyError::Work(error)) => Err(error),
                Err(acyclic_collections::BinaryKeyError::TooLong { .. }) => Ok(None),
                Err(acyclic_collections::BinaryKeyError::Width(_)) => {
                    unreachable!("roster widths include the original complete length")
                }
            }
        }
        match named.strip_prefix(USER_PREFIX) {
            Some(owner) => exact(&self.owners, owner, work),
            None => exact(&self.names, named, work),
        }
    }
}

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
/// Selection examines only the selected name's complete bounded byte domain;
/// cloning retains immutable storage without copying records or strings.
#[derive(Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[serde(try_from = "RosterWire")]
pub struct Roster {
    home: Arc<str>,
    targets: Arc<[TargetRecord]>,
    #[serde(skip)]
    home_position: usize,
    #[serde(skip)]
    canonical: Arc<[u8]>,
    #[serde(skip)]
    digest: [u8; 32],
    #[serde(skip)]
    positions: Arc<RosterPositions>,
}

impl PartialEq for Roster {
    fn eq(&self, other: &Self) -> bool {
        self.home == other.home && self.targets == other.targets
    }
}

impl Eq for Roster {}

impl fmt::Debug for Roster {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Roster")
            .field("home", &self.home)
            .field("targets", &self.targets)
            .finish()
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RosterWire {
    home: String,
    targets: Vec<TargetRecord>,
}

impl TryFrom<RosterWire> for Roster {
    type Error = super::Error;

    fn try_from(wire: RosterWire) -> Result<Self, Self::Error> {
        Self::new(&wire.home, wire.targets)
    }
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
        let mut positions = RosterPositions {
            names: NamePositions::default(),
            owners: OwnerPositions::default(),
        };
        let mut work = crate::NativeIndexWork(None);
        let mut home_position = None;
        for (position, target) in targets.iter().enumerate() {
            super::AgentRecord::check(target)?;
            let name = binary_key(target.name.as_bytes(), &mut work)
                .expect("validated target name fits its complete domain");
            let original = positions
                .names
                .get_or_try_insert_with(name, &mut work, |_| Ok(position))
                .expect("unbounded original roster admission");
            if *original != position {
                return Err(super::Error::Record("target name repeats in the roster"));
            }
            let owner = binary_key(target.owner.as_bytes(), &mut work)
                .expect("validated owner fits its complete domain");
            positions
                .owners
                .get_or_try_insert_with(owner, &mut work, |_| Ok(position))
                .expect("unbounded original roster admission");
            if target.name == home {
                home_position = Some(position);
            }
        }
        let home_position =
            home_position.ok_or(super::Error::Record("roster home is not a listed target"))?;
        let wire = RosterWire {
            home: home.to_owned(),
            targets,
        };
        let bytes = crate::contract::canonical_json_bytes(&wire)
            .map_err(|_| super::Error::Record("roster does not encode"))?;
        let mut hasher = blake3::Hasher::new_derive_key("acyclic.harness.targets.roster.v1");
        hasher.update(&bytes);
        Ok(Self {
            home: wire.home.into(),
            targets: wire.targets.into(),
            home_position,
            canonical: bytes.into(),
            digest: *hasher.finalize().as_bytes(),
            positions: Arc::new(positions),
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

    /// The target an omitted `target` names.
    pub fn home_name(&self) -> &str {
        &self.home
    }

    /// Every target in attach order. Changes require admitting a new roster.
    pub fn targets(&self) -> &[TargetRecord] {
        &self.targets
    }

    /// The retained canonical announcement bytes and digest, admitted once
    /// with this immutable roster. Reading them neither encodes nor hashes.
    pub fn canonical(&self) -> (&[u8], [u8; 32]) {
        (&self.canonical, self.digest)
    }

    /// What a turn announces when its agent last announced `announced`: this
    /// roster's encoding and digest when they differ, else nothing.
    ///
    pub fn announcement(&self, announced: Option<[u8; 32]>) -> Option<(&[u8], [u8; 32])> {
        (announced != Some(self.digest)).then(|| self.canonical())
    }

    /// The target a call names: its home when `target` is omitted, the
    /// owner's first-attached device for `user:<owner>`, else the device of
    /// that name; refused when none matches or it is offline.
    ///
    /// # Errors
    /// The refusal the call settles with.
    pub fn select(&self, target: Option<&str>) -> Result<&TargetRecord, TargetRefusal> {
        self.select_position(target)
            .map(|position| &self.targets[position])
    }

    /// The position in this admitted roster, shared with its executor table.
    pub(crate) fn select_position(&self, target: Option<&str>) -> Result<usize, TargetRefusal> {
        let named = target.unwrap_or(&self.home);
        let found = if named == &*self.home {
            Some(self.home_position)
        } else {
            self.positions
                .find(named, &mut crate::NativeIndexWork(None))
                .expect("unbounded original roster read")
        };
        let refused = match found {
            Some(position) if self.targets[position].state == TargetState::Online => {
                return Ok(position);
            }
            Some(_) => Refused::Offline,
            None => Refused::Unknown,
        };
        Err(self.refusal(named, refused))
    }

    /// Describe a refusal from this same admitted roster. A known-lost target
    /// cannot be advertised as online while its presence update is in flight.
    pub(crate) fn refusal(&self, target: &str, refused: Refused) -> TargetRefusal {
        TargetRefusal {
            target: target.to_owned(),
            online: self
                .targets
                .iter()
                .filter(|record| {
                    record.state == TargetState::Online
                        && (record.name != target || !matches!(refused, Refused::Offline))
                })
                .map(|record| record.name.clone())
                .collect(),
            refused,
        }
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

    #[test]
    fn admitted_roster_retains_exact_announcement_across_decode_and_changes() {
        #[derive(Default)]
        struct Examinations(u64);
        impl IndexReadWork for Examinations {
            type Error = std::convert::Infallible;
            fn validate(&mut self) -> Result<(), Self::Error> {
                Ok(())
            }
            fn examine(&mut self, items: u64) -> Result<(), Self::Error> {
                self.0 += items;
                Ok(())
            }
        }
        let roster = roster();
        let (bytes, digest) = roster.canonical();
        assert_eq!(
            bytes,
            crate::contract::canonical_json_bytes(&roster).expect("encode")
        );
        let decoded: Roster = serde_json::from_slice(bytes).expect("admit original records");
        assert_eq!(decoded.canonical(), (bytes, digest));
        for count in [1, 100_000] {
            let original = Roster::new(
                "host-0",
                (0..count)
                    .map(|position| device(&format!("host-{position}"), "ana", TargetState::Online))
                    .collect(),
            )
            .expect("admit original snapshot");
            let allocation = acyclic_test_support::measure(|| {
                drop(original.clone());
            });
            assert_eq!(
                allocation.count_total, 0,
                "immutable roster clone allocated"
            );
            let retained = original.clone();
            assert!(std::ptr::eq(original.home_name(), retained.home_name()));
            assert!(std::ptr::eq(original.targets(), retained.targets()));
            assert!(std::ptr::eq(original.canonical().0, retained.canonical().0));
            let name = &retained.targets()[count - 1].name;
            let selected = acyclic_test_support::measure(|| {
                assert_eq!(retained.select(Some(name)).unwrap().name, *name);
                assert_eq!(retained.select(Some("user:ana")).unwrap().name, "host-0");
            });
            assert_eq!(selected.count_total, 0);
            for (query, expected, width) in [
                (name.as_str(), count - 1, MAX_TARGET_NAME_BYTES + 8),
                ("user:ana", 0, MAX_OWNER_BYTES + 8),
            ] {
                let mut examined = Examinations::default();
                assert_eq!(
                    retained.positions.find(query, &mut examined).unwrap(),
                    Some(expected)
                );
                // Complete encoding, strictly increasing radix bits and one
                // exact leaf comparison, independently of roster population.
                assert!(examined.0 <= (10 * width + 1) as u64);
            }
            if count == 100_000 {
                let mut scanned = 0;
                assert_eq!(
                    retained.targets().iter().position(|record| {
                        scanned += 1;
                        record.name == *name
                    }),
                    Some(count - 1)
                );
                assert!(scanned > 10 * (MAX_TARGET_NAME_BYTES + 8) + 1);
            }
            drop(original);
            assert_eq!(retained.select(None).unwrap().name, "host-0");
        }
        // An announced roster is not announced again; any other one is.
        let first = roster.announcement(None).expect("announced");
        assert_eq!(roster.announcement(Some(first.1)), None);
        assert!(roster.announcement(Some([0; 32])).is_some());
        // Negative control: any visible change is a new announcement.
        let mut targets = roster.targets().to_vec();
        targets[1].state = TargetState::Online;
        let moved = Roster::new(roster.home_name(), targets).expect("new admission");
        assert_ne!(digest, moved.canonical().1);
        let rehomed = Roster::new("ci", roster.targets().to_vec()).expect("new home");
        assert_ne!(digest, rehomed.canonical().1);
        // Cached state is not accepted from serialized input; record admission
        // cannot be bypassed by deserialization either.
        let mut wire: serde_json::Value = serde_json::from_slice(bytes).expect("real encoding");
        wire["digest"] = serde_json::json!(vec![0; 32]);
        assert!(serde_json::from_value::<Roster>(wire).is_err());
        for invalid in [
            serde_json::json!({"home":"absent", "targets":roster.targets()}),
            serde_json::json!({"home":"laptop", "targets":[roster.targets()[0],roster.targets()[0]]}),
        ] {
            assert!(serde_json::from_value::<Roster>(invalid).is_err());
        }
    }
}
