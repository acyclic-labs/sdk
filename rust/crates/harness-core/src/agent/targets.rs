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

use acyclic_collections::{BinaryKeyError, FixedIndex, IndexReadWork, binary_key};
use serde::{Deserialize, Serialize};
use std::fmt;

// Selection borrows an immutable admission without an invocation work issuer.
// It allocates no index nodes and preserves this API's unbounded read policy.
struct RosterRead;

impl acyclic_collections::IndexReadWork for RosterRead {
    type Error = std::convert::Infallible;

    fn validate(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }

    fn examine(&mut self, _: u64) -> Result<(), Self::Error> {
        Ok(())
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
/// Clones retain this same immutable admission in constant time, without
/// allocating or copying records or canonical announcement bytes.
#[derive(Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[serde(try_from = "RosterWire")]
pub struct Roster {
    #[serde(flatten)]
    #[cfg_attr(
        feature = "wasm",
        tsify(type = "{ home: string; targets: TargetRecord[] }")
    )]
    admitted: std::sync::Arc<AdmittedRoster>,
}

impl fmt::Debug for Roster {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Roster")
            .field("home", &self.admitted.home)
            .field("targets", &self.admitted.targets)
            .finish()
    }
}

impl PartialEq for Roster {
    fn eq(&self, other: &Self) -> bool {
        self.admitted.home == other.admitted.home && self.admitted.targets == other.admitted.targets
    }
}

impl Eq for Roster {}

#[derive(Serialize)]
struct AdmittedRoster {
    home: String,
    targets: Vec<TargetRecord>,
    #[serde(skip)]
    home_position: usize,
    #[serde(skip)]
    canonical: Vec<u8>,
    #[serde(skip)]
    digest: [u8; 32],
    #[serde(skip)]
    names: FixedIndex<usize, crate::NativeCustody, { MAX_TARGET_NAME_BYTES + 8 }>,
    #[serde(skip)]
    owners: FixedIndex<usize, crate::NativeCustody, { MAX_OWNER_BYTES + 8 }>,
}

fn exact_key<const N: usize, W: IndexReadWork>(
    name: &str,
    work: &mut W,
) -> Result<Option<[u8; N]>, W::Error> {
    match binary_key(name.as_bytes(), work) {
        Ok(key) => Ok(Some(key)),
        Err(BinaryKeyError::Work(error)) => Err(error),
        Err(BinaryKeyError::Width(_) | BinaryKeyError::TooLong { .. }) => Ok(None),
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
        let mut names = FixedIndex::default();
        let mut owners = FixedIndex::default();
        let mut work = crate::NativeIndexWork(None);
        let index_error = |error| super::Error::Turn(Box::new(error));
        let mut home_position = None;
        for (position, target) in targets.iter().enumerate() {
            super::AgentRecord::check(target)?;
            let name = exact_key(&target.name, &mut work)
                .map_err(index_error)?
                .ok_or(super::Error::Record("target name exceeds its index width"))?;
            if *names
                .get_or_try_insert_with(name, &mut work, |_| Ok(position))
                .map_err(index_error)?
                != position
            {
                return Err(super::Error::Record("target name repeats in the roster"));
            }
            let owner = exact_key(&target.owner, &mut work)
                .map_err(index_error)?
                .ok_or(super::Error::Record("target owner exceeds its index width"))?;
            owners
                .get_or_try_insert_with(owner, &mut work, |_| Ok(position))
                .map_err(index_error)?;
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
            admitted: std::sync::Arc::new(AdmittedRoster {
                home: wire.home,
                targets: wire.targets,
                home_position,
                canonical: bytes,
                digest: *hasher.finalize().as_bytes(),
                names,
                owners,
            }),
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
        &self.admitted.home
    }

    /// Every target in attach order. Changes require admitting a new roster.
    pub fn targets(&self) -> &[TargetRecord] {
        &self.admitted.targets
    }

    /// The retained canonical announcement bytes and digest, admitted once
    /// with this immutable roster. Reading them neither encodes nor hashes.
    pub fn canonical(&self) -> (&[u8], [u8; 32]) {
        (&self.admitted.canonical, self.admitted.digest)
    }

    /// What a turn announces when its agent last announced `announced`: this
    /// roster's encoding and digest when they differ, else nothing.
    ///
    pub fn announcement(&self, announced: Option<[u8; 32]>) -> Option<(&[u8], [u8; 32])> {
        (announced != Some(self.admitted.digest)).then(|| self.canonical())
    }

    /// The target a call names: its home when `target` is omitted, the
    /// owner's first-attached device for `user:<owner>`, else the device of
    /// that name; refused when none matches or it is offline.
    /// Successful selection visits exact, fixed-width keys independently of
    /// the roster's population. A refusal lists every online target.
    ///
    /// # Errors
    /// The refusal the call settles with.
    pub fn select(&self, target: Option<&str>) -> Result<&TargetRecord, TargetRefusal> {
        self.select_position(target)
            .map(|position| &self.admitted.targets[position])
    }

    /// The position in this admitted roster, shared with its executor table.
    pub(crate) fn select_position(&self, target: Option<&str>) -> Result<usize, TargetRefusal> {
        let admitted = &self.admitted;
        let named = target.unwrap_or(&admitted.home);
        let found = match self.lookup(named, &mut RosterRead) {
            Ok(found) => found,
            Err(never) => match never {},
        };
        let refused = match found {
            Some(position) if admitted.targets[position].state == TargetState::Online => {
                return Ok(position);
            }
            Some(_) => Refused::Offline,
            None => Refused::Unknown,
        };
        Err(self.refusal(named, refused))
    }

    fn lookup<W: acyclic_collections::IndexReadWork>(
        &self,
        named: &str,
        work: &mut W,
    ) -> Result<Option<usize>, W::Error> {
        work.validate()?;
        if named == self.admitted.home {
            return Ok(Some(self.admitted.home_position));
        }
        if let Some(owner) = named.strip_prefix(USER_PREFIX) {
            let Some(key) = exact_key(owner, work)? else {
                return Ok(None);
            };
            self.admitted
                .owners
                .get(key, work)
                .map(|found| found.copied())
        } else {
            let Some(key) = exact_key(named, work)? else {
                return Ok(None);
            };
            self.admitted
                .names
                .get(key, work)
                .map(|found| found.copied())
        }
    }

    /// Describe a refusal from this same admitted roster. A known-lost target
    /// cannot be advertised as online while its presence update is in flight.
    pub(crate) fn refusal(&self, target: &str, refused: Refused) -> TargetRefusal {
        TargetRefusal {
            target: target.to_owned(),
            online: self
                .admitted
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
        let name = "z".repeat(MAX_TARGET_NAME_BYTES);
        let owner = format!("{}x", "é".repeat((MAX_OWNER_BYTES - 1) / 2));
        let wide = Roster::new(
            "pc",
            vec![
                device("pc", "root", TargetState::Online),
                device(&name, &owner, TargetState::Online),
            ],
        )
        .unwrap();
        for selector in [&name, &format!("user:{owner}")] {
            assert_eq!(wide.select(Some(selector)).unwrap().name, name);
        }
        for selector in [format!("{name}z"), format!("user:{owner}x")] {
            assert_eq!(
                wide.select(Some(&selector)).unwrap_err().refused,
                Refused::Unknown
            );
        }

        struct Reads(u64);
        impl acyclic_collections::IndexReadWork for Reads {
            type Error = &'static str;
            fn validate(&mut self) -> Result<(), Self::Error> {
                Ok(())
            }
            fn examine(&mut self, items: u64) -> Result<(), Self::Error> {
                self.0 += items;
                Ok(())
            }
        }
        let mut first = [0; 2];
        for count in [2, 4, 8, 16] {
            let records = (0..count)
                .map(|index| {
                    device(
                        &format!("t{index:02}"),
                        &format!("u{index:02}"),
                        TargetState::Online,
                    )
                })
                .collect();
            let admitted = Roster::new("t00", records).expect("original admission");
            for (kind, name) in [
                format!("t{:02}", count - 1),
                format!("user:u{:02}", count - 1),
            ]
            .iter()
            .enumerate()
            {
                let mut reads = Reads(0);
                assert_eq!(admitted.lookup(name, &mut reads), Ok(Some(count - 1)));
                eprintln!(
                    "roster lookup: records={count} selector={kind} examined={}",
                    reads.0
                );
                assert_eq!(
                    admitted.select(Some(name)).unwrap().name,
                    format!("t{:02}", count - 1)
                );
                if count == 2 {
                    first[kind] = reads.0;
                }
                assert!(
                    reads.0 <= first[kind] + 3,
                    "population-dependent lookup: {count} records, {} examinations",
                    reads.0
                );
            }
        }
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
        let roster = roster();
        let (bytes, digest) = roster.canonical();
        assert_eq!(
            bytes,
            crate::contract::canonical_json_bytes(&roster).expect("encode")
        );
        let decoded: Roster = serde_json::from_slice(bytes).expect("admit original records");
        assert_eq!(decoded.canonical(), (bytes, digest));
        assert!(std::ptr::eq(bytes, roster.canonical().0));
        let mut retained = None;
        let sharing = acyclic_test_support::measure(|| retained = Some(roster.clone()));
        assert_eq!(
            sharing.count_total, 0,
            "retain the admitted roster without copying"
        );
        let retained = retained.expect("retained snapshot");
        assert!(std::ptr::eq(bytes, retained.canonical().0));
        assert!(std::ptr::eq(roster.targets(), retained.targets()));
        assert!(std::ptr::eq(roster.home_name(), retained.home_name()));
        let copying = acyclic_test_support::measure(|| {
            std::hint::black_box(roster.targets().to_vec());
        });
        assert!(
            copying.count_total > sharing.count_total,
            "copy negative control"
        );
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
