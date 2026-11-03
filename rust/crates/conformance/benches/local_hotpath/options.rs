use acyclic_test_support::scale::{CORE_OPERATIONS, Operation, POPULATIONS};
use std::path::PathBuf;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

pub(super) struct Options {
    pub(super) root: PathBuf,
    pub(super) samples: usize,
    pub(super) source: String,
    pub(super) operations: Vec<Operation>,
    pub(super) populations: Vec<u64>,
    pub(super) setup_credit: usize,
    pub(super) proofs: bool,
    pub(super) observe_durability: bool,
}

impl Options {
    pub(super) fn recovery(&self) -> String {
        fn quoted(value: &str) -> String {
            if cfg!(windows) {
                format!("'{}'", value.replace('\'', "''"))
            } else {
                format!("'{}'", value.replace('\'', "'\\''"))
            }
        }
        let mut root = self.root.as_os_str().to_owned();
        root.push(".retry");
        if self.proofs {
            return format!(
                "cargo bench --locked -p acyclic-conformance --features local-hotpath-proofs --bench local_hotpath -- --proofs --root {} --source {} --operations 'swarm.session_start'",
                quoted(&root.to_string_lossy()),
                quoted(&self.source),
            );
        }
        format!(
            "cargo bench --locked -p acyclic-conformance --features local-hotpath --bench local_hotpath -- --root {} --source {} --samples {} --setup-credit {} --operations {} --populations {}{}",
            quoted(&root.to_string_lossy()),
            quoted(&self.source),
            self.samples,
            self.setup_credit,
            quoted(
                &self
                    .operations
                    .iter()
                    .map(|operation| operation.name())
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            quoted(
                &self
                    .populations
                    .iter()
                    .map(u64::to_string)
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            if self.observe_durability {
                " --observe-durability"
            } else {
                ""
            },
        )
    }

    pub(super) fn parse_from(arguments: impl IntoIterator<Item = String>) -> Result<Self> {
        let mut arguments = arguments.into_iter();
        let mut root = None;
        let mut samples = 200;
        let mut source = None;
        let mut operations = CORE_OPERATIONS.to_vec();
        let mut populations = POPULATIONS.to_vec();
        let mut setup_credit = 1;
        let mut proofs = false;
        let mut observe_durability = false;
        while let Some(argument) = arguments.next() {
            match argument.as_str() {
                "--proofs" => proofs = true,
                "--observe-durability" => observe_durability = true,
                "--root" => root = arguments.next().map(PathBuf::from),
                "--samples" => samples = arguments.next().ok_or("missing sample count")?.parse()?,
                "--source" => source = arguments.next(),
                "--setup-credit" => {
                    setup_credit = arguments.next().ok_or("missing setup credit")?.parse()?;
                    if setup_credit == 0 {
                        return Err("setup credit must be positive".into());
                    }
                }
                "--operations" => {
                    let names = arguments
                        .next()
                        .ok_or("missing comma-separated operation names")?;
                    operations.clear();
                    for name in names.split(',') {
                        let operation = CORE_OPERATIONS
                            .iter()
                            .copied()
                            .find(|operation| operation.name() == name)
                            .ok_or_else(|| format!("unknown core operation {name}"))?;
                        if operations.contains(&operation) {
                            return Err(format!("duplicate operation {name}").into());
                        }
                        operations.push(operation);
                    }
                }
                "--populations" => {
                    let sizes = arguments
                        .next()
                        .ok_or("missing comma-separated populations")?;
                    populations.clear();
                    for size in sizes.split(',') {
                        let agents = size.parse()?;
                        if !POPULATIONS.contains(&agents) {
                            return Err(format!("unknown qualification population {agents}").into());
                        }
                        if populations.contains(&agents) {
                            return Err(format!("duplicate population {agents}").into());
                        }
                        populations.push(agents);
                    }
                }
                // Cargo passes this flag to harness=false benchmarks.
                "--bench" => {}
                _ => return Err(format!("unknown option {argument}").into()),
            }
        }
        if !proofs && samples == 0 {
            return Err("measurement requires at least one observation".into());
        }
        // Report growth checks require increasing cohorts. Normalize before
        // calibration or any population preparation can consume resources.
        populations.sort_unstable();
        Ok(Self {
            root: root.ok_or("--root <fresh directory on the measured device> is required")?,
            samples,
            source: source.ok_or("--source <source revision> is required")?,
            operations,
            populations,
            setup_credit,
            proofs,
            observe_durability,
        })
    }
}
