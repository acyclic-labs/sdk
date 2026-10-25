use super::*;

/// Captured once by the original root SDK bootstrap and kept only in the protected Pi owner file.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize, acyclic_harness::HeapLayout)]
#[heap_layout(crate = acyclic_harness)]
#[serde(deny_unknown_fields)]
pub(crate) struct PiNativeHostRuntime {
    pub(crate) version: String,
    pub(crate) executable: PathBuf,
    pub(crate) executable_prefix: Vec<String>,
    pub(crate) extension: PathBuf,
    pub(crate) provider_extension: Option<PathBuf>,
    pub(crate) environment: BTreeMap<String, String>,
}

#[derive(Clone, Copy, Eq, PartialEq, Serialize, Deserialize, acyclic_harness::HeapLayout)]
#[heap_layout(crate = acyclic_harness)]
#[serde(deny_unknown_fields)]
pub(crate) struct PiNativeCaptureLimits {
    pub(crate) timeout_ms: u32,
    pub(crate) control_timeout_ms: u32,
    pub(crate) maximum_output_bytes: u32,
    pub(crate) maximum_result_bytes: u32,
}

/// Fixed native policy captured in the immutable seed, never a spawn override.
pub(crate) fn capture_limits(
    actual_root_limits: acyclic_harness::conversation::Limits,
    maximum_payload: u64,
) -> Result<PiNativeCaptureLimits> {
    actual_root_limits.validate(&mut None)?;
    let result = actual_root_limits
        .file_bytes
        .min(maximum_payload)
        .min(crate::MAXIMUM_ADAPTER_STATE_BYTES)
        .min(1_048_576);
    let output = result
        .checked_sub(2048)
        .map(|remaining| (remaining / 4).min(262_144))
        .filter(|bytes| *bytes > 0)
        .ok_or_else(|| invalid("original Pi root bounds cannot admit a native process result"))?;
    Ok(PiNativeCaptureLimits {
        timeout_ms: 180_000,
        control_timeout_ms: 5_000,
        maximum_output_bytes: u32::try_from(output).map_err(|error| invalid(&error.to_string()))?,
        maximum_result_bytes: u32::try_from(result).map_err(|error| invalid(&error.to_string()))?,
    })
}

impl PiNativeHostRuntime {
    pub(crate) fn sanitize(mut self, work: &NativeMaterializationWork) -> Result<Self> {
        self.environment
            .retain(|key, _| !pi_caller::is_environment_key(key));
        self.validate(work)?;
        Ok(self)
    }

    /// Whether two keys name one Windows variable: they differ only in ASCII case.
    ///
    /// Each key's ASCII-folded bytes are hashed once to a 32-byte BLAKE3 digest,
    /// the digests are merge sorted, and equal neighbours are duplicates. The
    /// work and memory are exact functions of the key count and key bytes:
    /// hashing reads each key once, every merge pass compares at most one pair
    /// per key, the final scan compares one pair per key, and the digests and
    /// the merge buffer are two vectors of exactly one digest per key. Both are
    /// admitted before any key is folded, so a budget that cannot pay refuses
    /// the check without doing it.
    pub(super) fn case_folded_duplicate(&self, work: &NativeMaterializationWork) -> Result<bool> {
        let (examined, table) = case_folded_check_cost(self.environment.keys())?;
        work.charge(WorkCounters {
            items_examined: examined,
            ..WorkCounters::default()
        })?;
        let _table_custody = work.claim(table, 1, WorkCounters::UNCHARGED)?;
        let keys = self.environment.len();
        let mut digests = digest_buffer(keys)?;
        let mut merged = digest_buffer(keys)?;
        for key in self.environment.keys() {
            let mut hasher = blake3::Hasher::new();
            let mut folded = [0_u8; 64];
            for part in key.as_bytes().chunks(folded.len()) {
                if let Some(folded) = folded.get_mut(..part.len()) {
                    folded.copy_from_slice(part);
                    folded.make_ascii_lowercase();
                    hasher.update(folded);
                }
            }
            digests.push(*hasher.finalize().as_bytes());
        }
        merged.resize(keys, [0; 32]);
        let mut width = 1;
        while width < keys {
            let runs = digests.chunks(2 * width).zip(merged.chunks_mut(2 * width));
            for (run, out) in runs {
                let (left, right) = run.split_at(width.min(run.len()));
                merge_digests(left, right, out);
            }
            std::mem::swap(&mut digests, &mut merged);
            width *= 2;
        }
        Ok(digests
            .windows(2)
            .any(|pair| matches!(pair, [left, right] if left == right)))
    }

    pub(crate) fn validate(&self, work: &NativeMaterializationWork) -> Result<()> {
        work.remaining()?;
        let reject = |reason| Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Invalid,
            reason,
        };
        let inspect = |items: usize| {
            work.charge(WorkCounters {
                items_examined: items as u64,
                ..WorkCounters::default()
            })
            .map_err(acyclic_harness::Error::from)
        };
        let mut environment_bytes = 0_u64;
        if cfg!(windows) && self.case_folded_duplicate(work)? {
            return Err(reject(
                "Pi original host environment is invalid or contains a recursive credential",
            ));
        }
        for (key, value) in &self.environment {
            inspect(1)?;
            inspect(key.len())?;
            inspect(value.len())?;
            environment_bytes = environment_bytes
                .checked_add(key.len() as u64)
                .and_then(|bytes| bytes.checked_add(value.len() as u64))
                .ok_or_else(|| reject("Pi host environment size overflow"))?;
            if key.is_empty()
                || key.contains(['=', '\0'])
                || value.contains('\0')
                || pi_caller::is_environment_key(key)
                || environment_bytes > crate::MAXIMUM_ADAPTER_STATE_BYTES
            {
                return Err(reject(
                    "Pi original host environment is invalid or contains a recursive credential",
                ));
            }
        }
        inspect(self.version.len())?;
        for path in [&self.executable, &self.extension] {
            inspect(path.as_os_str().len())?;
        }
        for path in &self.executable_prefix {
            inspect(path.len())?;
        }
        if let Some(path) = &self.provider_extension {
            inspect(path.as_os_str().len())?;
        }
        if self.version != PI_NATIVE_VERSION
            || !self.executable.is_absolute()
            || !self.extension.is_absolute()
            || has_parent(&self.executable)
            || has_parent(&self.extension)
            || self.environment.is_empty()
            || self.executable_prefix.len() > 1
            || self.executable_prefix.first().is_some_and(|path| {
                !Path::new(path).is_absolute() || has_parent(Path::new(path)) || path.contains('\0')
            })
            || self
                .provider_extension
                .as_ref()
                .is_some_and(|path| !path.is_absolute() || has_parent(path))
        {
            return Err(reject(
                "Pi original runtime must select the pinned absolute executable and extension",
            ));
        }
        Ok(())
    }

    pub(crate) fn clone_with_native_work(
        &self,
        work: &NativeMaterializationWork,
    ) -> Result<(Self, acyclic_fs::NativeAllocation)> {
        self.validate(work)?;
        let bytes = crate::pi_root_provider::guarded_pi_bytes(
            self,
            crate::MAXIMUM_ADAPTER_STATE_BYTES,
            work,
        )?;
        crate::pi_root_provider::guarded_pi_decode(
            &bytes.0,
            crate::MAXIMUM_ADAPTER_STATE_BYTES,
            acyclic_harness::wire::ErrorCode::Invalid,
            JsonValueSchema::None,
            PI_NATIVE_LAUNCH_DIAGNOSTIC_LABEL_BYTES,
            work,
        )
    }

    #[allow(clippy::too_many_arguments, reason = "borrowed native child inputs")]
    pub(crate) fn intent_invocation(
        &self,
        session_file: &Path,
        cwd: &Path,
        provider: &str,
        model: &str,
        prompt: &str,
        capture: PiNativeCaptureLimits,
        work: &NativeMaterializationWork,
    ) -> Result<(PiNativeInvocation, acyclic_fs::NativeAllocation)> {
        self.validate(work)?;
        let projection = PiBoundInvocation {
            runtime_digest: *digest_pi_value(self, crate::MAXIMUM_ADAPTER_STATE_BYTES, work)?
                .as_bytes(),
            session_file,
            cwd,
            provider,
            model,
            prompt,
            builtin_tools: &["read", "write", "edit"],
            environment: &BTreeMap::new(),
            timeout_ms: capture.timeout_ms,
            control_timeout_ms: capture.control_timeout_ms,
            maximum_output_bytes: capture.maximum_output_bytes,
            maximum_result_bytes: capture.maximum_result_bytes,
        };
        projection.decode(work)
    }

    pub(crate) fn bind_invocation(
        &self,
        intent: &PiNativeLaunchIntent,
        caller_environment: &BTreeMap<String, String>,
        work: &NativeMaterializationWork,
    ) -> Result<(PiNativeInvocation, acyclic_fs::NativeAllocation)> {
        require_caller_environment(caller_environment)?;
        let input = &intent.invocation;
        if !input.environment.is_empty()
            || !input
                .builtin_tools
                .iter()
                .map(String::as_str)
                .eq(["read", "write", "edit"])
        {
            return Err(invalid(
                "Pi immutable child seed changed its original protected native host settings",
            ));
        }
        self.validate(work)?;
        if input.runtime_digest
            != *digest_pi_value(self, crate::MAXIMUM_ADAPTER_STATE_BYTES, work)?.as_bytes()
        {
            return Err(invalid(
                "Pi immutable child seed changed its original protected native host settings",
            ));
        }
        intent.validate(work)?;
        PiBoundInvocation {
            runtime_digest: input.runtime_digest,
            session_file: &input.session_file,
            cwd: &input.cwd,
            provider: &input.provider,
            model: &input.model,
            prompt: &input.prompt,
            builtin_tools: &["read", "write", "edit"],
            environment: caller_environment,
            timeout_ms: input.timeout_ms,
            control_timeout_ms: input.control_timeout_ms,
            maximum_output_bytes: input.maximum_output_bytes,
            maximum_result_bytes: input.maximum_result_bytes,
        }
        .decode(work)
    }
}

#[derive(Serialize)]
struct PiBoundInvocation<'a> {
    runtime_digest: [u8; 32],
    session_file: &'a Path,
    cwd: &'a Path,
    provider: &'a str,
    model: &'a str,
    prompt: &'a str,
    builtin_tools: &'a [&'a str],
    environment: &'a BTreeMap<String, String>,
    timeout_ms: u32,
    control_timeout_ms: u32,
    maximum_output_bytes: u32,
    maximum_result_bytes: u32,
}

impl PiBoundInvocation<'_> {
    fn decode(
        &self,
        work: &NativeMaterializationWork,
    ) -> Result<(PiNativeInvocation, acyclic_fs::NativeAllocation)> {
        let bytes = crate::pi_root_provider::guarded_pi_bytes(
            self,
            crate::MAXIMUM_ADAPTER_STATE_BYTES,
            work,
        )?;
        crate::pi_root_provider::guarded_pi_decode(
            &bytes.0,
            crate::MAXIMUM_ADAPTER_STATE_BYTES,
            acyclic_harness::wire::ErrorCode::Invalid,
            JsonValueSchema::None,
            PI_NATIVE_LAUNCH_DIAGNOSTIC_LABEL_BYTES,
            work,
        )
    }
}

#[derive(Serialize)]
struct PiCallerEnvironment<'a> {
    #[serde(flatten)]
    host: &'a BTreeMap<String, String>,
    #[serde(flatten)]
    caller: &'a BTreeMap<String, String>,
}

pub(super) fn require_caller_environment(environment: &BTreeMap<String, String>) -> Result<()> {
    if environment
        .get("ACYCLIC_PI_CALLER")
        .is_none_or(String::is_empty)
        || environment.iter().any(|(key, value)| {
            !pi_caller::is_environment_key(key) || value.is_empty() || value.contains('\0')
        })
    {
        return Err(invalid(
            "Pi native child needs its separately authenticated original caller credential",
        ));
    }
    Ok(())
}

/// Child inputs bound to the protected host configuration.
#[derive(Clone, PartialEq, Serialize, Deserialize, acyclic_harness::HeapLayout)]
#[heap_layout(crate = acyclic_harness)]
#[serde(deny_unknown_fields)]
pub(crate) struct PiNativeInvocation {
    pub(crate) runtime_digest: [u8; 32],
    pub(crate) session_file: PathBuf,
    pub(crate) cwd: PathBuf,
    pub(crate) provider: String,
    pub(crate) model: String,
    pub(crate) prompt: String,
    pub(crate) builtin_tools: Vec<String>,
    pub(crate) environment: BTreeMap<String, String>,
    pub(crate) timeout_ms: u32,
    pub(crate) control_timeout_ms: u32,
    pub(crate) maximum_output_bytes: u32,
    pub(crate) maximum_result_bytes: u32,
}

impl PiNativeInvocation {
    pub(crate) fn request_with_work(
        &self,
        runtime: &PiNativeHostRuntime,
        binding: &PiTaskBinding,
        manifest: &NativeViewManifest,
        work: &NativeMaterializationWork,
    ) -> Result<(NativeProcessRequest, acyclic_fs::NativeAllocation)> {
        runtime.validate(work)?;
        if self.runtime_digest
            != *digest_pi_value(runtime, crate::MAXIMUM_ADAPTER_STATE_BYTES, work)?.as_bytes()
        {
            return Err(invalid(
                "Pi invocation changed its protected host configuration",
            ));
        }
        binding.validate(
            Some(&binding.native_session_id),
            binding.project_volumes.keys().map(String::as_str),
            false,
        )?;
        manifest.validate(Some(
            &acyclic_harness::filesystem::PreparationIo::from_native_work(work)?,
        ))?;
        let scratch = manifest
            .volumes
            .iter()
            .find(|mapping| mapping.volume == binding.scratch.volume)
            .ok_or_else(|| invalid("Pi native namespace omits original scratch"))?;
        if !matches!(binding.scratch.allocation, PiScratchAllocation::Created(_))
            || !scratch.writable
            || !self.session_file.starts_with(&scratch.path)
            || self.session_file == scratch.path
            || !self.cwd.starts_with(&manifest.options.root)
            || !manifest
                .volumes
                .iter()
                .any(|mapping| self.cwd.starts_with(&mapping.path))
            || !self.session_file.is_absolute()
            || !self.cwd.is_absolute()
            || [&self.session_file, &self.cwd]
                .iter()
                .any(|path| has_parent(path))
            || self.provider.is_empty()
            || self.model.is_empty()
            || self.prompt.is_empty()
            || self.environment.iter().any(|(key, value)| {
                !pi_caller::is_environment_key(key) || value.is_empty() || value.contains('\0')
            })
            || self.builtin_tools.iter().enumerate().any(|(index, name)| {
                !matches!(name.as_str(), "read" | "write" | "edit")
                    || self
                        .builtin_tools
                        .iter()
                        .take(index)
                        .any(|previous| previous == name)
            })
        {
            return Err(invalid(
                "Pi invocation differs from the pinned executable, session or namespace",
            ));
        }
        if manifest.volumes.len() != binding.project_volumes.len() + 1
            || binding.project_volumes.values().any(|volume| {
                !manifest.volumes.iter().any(|mapping| {
                    &mapping.volume == volume
                        && matches!(&mapping.namespace, NativeVolumeNamespace::LazyMount { .. })
                })
            })
        {
            return Err(invalid(
                "Pi native namespace differs from its admitted volume selection",
            ));
        }
        let mut argv = [""; 18];
        let prefix = runtime.executable_prefix.len();
        for (target, value) in argv.iter_mut().zip(&runtime.executable_prefix) {
            *target = value;
        }
        let arguments = [
            "--print",
            "--mode",
            "json",
            "--session",
            native_argv_path(&self.session_file)?,
            "--provider",
            &self.provider,
            "--model",
            &self.model,
            "--no-extensions",
            "--no-mcp",
            "-e",
            native_argv_path(&runtime.extension)?,
        ];
        argv[prefix..prefix + arguments.len()].copy_from_slice(&arguments);
        let mut count = prefix + arguments.len();
        let mut tools = std::io::Cursor::new([0; 15]);
        if self.builtin_tools.is_empty() {
            argv[count] = "--no-builtin-tools";
            count += 1;
        } else {
            use std::io::Write;
            for (index, tool) in self.builtin_tools.iter().enumerate() {
                write!(tools, "{}{tool}", if index == 0 { "" } else { "," })
                    .expect("validated builtin names fit their buffer");
            }
            argv[count] = "--tools";
            argv[count + 1] = std::str::from_utf8(&tools.get_ref()[..tools.position() as usize])
                .expect("validated builtin names are ASCII");
            count += 2;
        }
        if let Some(extension) = &runtime.provider_extension {
            argv[count] = "-e";
            argv[count + 1] = native_argv_path(extension)?;
            count += 2;
        }
        let projection = PiNativeRequestProjection {
            executable: &runtime.executable,
            argv: &argv[..count],
            cwd: &self.cwd,
            environment: PiCallerEnvironment {
                host: &runtime.environment,
                caller: &self.environment,
            },
            timeout_ms: self.timeout_ms,
            control_timeout_ms: self.control_timeout_ms,
            maximum_output_bytes: self.maximum_output_bytes,
            maximum_result_bytes: self.maximum_result_bytes,
            standard_input: Some(self.prompt.as_bytes()),
            view: manifest,
        };
        let encoded = crate::pi_root_provider::guarded_pi_bytes(
            &projection,
            crate::MAXIMUM_ADAPTER_STATE_BYTES,
            work,
        )?;
        let request = NativeProcessRequest::decode_with_native_work(&encoded.0, work)?;
        if !request.0.has_valid_capture_allowances()
            || request.0.argv.iter().any(|value| value.contains('\0'))
        {
            return Err(invalid("Pi native capture allowances or argv are invalid"));
        }
        Ok(request)
    }
}

#[derive(Serialize)]
struct PiNativeRequestProjection<'a> {
    executable: &'a Path,
    argv: &'a [&'a str],
    cwd: &'a Path,
    environment: PiCallerEnvironment<'a>,
    timeout_ms: u32,
    control_timeout_ms: u32,
    maximum_output_bytes: u32,
    maximum_result_bytes: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    standard_input: Option<&'a [u8]>,
    view: &'a NativeViewManifest,
}

fn native_argv_path(path: &Path) -> Result<&str> {
    path.to_str()
        .ok_or_else(|| invalid("Pi invocation path is not UTF-8"))
}

/// Items examined and bytes held by the case-folded duplicate check of `keys`.
pub(super) fn case_folded_check_cost<'a>(
    keys: impl ExactSizeIterator<Item = &'a String>,
) -> Result<(u64, u64)> {
    let overflow = || Error::NativeBoundary {
        code: acyclic_harness::wire::ErrorCode::Invalid,
        reason: "Pi host environment size overflow",
    };
    let count = keys.len() as u64;
    let passes = u64::from(count.max(1).next_power_of_two().trailing_zeros());
    let hashed = keys
        .into_iter()
        .try_fold(0_u64, |sum, key| sum.checked_add(key.len() as u64 + 1))
        .ok_or_else(overflow)?;
    let examined = count
        .checked_mul(passes + 1)
        .and_then(|compared| compared.checked_add(hashed))
        .ok_or_else(overflow)?;
    let table = count
        .checked_mul(2 * std::mem::size_of::<[u8; 32]>() as u64)
        .ok_or_else(overflow)?;
    Ok((examined, table))
}

/// A vector of exactly `keys` digest slots.
fn digest_buffer(keys: usize) -> Result<Vec<[u8; 32]>> {
    let mut buffer = Vec::new();
    buffer
        .try_reserve_exact(keys)
        .map_err(|_| Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Storage,
            reason: "Pi host environment check cannot allocate its digests",
        })?;
    Ok(buffer)
}

/// Merges two sorted runs into `merged`, one comparison per element at most.
fn merge_digests(left: &[[u8; 32]], right: &[[u8; 32]], merged: &mut [[u8; 32]]) {
    let (mut left, mut right) = (left.iter().peekable(), right.iter().peekable());
    for slot in merged {
        let next = match (left.peek(), right.peek()) {
            (Some(l), Some(r)) if r < l => right.next(),
            (Some(_), _) => left.next(),
            (None, _) => right.next(),
        };
        if let Some(digest) = next {
            *slot = *digest;
        }
    }
}
