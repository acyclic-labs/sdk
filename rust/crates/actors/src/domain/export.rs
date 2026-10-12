use super::*;

macro_rules! typescript_message_roots {
    ($callback:ident) => {
        $callback!(
            ActorObservation,
            CreateActorRequest,
            UpdateActorRequest,
            InspectActorRequest,
            AddSubscriptionRequest,
            RemoveSubscriptionRequest,
            ResumeSubscriptionRequest,
            CheckpointActorRequest,
            InvokeActorRequest,
            DeleteActorRequest,
            CreateActorResponse,
            UpdateActorResponse,
            InspectActorResponse,
            AddSubscriptionResponse,
            RemoveSubscriptionResponse,
            ResumeSubscriptionResponse,
            CheckpointActorResponse,
            InvokeActorResponse,
            DeleteActorResponse,
        )
    };
}
macro_rules! typescript_roots {
    ($callback:ident) => {
        $callback!(
            subscription_start::CurrentHeadMarker,
            PositiveU64,
            ErrorCode,
            ServiceError,
        );
        typescript_message_roots!($callback);
    };
}

struct TypeScriptExportVisitor<'a> {
    config: &'a Config,
    names: Vec<String>,
    seen: std::collections::HashSet<std::any::TypeId>,
}

impl TypeVisitor for TypeScriptExportVisitor<'_> {
    fn visit<T: TS + 'static + ?Sized>(&mut self) {
        if !self.seen.insert(std::any::TypeId::of::<T>()) {
            return;
        }
        if T::output_path().is_some() {
            self.names.push(T::ident(self.config));
        }
        T::visit_dependencies(self);
    }
}

/// Names emitted by the executable Rust semantic export registry, including
pub fn typescript_export_names(config: &Config) -> Vec<String> {
    let mut visitor = TypeScriptExportVisitor {
        config,
        names: Vec::new(),
        seen: std::collections::HashSet::new(),
    };
    macro_rules! visit_roots {
        ($($root:ty),+ $(,)?) => {
            $(visitor.visit::<$root>();)+
        };
    }
    typescript_roots!(visit_roots);
    visitor.names.sort();
    visitor.names
}

/// Actual exported names and compiler-linked wire/semantic identities.
pub fn typescript_export_metadata(config: &Config) -> (Vec<String>, Vec<(String, String)>) {
    fn paired<D, W>(config: &Config) -> (String, String)
    where
        D: TS + TryFrom<W>,
        W: prost::Name + prost::Message + From<D>,
    {
        (W::full_name(), D::ident(config))
    }
    let mut messages = Vec::new();
    macro_rules! roots {
        ($($root:ident),+ $(,)?) => {
            $(messages.push(paired::<$root, crate::wire::$root>(config));)+
        };
    }
    typescript_message_roots!(roots);
    (typescript_export_names(config), messages)
}

fn export_numeric_union<T: TS + 'static>(path: &Path, config: &Config) -> Result<(), ExportError> {
    let generated = T::export_to_string(config)?;
    let declaration = format!("{}\n", T::decl(config));
    let prefix = generated.strip_suffix(&declaration).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "ts-rs enum declaration suffix is absent",
        )
    })?;
    let output =
        T::output_path().ok_or(ExportError::CannotBeExported(std::any::type_name::<T>()))?;
    std::fs::write(
        path.join(output),
        format!(
            "{prefix}type {} = {};\n",
            T::ident(config),
            T::inline(config)
        ),
    )?;
    Ok(())
}

/// Export all public Actors request and response declarations and their
pub fn export_typescript(path: impl AsRef<Path>) -> Result<(), ExportError> {
    let config = Config::default()
        .with_out_dir(path.as_ref())
        .with_import_extension(Some("js"));

    macro_rules! export_roots {
        ($($root:ty),+ $(,)?) => {
            $(<$root as TS>::export_all(&config)?;)+
        };
    }

    typescript_roots!(export_roots);
    export_numeric_union::<SubscriptionState>(path.as_ref(), &config)?;
    export_numeric_union::<ActorState>(path.as_ref(), &config)?;
    export_numeric_union::<ErrorCode>(path.as_ref(), &config)?;
    Ok(())
}
