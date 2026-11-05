//! Closed semantic action vocabulary shared by runtime admission and code generation.
#![no_std]

pub mod method;

/// An action is not declared by any SDK product.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UnknownAction;

impl core::fmt::Display for UnknownAction {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str("undeclared capability action")
    }
}

impl core::error::Error for UnknownAction {}

macro_rules! catalog {
    ($($product:ident $kind:ident $prefix:literal { $($variant:ident => $name:literal),* $(,)? })*) => {
        $(
            #[doc = concat!("Declared `", $prefix, "` capabilities.")]
            #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
            pub enum $kind {
                $(#[doc = concat!("`", $prefix, "/", $name, "`.")] $variant,)*
            }

            impl $kind {
                /// Every declared action for this product.
                pub const ALL: &'static [Self] = &[$(Self::$variant),*];
                /// Product-local names, without the product prefix.
                pub const NAMES: &'static [&'static str] = &[$($name),*];

                /// Canonical capability spelling. No allocation or lookup.
                #[must_use]
                pub const fn as_str(self) -> &'static str {
                    match self { $(Self::$variant => concat!($prefix, "/", $name)),* }
                }
            }

            impl From<$kind> for Action {
                fn from(value: $kind) -> Self { Self::$product(value) }
            }

            impl core::str::FromStr for $kind {
                type Err = UnknownAction;
                fn from_str(value: &str) -> Result<Self, Self::Err> {
                    match value {
                        $(concat!($prefix, "/", $name) => Ok(Self::$variant),)*
                        _ => Err(UnknownAction),
                    }
                }
            }
        )*

        /// A declared action, retaining its owning product in the type.
        #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub enum Action {
            $(#[doc = concat!("A `", $prefix, "` action.")] $product($kind),)*
        }

        impl Action {
            /// Complete SDK vocabulary, generated from the same declarations.
            pub const ALL: &'static [Self] = &[$($(Self::$product($kind::$variant)),*),*];

            /// Canonical capability spelling. No allocation or lookup.
            #[must_use]
            pub const fn as_str(self) -> &'static str {
                match self { $(Self::$product(value) => value.as_str()),* }
            }

            /// Owning capability product.
            #[must_use]
            pub const fn product(self) -> &'static str {
                match self { $(Self::$product(_) => $prefix),* }
            }
        }

        impl core::str::FromStr for Action {
            type Err = UnknownAction;
            fn from_str(value: &str) -> Result<Self, Self::Err> {
                match value {
                    $($(concat!($prefix, "/", $name) => Ok(Self::$product($kind::$variant)),)*)*
                    _ => Err(UnknownAction),
                }
            }
        }
    };
}

catalog! {
    Actors ActorsAction "actors" {
        Checkpoint => "checkpoint", Connect => "connect", DeployNamespace => "deploy-namespace",
        DeleteAll => "delete-all", Fork => "fork", Invoke => "invoke", Read => "read",
        Resume => "resume", Subscribe => "subscribe", Unsubscribe => "unsubscribe",
        UpdateNamespace => "update-namespace",
    }
    Billing BillingAction "billing" {
        Audit => "audit", Cost => "cost", Inventory => "inventory", Receipt => "receipt", Usage => "usage",
    }
    Execution ExecutionAction "execution" {
        JobExecute => "job-execute", WorkerExecute => "worker-execute",
    }
    Fs FsAction "fs" {
        List => "list", Read => "read", Create => "create", Write => "write", Append => "append",
        Delete => "delete", Rename => "rename", Fork => "fork", MergeInto => "merge-into",
        ReadHistory => "read-history", Lock => "lock", Admin => "admin",
    }
    Infer InferAction "infer" {
        CancelRun => "cancel-run", CreateContext => "create-context", CreateEvaluation => "create-evaluation",
        FollowRun => "follow-run", Generate => "generate", ListModels => "list-models",
        ReadContext => "read-context", ReadEvaluation => "read-evaluation", ReadRetention => "read-retention",
        ReadRun => "read-run", ReleaseRetention => "release-retention", RenewRetention => "renew-retention",
        RetainContext => "retain-context", WriteContext => "write-context",
    }
    Machines MachinesAction "machines" {
        Cancel => "cancel", Checkpoint => "checkpoint", Create => "create", Delete => "delete",
        DeleteCheckpoint => "delete-checkpoint", Exec => "exec", Follow => "follow", List => "list",
        Qualify => "qualify", Read => "read", ReadCheckpoint => "read-checkpoint", ReadFile => "read-file",
        ReadOutput => "read-output", ReadUsage => "read-usage", Recover => "recover", Update => "update",
        WriteFile => "write-file",
    }
    Objects ObjectsAction "objects" {
        Collect => "collect", Delete => "delete", Get => "get", Head => "head", List => "list", Put => "put", Retain => "retain",
    }
    Secrets SecretsAction "secrets" { Delete => "delete", List => "list", Put => "put" }
    Streams StreamsAction "streams" {
        Append => "append", Commit => "commit", Create => "create", Follow => "follow", Fork => "fork", List => "list", Read => "read",
    }
    Workers WorkersAction "workers" { Deploy => "deploy", Delete => "delete", Invoke => "invoke" }
}

impl core::fmt::Display for Action {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str(self.as_str())
    }
}
