//! Projection of Modal's published api.proto and task_command_router.proto.
//! Field numbers follow modal npm 0.11.1. Unknown protobuf fields are ignored;
//! no server responses or response fixtures are synthesized here.
#[derive(Clone, PartialEq, prost::Message)]
pub struct TaskSnapshotFilesystemRequest {
    #[prost(string, tag = "1")]
    pub task_id: String,
    #[prost(string, tag = "2")]
    pub snapshot_id: String,
    #[prost(int64, optional, tag = "3")]
    pub ttl_seconds: Option<i64>,
}
#[derive(Clone, PartialEq, prost::Message)]
pub struct ImageId {
    #[prost(string, tag = "1")]
    pub image_id: String,
}
#[derive(Clone, PartialEq, prost::Message)]
pub struct Sandbox {
    #[prost(string, repeated, tag = "1")]
    pub entrypoint_args: Vec<String>,
    #[prost(string, tag = "3")]
    pub image_id: String,
    #[prost(message, optional, tag = "5")]
    pub resources: Option<Resources>,
    #[prost(uint32, tag = "7")]
    pub timeout_secs: u32,
    #[prost(message, optional, tag = "22")]
    pub network_access: Option<NetworkAccess>,
    #[prost(string, optional, tag = "30")]
    pub name: Option<String>,
}
#[derive(Clone, PartialEq, prost::Message)]
pub struct Resources {
    #[prost(uint32, tag = "2")]
    pub memory_mb: u32,
    #[prost(uint32, tag = "3")]
    pub milli_cpu: u32,
}
#[derive(Clone, PartialEq, prost::Message)]
pub struct NetworkAccess {
    #[prost(int32, tag = "1")]
    pub network_access_type: i32,
    #[prost(string, repeated, tag = "3")]
    pub allowed_domains: Vec<String>,
}
#[derive(Clone, PartialEq, prost::Message)]
pub struct SandboxCreateV2Request {
    #[prost(string, tag = "1")]
    pub app_id: String,
    #[prost(message, optional, tag = "2")]
    pub definition: Option<Sandbox>,
}
#[derive(Clone, PartialEq, prost::Message)]
pub struct CommandRouterAccess {
    #[prost(string, tag = "1")]
    pub jwt: String,
    #[prost(string, tag = "2")]
    pub url: String,
}
#[derive(Clone, PartialEq, prost::Message)]
pub struct SandboxCreateV2Response {
    #[prost(string, tag = "1")]
    pub sandbox_id: String,
    #[prost(string, tag = "3")]
    pub task_id: String,
    #[prost(message, optional, tag = "5")]
    pub command_router_access: Option<CommandRouterAccess>,
    #[prost(string, tag = "6")]
    pub sandbox_token: String,
}
#[derive(Clone, PartialEq, prost::Message)]
pub struct SandboxRequest {
    #[prost(string, tag = "1")]
    pub sandbox_id: String,
}
#[derive(Clone, PartialEq, prost::Message)]
pub struct SandboxWaitRequest {
    #[prost(string, tag = "1")]
    pub sandbox_id: String,
    #[prost(float, tag = "2")]
    pub timeout: f32,
}
#[derive(Clone, PartialEq, prost::Message)]
pub struct GenericResult {
    #[prost(int32, tag = "1")]
    pub status: i32,
}
#[derive(Clone, PartialEq, prost::Message)]
pub struct SandboxWaitResponse {
    #[prost(message, optional, tag = "1")]
    pub result: Option<GenericResult>,
}
#[derive(Clone, PartialEq, prost::Message)]
pub struct TaskExecStartRequest {
    #[prost(string, tag = "1")]
    pub task_id: String,
    #[prost(string, tag = "2")]
    pub exec_id: String,
    #[prost(string, repeated, tag = "3")]
    pub command_args: Vec<String>,
    #[prost(int32, tag = "4")]
    pub stdout_config: i32,
    #[prost(int32, tag = "5")]
    pub stderr_config: i32,
    #[prost(uint32, optional, tag = "6")]
    pub timeout_secs: Option<u32>,
}
#[derive(Clone, PartialEq, prost::Message)]
pub struct TaskExecStdioReadRequest {
    #[prost(string, tag = "1")]
    pub task_id: String,
    #[prost(string, tag = "2")]
    pub exec_id: String,
    #[prost(uint64, tag = "3")]
    pub offset: u64,
    #[prost(int32, tag = "4")]
    pub file_descriptor: i32,
}
#[derive(Clone, PartialEq, prost::Message)]
pub struct TaskExecStdioReadResponse {
    #[prost(bytes = "vec", tag = "1")]
    pub data: Vec<u8>,
}
#[derive(Clone, PartialEq, prost::Message)]
pub struct TaskExecWaitRequest {
    #[prost(string, tag = "1")]
    pub task_id: String,
    #[prost(string, tag = "2")]
    pub exec_id: String,
}
#[derive(Clone, PartialEq, prost::Message)]
pub struct TaskExecWaitResponse {
    #[prost(oneof = "ExitStatus", tags = "1,2")]
    pub exit_status: Option<ExitStatus>,
}
#[derive(Clone, PartialEq, prost::Oneof)]
pub enum ExitStatus {
    #[prost(int32, tag = "1")]
    Code(i32),
    #[prost(int32, tag = "2")]
    Signal(i32),
}
#[derive(Clone, PartialEq, prost::Message)]
pub struct TaskExecStdinWriteRequest {
    #[prost(string, tag = "1")]
    pub task_id: String,
    #[prost(string, tag = "2")]
    pub exec_id: String,
    #[prost(uint64, tag = "3")]
    pub offset: u64,
    #[prost(bytes = "vec", tag = "4")]
    pub data: Vec<u8>,
    #[prost(bool, tag = "5")]
    pub eof: bool,
}
