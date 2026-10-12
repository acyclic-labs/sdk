/** Cloud public HTTP contract at 0c900150c3c7cda2c7b02434838f6c410cd74018. */
export type Json = null | boolean | number | string | Json[] | { [key: string]: Json };
export type Harness = "acyclic" | "codex";
export type SwarmStatus = "queued" | "running" | "succeeded" | "failed" | "canceled";
export type AgentStatus = "booting" | "running" | "waiting" | "succeeded" | "failed" | "canceled";
export type AgentKind = "root" | "fork" | "attempt" | "judge" | "delegate";

export interface AgentSpend {
  inference_usd: number;
  sandbox_usd: number;
  model_calls: number;
  prompt_tokens: number;
  cached_tokens: number;
  completion_tokens: number;
}
export interface Agent {
  agent_id: string;
  parent_id: string | null;
  kind: AgentKind;
  status: AgentStatus;
  depth: number;
  box: { provider: string; sandbox_id: string; class: string; fork_mode: string | null } | null;
  started_at: string | null;
  ended_at: string | null;
  summary: string | null;
  error: string | null;
  spend: AgentSpend;
  preset?: string | null;
  model?: string | null;
}
export interface Speculation {
  speculation_id: string;
  parent_agent_id: string;
  attempt_agent_ids: string[];
  status: "running" | "judging" | "decided" | "none_qualified" | "failed";
  verdict: { winner_agent_id: string | null; rationale: string } | null;
}
export interface SwarmLimits {
  width: number;
  depth: number;
  running: number;
  web_calls: number;
  budget_usd: number | null;
}
export interface SwarmResult { summary: string; diff_stat: string | null }
export interface SwarmArtifact {
  artifact_id: string;
  created_at: string;
  size_bytes: number;
  files_changed: number;
  insertions: number;
  deletions: number;
  base_ref: string | null;
  sha256: string;
  truncated: boolean;
  partial?: boolean;
}
export interface ArtifactFile {
  path: string;
  status: "added" | "modified" | "deleted";
  insertions: number;
  deletions: number;
}
export interface SwarmSummary {
  swarm_id: string;
  task: string;
  status: SwarmStatus;
  created_at: string;
  agent_count: number;
  spend_usd: number;
  created_by: string;
  error: string | null;
}
export interface SwarmTimings {
  wall_ms: number;
  boot_p50_ms: number | null;
  boot_p95_ms: number | null;
  fork_p50_ms: number | null;
  ttft_p50_ms: number | null;
  model_ms_total: number;
  tool_ms_total: number;
  idle_ms_total: number;
  critical_path: string[];
}
export interface CheckOutcome { passed: boolean; exit_code?: number | null; output: string }
export interface SwarmDetail extends SwarmSummary {
  repo: string;
  model: string;
  harness: Harness;
  limits: SwarmLimits;
  agents: Agent[];
  speculations: Speculation[];
  spend: { inference_usd: number; sandbox_usd: number; total_usd: number };
  result: SwarmResult | null;
  last_seq: number;
  timings: SwarmTimings;
  artifact?: SwarmArtifact | null;
  pull_request?: SwarmPullRequest | null;
  pull_request_error?: string | null;
  pull_request_pending?: boolean;
  pull_request_requested?: boolean;
  from_swarm?: string | null;
  models?: Record<string, string>;
  verify?: { command: string; round: number; rounds_left: number } | null;
  verification?: CheckOutcome | null;
  next_round?: string | null;
  spend_by_role?: Record<string, { agents: number } & AgentSpend>;
}
export interface SwarmPullRequest {
  url: string;
  number: number;
  repo: string;
  branch: string;
  base: string;
  created_at: string;
  created_by: string;
}
export interface Span {
  span_id: string;
  parent_span_id: string | null;
  name: string;
  agent_id: string | null;
  start: string;
  end: string | null;
  attrs?: Record<string, Json>;
  status?: "ok" | "error";
  error?: string | null;
}
export interface TraceResponse { trace_id: string; spans: Span[] }
export type SwarmEvent = { seq: number } & (
  | { type: "agent_updated"; agent: Agent }
  | { type: "speculation_updated"; speculation: Speculation }
  | { type: "swarm_updated"; swarm: SwarmSummary; result?: SwarmResult | null }
  | { type: "log"; agent_id: string; line: string }
);
export interface PullRequestInput { github_token: string; title?: string | null; body?: string | null; draft?: boolean }
export interface CreateSwarmRequest {
  task: string;
  repo?: string | null;
  upload_id?: string | null;
  limits?: { width?: number | null; depth?: number | null; running?: number | null; web_calls?: number | null; budget_usd?: number | null } | null;
  speculation?: { attempts?: number | null } | null;
  inference?: { base_url: string; token: string; budget_micros?: number | null } | null;
  mcp_servers?: { name: string; url: string; headers?: Record<string, string> }[] | null;
  skills?: { content: string }[] | null;
  model?: string | null;
  harness?: Harness | null;
  from_swarm?: string | null;
  attachments?: { path: string; content: string }[] | null;
  merge_requires?: string | null;
  models?: Record<string, string> | null;
  verify?: { command: string; rounds?: number | null } | null;
  pull_request?: PullRequestInput | false | null;
}
export interface CreateSwarmResponse { swarm_id: string; harness: Harness }
export interface UploadResponse {
  upload_id: string;
  files: number;
  compressed_bytes: number;
  tar_bytes: number;
  sha256: string;
  expires_at: string;
}
export interface CreateTokenRequest { org: string; user: string; ttl_hours: number; scopes: string[] }
export interface CreateTokenResponse { token: string; token_id: string; expires_at: string }
export interface TokenInfo {
  token_id: string;
  org: string;
  user: string;
  scopes: string[];
  created_at: string;
  expires_at: string;
  revoked: boolean;
}
export interface Whoami {
  org: string | null;
  user: string | null;
  scopes: string[];
  expires_at: string | null;
  kind: string;
  key_org?: string | null;
  capabilities?: string[];
}
