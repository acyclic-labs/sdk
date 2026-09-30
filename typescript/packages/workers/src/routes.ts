// Generated from acyclic-workers::HTTP_ROUTES. Do not edit.
export const HTTP_ROUTES = {
  "publishVersion": "v1/workers/versions/publish",
  "selectDeployment": "v1/workers/deployments/select",
  "submitJob": "v1/workers/jobs/submit",
  "inspectJob": "v1/workers/jobs/inspect",
  "cancelJob": "v1/workers/jobs/cancel",
  "invokeVersion": "v1/workers/versions/{sha256hex}/invoke",
  "invokeDeployment": "v1/workers/deployments/{alias}/invoke"
} as const;
