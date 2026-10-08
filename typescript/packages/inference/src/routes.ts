// Generated from acyclic_inference::http_codec::routes. Do not edit.
export const HTTP_ROUTES = {
  "inference.customer.v1.ContextsService.Create": [
    "contexts/create",
    "unary"
  ],
  "inference.customer.v1.ContextsService.Inspect": [
    "contexts/inspect",
    "unary"
  ],
  "inference.customer.v1.ContextsService.Mutate": [
    "contexts/mutate",
    "unary"
  ],
  "inference.customer.v1.EvaluationsService.Create": [
    "evaluations/create",
    "unary"
  ],
  "inference.customer.v1.EvaluationsService.Inspect": [
    "evaluations/inspect",
    "unary"
  ],
  "inference.customer.v1.ModelsService.List": [
    "models/list",
    "unary"
  ],
  "inference.customer.v1.RunsService.Cancel": [
    "runs/cancel",
    "unary"
  ],
  "inference.customer.v1.RunsService.Generate": [
    "runs/generate",
    "unary"
  ],
  "inference.customer.v1.RunsService.Inspect": [
    "runs/inspect",
    "unary"
  ],
  "inference.customer.v1.RunsService.Watch": [
    "runs/watch",
    "server_streaming"
  ],
  "inference.customer.v1.WarmContextsService.Inspect": [
    "warm/inspect",
    "unary"
  ],
  "inference.customer.v1.WarmContextsService.Release": [
    "warm/release",
    "unary"
  ],
  "inference.customer.v1.WarmContextsService.Renew": [
    "warm/renew",
    "unary"
  ],
  "inference.customer.v1.WarmContextsService.Retain": [
    "warm/retain",
    "unary"
  ]
} as const;
