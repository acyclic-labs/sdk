# acyclic-jev

Agent swarms over System One typed decisions, with an append-only decision log.

System One ([Space](https://huggingface.co/spaces/pngwn/open-jev),
[model](https://huggingface.co/pngwn/system-one-qwen3.5-4b-scorer)) is a Qwen3.5-4B scorer. It takes
an unstructured *state* plus typed questions with fixed options and returns a calibrated probability
distribution over exactly those options in one forward pass. Nothing is generated or parsed, so
nothing can drift outside the schema. The state is prefilled once and every (question, option)
branch is scored from that cache in one batched forward.

`jev` builds on that shape:

- `Question` / `Decision`: yes/no, choice, and ordered score questions. Answers are distributions,
  not strings.
- `Swarm`: many agents, one state, one encode. Every agent's questions for a step go to the backend
  in a single batch, come back to the agent that asked, and every agent then acts.
- `DecisionLog`: append-only, hash-chained JSONL. `verify()` proves the file was not edited, and
  `ReplayBackend` replays a run without the model.
- `ForkArena`: judge N acyclic forks in one call and promote the winner.
- `jev gate`: a Claude Code `PreToolUse` hook that answers allow, ask or deny.

## Install and pick a backend

```
pip install "acyclic-jev[space]"     # hosted Space, no key needed
pip install "acyclic-jev[local]"     # the scorer in-process, needs a GPU
pip install acyclic-jev              # mock, jev, openrouter, http and replay are stdlib only
```

| `--backend` | needs | what it does |
|---|---|---|
| `space` | `space` extra | Calls the public Space over its Gradio API. About 4 s per batch on a shared ZeroGPU, up to 24 questions per call (larger batches are chunked). Anonymous callers get a small daily quota; set `HF_TOKEN` to a free Hugging Face token to raise it. |
| `local` | `local` extra, a GPU with about 10 GB | Downloads the public base model and adapter from Hugging Face on first use, no login. Same code path as the Space's cached lane. |
| `jev[:<model>]` | `OPENROUTER_API_KEY` | TypeSafe's Jev, the original System One model, through OpenRouter's Decisions endpoint. Calibrated as served, metered, 32k-token context. |
| `openrouter[:<model>]` | `OPENROUTER_API_KEY` | Letter-logprob decisions from any logprobs-capable chat model. Metered, no quota wall. |
| `http://host:port` | a `jev serve` instance | Talks to a self-hosted scorer. |
| `replay:<log>` | a decision log | Answers from the log instead of a model. |
| `mock` | nothing | Hash-based fake distributions, stable across runs. The default. |

The published adapter's training data is CC-BY-NC-4.0, so `space` and `local` are non-commercial.
See [Commercial use](#commercial-use) for the way around that.

## Quick start

```python
from jev import Question, Swarm, FunctionAgent, DecisionLog
from jev.backends.space import SpaceBackend

reviewer = FunctionAgent("reviewer", [
    Question("How risky is this change?", ("low", "medium", "high")),
    Question.yes_no("Does this change need new tests?", id="tests"),
    Question.score("Review effort, 1 (trivial) to 5 (major)?", 1, 5),
])
router = FunctionAgent("router", lambda state, mem: [
    Question("Which area does this touch?", ("backend", "frontend", "docs", "ci")),
])

swarm = Swarm([reviewer, router], SpaceBackend(), DecisionLog("decisions.jsonl"))
step = swarm.step(open("pr.txt").read())

print(step["reviewer"]["tests"].yes, step["router"][0].chosen)
print(step.timing.forwards, "forwards for", step.n_questions, "questions")
```

Every decision from that step is now in `decisions.jsonl`, tagged with the agent, the step, the
state's sha256, and a hash chained to the previous record.

An agent is anything with a `name`, `ask(state, memory) -> [Question]`, and
`act(decisions, memory)`. `memory` is a per-agent dict the swarm keeps between steps. Return `[]`
from `ask` to sit a step out. `FunctionAgent` wraps a fixed list or a function.

### Replay

```python
from jev import ReplayBackend, DecisionLog
swarm = Swarm([reviewer, router], ReplayBackend(DecisionLog("decisions.jsonl")))
swarm.step(open("pr.txt").read())   # identical decisions, no model
```

Give `ReplayBackend` a `fallback=` backend and decisions missing from the log are made there and
appended, so a replay can extend a run deterministically.

### CLI

```
jev decide --backend space --state-file pr.txt \
    -q "How risky is this?" -o low,medium,high --yes-no "Needs tests?" --log run.jsonl
jev log show run.jsonl
jev log verify run.jsonl
```

## Judging acyclic forks

`jev.acyclic.ForkArena` forks the working tree N ways with `acyclic fork`, lets anything do work in
the fork mounts, renders every fork's diff (plus an optional probe such as test output) into one
shared state, and asks System One in a single batch: which fork should be promoted, is each fork
safe to land, how complete is each fork. The winner is promoted with `acyclic promote`, the losers
are dropped, and every fork id, base generation, decision, cost, promotion and drop lands in the log.

```python
from jev import DecisionLog
from jev.acyclic import AcyclicCLI, ForkArena
from jev.backends.space import SpaceBackend

arena = ForkArena(AcyclicCLI("."), SpaceBackend(), DecisionLog("run.jsonl"),
                  task="Add slugify(title) to src/util.py")
forks = arena.open(3)
# ... run one coding agent per fork.path ...
verdict = arena.judge(probe=lambda f: run_tests(f.path))
arena.resolve(verdict, min_conf=0.6, require_safe=True)   # promote or skip, drop the rest
```

`AcyclicCLI` shells out to the `acyclic` binary (`ACYCLIC_BIN` or PATH), as the pydantic-ai
adapter does. Pass `runner=` to script it in tests.

## Gating tool calls

`jev gate` is a Claude Code `PreToolUse` hook. It reads the tool call on stdin, asks System One how
risky the call is, whether the tree should be checkpointed first, and whether the call is in scope
for the task, logs the three decisions, and answers `allow`, `ask` or `deny`. It follows acyclic's
hook contract: any error or timeout answers `allow` and exits 0. Denying is opt-in via `--deny-at`.

```
jev gate --print-hook-snippet          # the settings.json entry to add
echo '{"tool_name":"Bash","tool_input":{"command":"rm -rf build"}}' | jev gate --backend http://gpu:8788
```

With `--checkpoint` and an acyclic daemon, a "checkpoint first: yes" runs `acyclic checkpoint --no-wait`.
The shared Space is too slow and too quota-bound for a hook on every tool call. Point the gate at a
`jev serve` instance instead.

## Self-hosting the scorer

```
jev serve --backend local --host 0.0.0.0 --port 8788      # on a GPU box
jev decide --backend http://gpu:8788 ...                   # anywhere else
```

`HttpBackend` and the server are stdlib only. The server takes one batch at a time per GPU.
`SYSTEM_ONE_ADAPTER` and `SYSTEM_ONE_TEMPERATURE` override the adapter and its temperature, as in
the Space.

## Hosted backends

`JevBackend` calls TypeSafe's Jev through OpenRouter's Decisions endpoint (`typesafe/jev-1.13`,
$0.042 per million input tokens, output free) or TypeSafe's own API. All questions ride in one
request, so the state is encoded once, and the probabilities come back as served: this backend
never rescales them.

```
jev decide --backend jev -q "Risk?" -o low,medium,high --state "..."                # OPENROUTER_API_KEY
JevBackend(base_url="https://api.typesafe.ai/v1/systemone", model="jev-latest")   # TYPESAFE_API_KEY
```

OpenRouter does not host the System One scorer, so `OpenRouterBackend` reads typed decisions out of
an ordinary chat model through its next-token logprobs: options are listed as letters, one token is
requested, and the letter logprobs become the distribution. This is the letter-logit baseline from
the System One report. It needs a model with `logprobs` and `top_logprobs` (150+ on OpenRouter do),
costs one request per question, and its calibration is the model's own. Any OpenAI-compatible
endpoint works via `OpenRouterBackend(base_url=...)`.

```
export OPENROUTER_API_KEY=sk-or-...
jev decide --backend openrouter -q "Risk?" -o low,medium,high --state "..."    # meta-llama/llama-3.1-8b-instruct
jev decide --backend openrouter:meta-llama/llama-3.1-8b-instruct ...
jev gate --backend openrouter --log .jev/gate.jsonl
```

Dollars from OpenRouter's usage accounting land in every `step` record and in `jev log costs`.

## Costs and evals

Every swarm step writes a `step` record: questions, branches, tokens the cached path processed
against what the naive path would have, forward passes, model and wall milliseconds. Every
`run_eval` writes an `eval` record with accuracy, expected calibration error (15 bins), Brier and
negative log-likelihood, plus the summed cost of the run.

```
jev eval cases.jsonl --backend space --log run.jsonl     # {state, questions:[{question, options, label}]}
jev log costs run.jsonl
jev log evals run.jsonl
```

## Limits that come from the model

These mirror the Space; see `jev/format.py`. Questions over 96 tokens and options over 64 are
refused, not truncated. At most 32 options. States are cut at 16,384 tokens. The scorer was trained
at 384 tokens with at most 16 options, so `Plan.beyond_train_len` and `Plan.over_option_cap` flag
when you are out of distribution. Long states get fewer branches per forward because each branch
carries its own copy of the state's cache.

## Commercial use

`train/retrain.sh` retrains the scorer on a GPU box without the CC-BY-NC ticket data. See
`train/README.md`.

## Development

```
python -m venv .venv && .venv/bin/pip install -e ".[space,dev]"
.venv/bin/pytest
```

`LocalBackend` is a direct port of the Space's app.py and has not been run on this machine (no GPU).
