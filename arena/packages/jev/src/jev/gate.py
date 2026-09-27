"""jev as a pre-tool gate.

`jev gate` reads a Claude Code style PreToolUse payload on stdin, renders the
tool call as a state, asks System One a few typed questions (is this call
risky, does it need a checkpoint first, is it inside the task), logs the
decisions, and writes a hook decision on stdout. It follows acyclic's hook
contract: never block the agent by accident. Any error, timeout or missing
backend results in "allow" and exit 0. Denying is opt-in via thresholds.

Hook output shape (Claude Code):
  {"hookSpecificOutput": {"hookEventName": "PreToolUse",
                          "permissionDecision": "allow" | "ask" | "deny",
                          "permissionDecisionReason": "..."}}
"""
from __future__ import annotations

import json
import os
import sys
import threading
from dataclasses import dataclass
from typing import Optional

from .types import Question, Decision
from .backends.base import Backend
from .log import DecisionLog
from .swarm import Swarm, FunctionAgent

MAX_INPUT_CHARS = 1_200


@dataclass
class GateVerdict:
    decision: str            # allow | ask | deny
    reason: str
    risk: Optional[Decision] = None
    checkpoint: Optional[Decision] = None
    in_scope: Optional[Decision] = None

    def hook_json(self) -> dict:
        return {"hookSpecificOutput": {"hookEventName": "PreToolUse", "permissionDecision": self.decision,
                                       "permissionDecisionReason": self.reason}}


def render_tool_call(payload: dict, task: Optional[str] = None) -> str:
    tool = payload.get("tool_name") or ("Bash" if payload.get("command") else "unknown")
    inp = payload.get("tool_input") or {}
    if payload.get("command") and not inp:
        inp = {"command": payload["command"]}
    body = json.dumps(inp, ensure_ascii=False, indent=1)
    if len(body) > MAX_INPUT_CHARS:
        body = body[:MAX_INPUT_CHARS] + "\n... (truncated)"
    lines = []
    if task:
        lines += ["Task the agent is working on:", task.strip(), ""]
    lines += [f"The coding agent is about to call tool: {tool}", "Tool input:", body]
    return "\n".join(lines)


GATE_QUESTIONS = [
    Question("How risky is this tool call to the repository or the machine?", ("harmless", "moderate", "destructive"),
             id="risk"),
    Question.yes_no("Should the working tree be checkpointed before this call runs?", id="checkpoint"),
    Question.yes_no("Is this tool call a reasonable step for the task?", id="in_scope"),
]


class Gate:
    def __init__(self, backend: Backend, log: Optional[DecisionLog] = None, *, task: Optional[str] = None,
                 deny_at: float = 1.01, ask_at: float = 0.6, timeout_s: float = 8.0, agent: str = "gate"):
        """deny_at / ask_at: P(destructive) thresholds. Defaults never deny; ask above 0.6."""
        self.backend = backend
        self.log = log if log is not None else DecisionLog()
        self.task = task
        self.deny_at = deny_at
        self.ask_at = ask_at
        self.timeout_s = timeout_s
        self.swarm = Swarm([FunctionAgent(agent, GATE_QUESTIONS)], backend, self.log)
        self.agent = agent

    def _decide(self, state: str, meta: dict):
        return self.swarm.step(state, meta=meta)

    def judge(self, payload: dict) -> GateVerdict:
        state = render_tool_call(payload, self.task)
        meta = {"gate": True, "tool": payload.get("tool_name"), "tool_use_id": payload.get("tool_use_id"),
                "session_id": payload.get("session_id")}
        box: dict = {}

        def work():
            try:
                box["step"] = self._decide(state, meta)
            except Exception as e:  # noqa: BLE001
                box["error"] = e

        t = threading.Thread(target=work, daemon=True)
        t.start()
        t.join(self.timeout_s)
        if t.is_alive():
            self.log.note("gate_timeout", timeout_s=self.timeout_s, **meta)
            return GateVerdict("allow", f"jev gate: backend did not answer within {self.timeout_s}s; allowed")
        if "error" in box:
            self.log.note("gate_error", error=repr(box["error"]), **meta)
            return GateVerdict("allow", f"jev gate: backend error, allowed ({box['error']!r})")
        r = box["step"][self.agent]
        risk, ckpt, scope = r["risk"], r["checkpoint"], r["in_scope"]
        p_destr = risk.prob("destructive")
        reason = (f"jev gate: risk {risk.chosen} (destructive {p_destr:.2f}), checkpoint first: {ckpt.chosen} "
                  f"({ckpt.conf:.2f}), in scope: {scope.chosen} ({scope.conf:.2f})")
        if p_destr >= self.deny_at:
            decision = "deny"
        elif p_destr >= self.ask_at:
            decision = "ask"
        else:
            decision = "allow"
        self.log.note("gate_verdict", decision=decision, p_destructive=round(p_destr, 4), **meta)
        return GateVerdict(decision, reason, risk, ckpt, scope)


def main_gate(args) -> int:
    """`jev gate` entrypoint. Always exits 0."""
    from .backends import get_backend
    try:
        payload = json.load(sys.stdin) if not sys.stdin.isatty() else {}
    except Exception:  # noqa: BLE001
        payload = {}
    try:
        backend = get_backend(args.backend)
        log = DecisionLog(args.log) if args.log else DecisionLog()
        with log:
            g = Gate(backend, log, task=args.task, deny_at=args.deny_at, ask_at=args.ask_at,
                     timeout_s=args.timeout)
            v = g.judge(payload)
    except Exception as e:  # noqa: BLE001
        v = GateVerdict("allow", f"jev gate: not run, allowed ({e!r})")
    if args.checkpoint and v.checkpoint is not None and v.checkpoint.yes and os.environ.get("ACYCLIC_DISABLED") != "1":
        try:
            from .acyclic import AcyclicCLI
            AcyclicCLI(args.repo or ".").run("--hook", "checkpoint", "-m", "jev gate", "--no-wait")
        except Exception:  # noqa: BLE001
            pass
    sys.stdout.write(json.dumps(v.hook_json()) + "\n")
    return 0


HOOK_SNIPPET = {
    "hooks": {"PreToolUse": [{"matcher": "Bash|Edit|Write|MultiEdit",
                              "hooks": [{"type": "command",
                                         "command": "jev gate --backend space --log .jev/gate.jsonl"}]}]}}
