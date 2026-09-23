"""jev CLI: decide, replay, verify.

  jev decide --state-file pr.txt --backend space -q "Risk?" -o low,medium,high --log run.jsonl
  jev eval cases.jsonl --backend space --log run.jsonl
  jev gate --backend http://gpu:8788 --log .jev/gate.jsonl   (stdin: PreToolUse JSON)
  jev serve --backend local --port 8788
  jev log verify run.jsonl
  jev log show run.jsonl [--agent NAME]
  jev log costs run.jsonl
  jev log evals run.jsonl
"""
from __future__ import annotations

import argparse
import json
import os
import sys

from .types import Question
from .backends import get_backend
from .log import DecisionLog


def _questions(args) -> list[Question]:
    qs = []
    if args.questions_json:
        for d in json.load(open(args.questions_json)):
            qs.append(Question(d["question"], tuple(d.get("options", ())), d.get("type", "choice"), d.get("id")))
    for q, o in zip(args.question or [], args.options or []):
        qs.append(Question(q, tuple(o.split(","))))
    for q in args.yes_no or []:
        qs.append(Question.yes_no(q))
    return qs


def cmd_decide(args) -> int:
    state = open(args.state_file).read() if args.state_file else (args.state or sys.stdin.read())
    qs = _questions(args)
    backend = get_backend(args.backend)
    res = backend.decide(state, qs)
    if args.log:
        with DecisionLog(args.log) as log:
            log.append_result(state, args.agent, res)
    out = {"backend": res.backend, "temperature": res.temperature, "plan": res.plan.to_dict(),
           "timing": res.timing.__dict__, "decisions": [d.to_dict() for d in res.decisions]}
    if args.json:
        print(json.dumps(out, indent=2, ensure_ascii=False))
    else:
        for d in res.decisions:
            dist = ", ".join(f"{o} {p:.2f}" for o, p in zip(d.question.options, d.probs))
            print(f"{d.question.question}\n  -> {d.chosen} ({d.conf:.2f})   [{dist}]")
        p = res.plan
        print(f"\n{p.n_questions} questions, {p.n_branches} branches; cached {p.cached_tokens} tokens vs naive "
              f"{p.naive_tokens} ({p.ratio:.1f}x); {res.timing.forwards} forwards, {res.timing.total_ms:.0f} ms")
    return 0


def cmd_eval(args) -> int:
    from .evals import load_cases, run_eval
    cases = load_cases(args.dataset)
    backend = get_backend(args.backend)
    log = DecisionLog(args.log) if args.log else DecisionLog()
    with log:
        r = run_eval(cases, backend, log, name=args.name or os.path.basename(args.dataset))
    print(json.dumps({"name": r.name, "backend": r.backend, "metrics": r.metrics, "wall_ms": round(r.wall_ms, 1),
                      "cost": r.cost, **({"per_case": r.per_case} if args.verbose else {})}, indent=2))
    return 0


def cmd_gate(args) -> int:
    from .gate import main_gate, HOOK_SNIPPET
    if args.print_hook_snippet:
        print(json.dumps(HOOK_SNIPPET, indent=2))
        return 0
    return main_gate(args)


def cmd_serve(args) -> int:
    from .server import serve
    serve(get_backend(args.backend), args.host, args.port, args.token)
    return 0


def cmd_log(args) -> int:
    log = DecisionLog(args.path)
    if args.log_cmd == "costs":
        print(json.dumps(log.costs(), indent=2))
        return 0
    if args.log_cmd == "evals":
        for r in log.kind("eval"):
            d = r.data
            if d.get("aborted"):
                print(f'{r.seq:6d} {r.ts} {d["name"]:>24s} {d["backend"]:>6s} ABORTED after {d["cases_done"]} case(s): {d["error"][:120]}')
                continue
            m = d["metrics"]
            print(f'{r.seq:6d} {r.ts} {d["name"]:>24s} {d["backend"]:>6s} n={m.get("n")} acc={m.get("accuracy")} '
                  f'ece={m.get("ece")} brier={m.get("brier")} ms={d["wall_ms"]}')
        return 0
    if args.log_cmd == "verify":
        try:
            log.verify()
        except ValueError as e:
            print(f"BROKEN: {e}")
            return 1
        print(f"ok: {len(log)} records, tip {log.tip[:16]}")
        return 0
    for r in log:
        d = r.data
        if r.kind == "decision":
            if args.agent and d["agent"] != args.agent:
                continue
            print(f'{r.seq:6d} {r.ts} decision step={d["step"]} {d["agent"]:>12s} {d["state_sha"][:8]} '
                  f'{d["question"]!r} -> {r.chosen} ({r.conf:.2f})')
        elif r.kind == "step":
            print(f'{r.seq:6d} {r.ts} step     #{d["step"]} {d["n_questions"]}q {d["n_branches"]}b '
                  f'{d["cached_tokens"]} tok (naive {d["naive_tokens"]}, {d["ratio"]}x) {d["forwards"]} fwd {d["wall_ms"]} ms')
        elif r.kind == "eval":
            print(f'{r.seq:6d} {r.ts} eval     {d["name"]} {d["metrics"]}')
        else:
            print(f'{r.seq:6d} {r.ts} {r.kind:8s} {json.dumps(d, ensure_ascii=False)[:160]}')
    return 0


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="jev")
    sub = p.add_subparsers(dest="cmd", required=True)
    d = sub.add_parser("decide", help="decide typed questions against a state")
    d.add_argument("--state"), d.add_argument("--state-file")
    d.add_argument("-q", "--question", action="append", help="question text; pair with -o")
    d.add_argument("-o", "--options", action="append", help="comma-separated options for the matching -q")
    d.add_argument("--yes-no", action="append", help="a yes/no question")
    d.add_argument("--questions-json", help="a JSON list of {question, options, type}")
    d.add_argument("--backend", default="mock", help="mock | space | local | replay:<log>")
    d.add_argument("--log", help="append decisions to this JSONL log")
    d.add_argument("--agent", default="cli")
    d.add_argument("--json", action="store_true")
    d.set_defaults(fn=cmd_decide)
    ev = sub.add_parser("eval", help="decide a labelled dataset and log accuracy / ECE / Brier plus costs")
    ev.add_argument("dataset", help="JSON list or JSONL of {state, questions:[{question, options, label}]}")
    ev.add_argument("--backend", default="mock")
    ev.add_argument("--log")
    ev.add_argument("--name")
    ev.add_argument("-v", "--verbose", action="store_true")
    ev.set_defaults(fn=cmd_eval)
    g = sub.add_parser("gate", help="PreToolUse hook: read the tool call on stdin, answer allow/ask/deny (always exit 0)")
    g.add_argument("--backend", default="mock")
    g.add_argument("--log")
    g.add_argument("--task", help="what the agent is working on, for the in-scope question")
    g.add_argument("--ask-at", type=float, default=0.6, help="P(destructive) at or above which to answer 'ask'")
    g.add_argument("--deny-at", type=float, default=1.01, help="P(destructive) at or above which to deny (default: never)")
    g.add_argument("--timeout", type=float, default=8.0)
    g.add_argument("--checkpoint", action="store_true", help="run `acyclic checkpoint --no-wait` when jev says to")
    g.add_argument("--repo")
    g.add_argument("--print-hook-snippet", action="store_true", help="print the settings.json hook entry and exit")
    g.set_defaults(fn=cmd_gate)
    sv = sub.add_parser("serve", help="serve a backend over HTTP for HttpBackend / --backend http://host:port")
    sv.add_argument("--backend", default="local")
    sv.add_argument("--host", default="127.0.0.1")
    sv.add_argument("--token", default=None, help="bearer token every /decide must carry (or JEV_SERVE_TOKEN); required off loopback")
    sv.add_argument("--port", type=int, default=8788)
    sv.set_defaults(fn=cmd_serve)
    lg = sub.add_parser("log", help="inspect a decision log")
    lg.add_argument("log_cmd", choices=["verify", "show", "costs", "evals"])
    lg.add_argument("path")
    lg.add_argument("--agent")
    lg.set_defaults(fn=cmd_log)
    args = p.parse_args(argv)
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
