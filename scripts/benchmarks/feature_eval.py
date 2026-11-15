"""Score a feature on/off plan from its Harbor trial directories; never runs Harbor.

`jobs` writes each arm's Harbor job config. After both arms have run with
`harbor run -c CONFIG -o JOBS_DIR`, `score` reads `JOBS_DIR/<job_name>/<trial>/`:
`result.json` for the verifier outcome and `agent/usage.jsonl` (GraphCoder's
committed per-call usage export) for tokens. It prints one verdict per feature.
Trials are matched by the task path their own config records, never by
`task_name`: Harbor takes that from task.toml's package name when one exists.
"""

import argparse
import json
from pathlib import Path

from .plan import DECISION_RULE


def jobs(plan: dict, out: Path) -> list[Path]:
    """One Harbor job config per arm, named by its job_name."""
    _check(plan)
    out.mkdir(parents=True, exist_ok=True)
    written = []
    for feature in plan["features"]:
        for side in ("on", "off"):
            path = out / (feature[side]["job_name"] + ".json")
            path.write_text(json.dumps(feature[side], indent=2) + "\n")
            written.append(path)
    return written


def _check(plan: dict):
    """The arms of every pair differ only in their feature arm."""
    if plan.get("kind") != "feature-pairs":
        raise ValueError("not a feature on/off plan")
    names = [feature["name"] for feature in plan["features"]]
    if len(set(names)) != len(names):
        raise ValueError("duplicate feature")
    for feature in plan["features"]:
        on, off = feature["on"], feature["off"]
        strip = lambda job: {**job, "job_name": None, "agents": [
            {**agent, "kwargs": {**agent["kwargs"], "feature_arm": None}} for agent in job["agents"]]}
        if strip(on) != strip(off) or len(on["agents"]) != 1:
            raise ValueError("arms differ outside their feature arm: " + feature["name"])
        agent = on["agents"][0]
        if (agent["model_name"] != plan["model"]
                or agent["kwargs"]["source_sha"] != plan["source_sha"]
                or [task["path"].rsplit("/", 1)[-1] for task in on["tasks"]] != plan["task_names"]):
            raise ValueError("arm model, source or tasks differ from the plan: " + feature["name"])
        if agent["kwargs"]["feature_arm"] == off["agents"][0]["kwargs"]["feature_arm"]:
            raise ValueError("a feature's arms are identical: " + feature["name"])


def _tokens(path: Path):
    """Input and output totals over committed calls, None when either is unknown.
    Cached input is reported when every call reports it; it never decides."""
    if not path.is_file():
        return None
    totals = {"input": 0, "cached": 0, "output": 0}
    for line in path.read_text().splitlines():
        row = json.loads(line)
        usage = row["usage"]
        if row["version"] != 3 or not usage["completed"]:
            return None
        for name in ("input", "output"):
            if type(usage["tokens"][name]) is not int:
                return None
            totals[name] += usage["tokens"][name]
        cached = usage["tokens"]["cached"]
        totals["cached"] = (totals["cached"] + cached
                            if type(cached) is int and totals["cached"] is not None else None)
    return totals


def trials(job: Path) -> dict:
    """task path -> (solved, tokens). A trial that raised or has no reward of 1 failed."""
    result = {}
    for directory in sorted(path for path in job.iterdir() if path.is_dir()):
        record = json.loads((directory / "result.json").read_text())
        name = record["config"]["task"]["path"]
        if name in result:
            raise ValueError("more than one trial for task: " + name)
        rewards = (record.get("verifier_result") or {}).get("rewards") or {}
        solved = record.get("exception_info") is None and rewards.get("reward") == 1
        result[name] = (solved, _tokens(directory / "agent" / "usage.jsonl"))
    return result


def decide(placement: str, successes: int, tokens: int | None) -> str:
    """The plan's DECISION_RULE, from the on-minus-off deltas."""
    if tokens is None:
        return "INCOMPLETE"
    if successes >= 0 and tokens <= 0:
        return "ON"
    if placement == "in-turn" and successes > 0:
        return "JUSTIFY"
    return "OFF"


def score(plan: dict, jobs_dir: Path) -> dict:
    _check(plan)
    report = []
    for feature in plan["features"]:
        arms = {}
        for side in ("on", "off"):
            observed = trials(jobs_dir / feature[side]["job_name"])
            # Both arms answer for the plan's exact tasks, or nothing is compared.
            if sorted(observed) != sorted(task["path"] for task in feature[side]["tasks"]):
                raise ValueError(f"{feature['name']} {side} trials are not the plan's tasks")
            known = [tokens for _, tokens in observed.values()]
            arms[side] = {
                "tasks": len(observed),
                "successes": sum(solved for solved, _ in observed.values()),
                "failures": sum(not solved for solved, _ in observed.values()),
                "unknown_tokens": sum(tokens is None for tokens in known),
                "tokens": None if None in known else {
                    "input": sum(tokens["input"] for tokens in known),
                    "output": sum(tokens["output"] for tokens in known),
                    "cached": None if any(tokens["cached"] is None for tokens in known)
                    else sum(tokens["cached"] for tokens in known)},
            }
        successes = arms["on"]["successes"] - arms["off"]["successes"]
        total = lambda side: (None if arms[side]["tokens"] is None else
                              arms[side]["tokens"]["input"] + arms[side]["tokens"]["output"])
        tokens = None if None in (total("on"), total("off")) else total("on") - total("off")
        report.append({"name": feature["name"], "placement": feature["placement"],
                       "on": arms["on"], "off": arms["off"], "success_delta": successes,
                       "token_delta": tokens,
                       "verdict": decide(feature["placement"], successes, tokens)})
    return {"source_sha": plan["source_sha"], "model": plan["model"],
            "decision_rule": DECISION_RULE, "features": report}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name, target in (("jobs", "out"), ("score", "jobs-dir")):
        inputs = commands.add_parser(name)
        inputs.add_argument("--plan", required=True)
        inputs.add_argument("--" + target, required=True)
    args = parser.parse_args()
    plan = json.loads(Path(args.plan).read_text())
    if args.command == "jobs":
        print("\n".join(str(path) for path in jobs(plan, Path(args.out))))
    else:
        print(json.dumps(score(plan, Path(args.jobs_dir)), indent=2))


if __name__ == "__main__":
    main()
