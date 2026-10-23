"""Emit paired benchmark job configs; never starts Harbor or resolves credentials."""

import argparse
import hashlib
import json
import re
import subprocess
from pathlib import Path, PurePosixPath
from urllib.parse import urlsplit

from .contracts import Artifact, catalog, digest


STAGES = {"tb2-20": ("terminal-bench@2.0", 89, 20),
          "tb2-full": ("terminal-bench@2.0", 89, 89),
          "swe-verified": ("swe-bench-verified", 500, 500)}


def manifest(dataset: str, git_url: str, commit: str, paths: list[str]):
    digest(commit, 40)
    url = urlsplit(git_url)
    if (url.scheme != "https" or url.hostname != "github.com" or url.username
            or url.password or url.query or url.fragment
            or re.fullmatch(r"/[\w.-]+/[\w.-]+(?:\.git)?", url.path) is None):
        raise ValueError("dataset must use an explicit public GitHub HTTPS repository")
    if dataset not in {values[0] for values in STAGES.values()}:
        raise ValueError("unknown benchmark dataset")
    tasks = []
    for path in paths:
        task = PurePosixPath(path)
        if (not path or task.is_absolute() or ".." in task.parts or str(task) != path
                or re.fullmatch(r"[A-Za-z0-9_./-]+", path) is None):
            raise ValueError("task paths must be canonical repository-relative paths")
        tasks.append({"name": task.name, "path": path})
    if len({task["name"] for task in tasks}) != len(tasks):
        raise ValueError("duplicate task identity")
    return {"dataset": dataset, "git_url": git_url, "git_commit_id": commit,
            "tasks": sorted(tasks, key=lambda task: task["name"])}


def inventory(repository: str, dataset: str, git_url: str, commit: str):
    digest(commit, 40)
    # Git metadata only, at an exact commit already present locally. No fetch,
    # task contents, model, auth resolution or container execution.
    result = subprocess.run(["git", "-C", repository, "ls-tree", "-r", "--name-only",
                             commit], check=True, capture_output=True, text=True)
    paths = [path.removesuffix("/task.toml") for path in result.stdout.splitlines()
             if path.endswith("/task.toml")]
    return manifest(dataset, git_url, commit, paths)


def paired_plan(stage: str, dataset: dict, artifact: Artifact, binary_path: str,
                model: str, base_url: str, context_tokens: int, output_tokens: int,
                codex_version: str):
    label, count, selected_count = STAGES[stage]
    canonical = manifest(dataset["dataset"], dataset["git_url"], dataset["git_commit_id"],
                         [task["path"] for task in dataset["tasks"]])
    if canonical != dataset or label != dataset["dataset"] or len(dataset["tasks"]) != count:
        raise ValueError("wrong, noncanonical or incomplete dataset manifest")
    if re.fullmatch(r"\d+\.\d+\.\d+", codex_version) is None:
        raise ValueError("pin the exact Codex CLI release")
    catalog(model, base_url, context_tokens, output_tokens)
    if model.count("/") != 1:
        raise ValueError("pinned Codex comparator truncates nested model names")
    # Selection depends on task identity, never registry order or agent result.
    tasks = sorted(dataset["tasks"], key=lambda task: hashlib.sha256(
        ("predictor-tb2-v1\0" + label + "\0" + task["name"]).encode()).digest())[:selected_count]
    task_configs = [{"git_url": dataset["git_url"], "git_commit_id": dataset["git_commit_id"],
                     "path": task["path"]} for task in tasks]
    graphcoder = {"import_path": "scripts.benchmarks.graphcoder_harbor:GraphCoder",
                  "model_name": model, "kwargs": {
                      "graphcoder_binary": str(Path(binary_path).resolve()),
                      "graphcoder_sha256": artifact.sha256, "source_sha": artifact.source_sha,
                      "base_url": base_url, "context_tokens": context_tokens,
                      "output_tokens": output_tokens}}
    comparator = {"name": "codex", "model_name": model, "kwargs": {"version": codex_version},
                  "env": {"OPENAI_BASE_URL": base_url}}
    common = {"tasks": task_configs, "n_attempts": 1,
              "n_concurrent_trials": 1, "retry": {"max_retries": 0},
              "environment": {"type": "docker"}}
    prior_stage = {"tb2-full": "tb2-20", "swe-verified": "tb2-full"}.get(stage)
    return {"stage": stage, "prior_stage_receipt_required": prior_stage,
            "dataset_sha256": hashlib.sha256(
                json.dumps(dataset, sort_keys=True).encode()).hexdigest(),
            "task_names": [task["name"] for task in tasks],
            "source_sha": artifact.source_sha, "harbor_version": "0.24.0",
            "readiness": {"status": "BLOCKED", "requirements": [
                "zero-failure wide-main receipt for source_sha",
                "same-source three-platform live-pass receipt",
                "native FUSE mount admission inside every task container",
                "separately authorized credential-free model access for both agents",
                "matched effective reasoning/token budgets for both agents"]},
            "graphcoder": {**common, "agents": [graphcoder]},
            "codex": {**common, "agents": [comparator]}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    inputs = commands.add_parser("inventory")
    for name in ("repository", "dataset", "git-url", "commit"):
        inputs.add_argument("--" + name, required=True)
    inputs = commands.add_parser("plan")
    inputs.add_argument("--stage", choices=STAGES, required=True)
    for name in ("manifest", "binary", "binary-sha256", "source-sha", "model", "base-url", "codex-version"):
        inputs.add_argument("--" + name, required=True)
    for name in ("context-tokens", "output-tokens"):
        inputs.add_argument("--" + name, type=int, required=True)
    args = parser.parse_args()
    if args.command == "inventory":
        output = inventory(args.repository, args.dataset, args.git_url, args.commit)
    else:
        output = paired_plan(args.stage, json.loads(Path(args.manifest).read_text()),
            Artifact.read(args.binary, args.binary_sha256, args.source_sha), args.binary,
            args.model, args.base_url, args.context_tokens, args.output_tokens, args.codex_version)
    print(json.dumps(output, indent=2))


if __name__ == "__main__":
    main()
