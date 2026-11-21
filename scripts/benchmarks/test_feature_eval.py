"""Mock tier of the feature on/off evaluation: the real plan, arm arguments,
run command and scorer, with a scripted stand-in agent. No model, Harbor,
Docker, network or credential is used."""

import copy
import hashlib
import json
import shlex
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

from .contracts import Artifact, arm, arm_arguments, run_command
from .feature_eval import decide, jobs, score, trials
from .plan import feature_plan, manifest
from .test_contracts import CONTRACTS_COMMAND

REGISTRY = json.loads((Path(__file__).resolve().parent / "features.json").read_text())

# Deterministic stand-in: compaction (the default) solves every task with
# fewer input tokens; --no-compaction solves only even-numbered tasks.
AGENT = """#!/usr/bin/python3
import json, sys
""" + CONTRACTS_COMMAND + """argv = sys.argv[1:]
usage = argv[argv.index("--usage-jsonl") + 1]
prompt = argv[-1].removeprefix("--prompt=")
off = "--no-compaction" in argv
for index, value in enumerate(argv):
    if value == "--home-file":
        open(argv[index + 1].split("=", 1)[1]).read()
tokens = {"input": 3000 if off else 2000, "cached": 500, "cache_write": None,
          "output": 100, "reasoning": None}
with open(usage, "w") as file:
    file.write(json.dumps({"version": 3, "usage": {"completed": True, "tokens": tokens},
                           "estimated_cost": {}}) + "\\n")
print(json.dumps({"solved": not off or int(prompt[-1]) % 2 == 0}))
"""


class FeatureEval(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="predictor-features-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.binary = self.root / "graphcoder"
        self.binary.write_text(AGENT)
        self.binary.chmod(0o755)
        sha = hashlib.sha256(self.binary.read_bytes()).hexdigest()
        self.artifact = Artifact.read(str(self.binary), sha, "a" * 40)
        self.dataset = manifest("terminal-bench@2.0", "https://github.com/example/benchmark.git",
                                "b" * 40, [f"tasks/task-{index:03}" for index in range(89)])

    def plan(self, registry=REGISTRY, base_url="http://127.0.0.1:9999/v1"):
        return feature_plan("tb2-20", self.dataset, self.artifact, str(self.binary),
                            "openai/fixture", base_url, 100, 10, registry)

    def run_arms(self, plan, jobs_dir):
        """Each trial as Harbor lays it out: result.json plus agent/usage.jsonl."""
        for feature in plan["features"]:
            for side in ("on", "off"):
                path = {config["path"].rsplit("/", 1)[-1]: config for config in feature[side]["tasks"]}
                agent = feature[side]["agents"][0]
                for task in plan["task_names"]:
                    trial = jobs_dir / feature[side]["job_name"] / (task + "__trial")
                    logs, state = trial / "agent", self.root / "state" / side / task
                    logs.mkdir(parents=True)
                    arguments, uploads = arm_arguments(agent["kwargs"]["feature_arm"], str(state))
                    for target, text in uploads.items():
                        Path(target).parent.mkdir(parents=True, exist_ok=True)
                        Path(target).write_text(text)
                    command = run_command(str(self.binary), str(state), str(logs),
                                          agent["model_name"], "solve " + task,
                                          arguments=tuple(arguments))
                    subprocess.run(["bash", "-c", command], check=True, cwd=self.root)
                    solved = json.loads((logs / "graphcoder.jsonl").read_text())["solved"]
                    # Harbor names a packaged task by its task.toml name, not its directory.
                    (trial / "result.json").write_text(json.dumps({
                        "task_name": "benchmark/" + task, "exception_info": None,
                        "config": {"task": path[task], "agent": agent},
                        "verifier_result": {"rewards": {"reward": 1 if solved else 0}}}))

    def test_mock_tier_runs_plan_commands_and_scores_paired_arms(self):
        plan = self.plan()
        self.assertEqual(plan["readiness"]["status"], "BLOCKED")
        written = jobs(plan, self.root / "configs")
        self.assertEqual(sorted(path.name for path in written),
                         ["r44-context-management-off.json", "r44-context-management-on.json"])
        jobs_dir = self.root / "jobs"
        self.run_arms(plan, jobs_dir)
        # Trials answer for the task paths the plan wrote, whatever Harbor names them.
        self.assertEqual(sorted(trials(jobs_dir / "r44-context-management-on",
                                       plan["features"][0]["on"])),
                         sorted(task["path"] for task in plan["features"][0]["on"]["tasks"]))
        (result,) = score(plan, jobs_dir)["features"]
        tasks = len(plan["task_names"])
        even = sum(int(name[-1]) % 2 == 0 for name in plan["task_names"])
        self.assertEqual(tasks, 20)
        self.assertEqual((result["on"]["successes"], result["off"]["successes"]), (tasks, even))
        self.assertEqual(result["off"]["failures"], tasks - even)
        self.assertEqual(result["on"]["tokens"], {"input": 2000 * tasks, "output": 100 * tasks,
                                                  "cached": 500 * tasks})
        self.assertEqual(result["token_delta"], -1000 * tasks)
        self.assertEqual(result["verdict"], "ON")
        # An unknown count is never zero: one unfinished call blocks the verdict.
        usage = next((jobs_dir / "r44-context-management-off").iterdir()) / "agent" / "usage.jsonl"
        row = json.loads(usage.read_text())
        row["usage"]["completed"] = False
        usage.write_text(json.dumps(row) + "\n")
        (result,) = score(plan, jobs_dir)["features"]
        self.assertEqual((result["off"]["unknown_tokens"], result["verdict"]), (1, "INCOMPLETE"))

    def test_scorer_refuses_arms_that_are_not_one_pair(self):
        plan = self.plan()
        jobs_dir = self.root / "jobs"
        self.run_arms(plan, jobs_dir)
        moved = copy.deepcopy(plan)
        moved["features"][0]["off"]["agents"][0]["model_name"] = "openai/other"
        with self.assertRaisesRegex(ValueError, "differ outside their feature arm"):
            score(moved, jobs_dir)
        # A later plan's jobs share these names; trials of another model are refused.
        later = copy.deepcopy(plan)
        for side in ("on", "off"):
            later["features"][0][side]["agents"][0]["model_name"] = "openai/later"
        later["model"] = "openai/later"
        with self.assertRaisesRegex(ValueError, "did not run the planned agent"):
            score(later, jobs_dir)
        # So are trials of the same task paths at another dataset revision.
        revised = copy.deepcopy(plan)
        for side in ("on", "off"):
            for task in revised["features"][0][side]["tasks"]:
                task["git_commit_id"] = "c" * 40
        with self.assertRaisesRegex(ValueError, "did not run the planned task revision"):
            score(revised, jobs_dir)
        shutil.rmtree(next((jobs_dir / "r44-context-management-on").iterdir()))
        with self.assertRaisesRegex(ValueError, "not the plan's tasks"):
            score(plan, jobs_dir)

    def test_decision_rule_keeps_only_net_gains(self):
        for placement, successes, tokens, verdict in (
                ("off-turn", 0, 0, "ON"), ("off-turn", 2, -5, "ON"),
                ("off-turn", 2, 5, "OFF"), ("off-turn", -1, -5, "OFF"),
                ("in-turn", 2, 5, "JUSTIFY"), ("in-turn", 0, 5, "OFF"),
                ("in-turn", -1, -5, "OFF"), ("in-turn", 3, None, "INCOMPLETE")):
            self.assertEqual(decide(placement, successes, tokens), verdict,
                             (placement, successes, tokens))

    def test_arms_admit_only_feature_flags_and_canonical_home_files(self):
        for definition in ({"flags": ["--credentials"], "home_files": {}},
                           {"flags": ["--model"], "home_files": {}},
                           {"flags": ["--no-compaction", "--no-compaction"], "home_files": {}},
                           {"flags": [], "home_files": {"../escape": "x"}},
                           {"flags": [], "home_files": {"/absolute": "x"}},
                           {"flags": [], "home_files": {}, "env": {}}):
            with self.assertRaises(ValueError, msg=definition):
                arm(definition)
        arguments, uploads = arm_arguments({"flags": [], "home_files": {"policy/a.md": "x"}}, "/s")
        self.assertEqual(arguments, ["--home-file", "policy/a.md=/s/home-files/0"])
        self.assertEqual(uploads, {"/s/home-files/0": "x"})
        # Every home file reaches the command line as its own flag and value.
        arguments, _ = arm_arguments(
            {"flags": [], "home_files": {"policy/a.md": "x", "policy/b.md": "y"}}, "/s")
        command = shlex.split(run_command("/g", "/s", "/l", "openai/fixture", "solve",
                                          compaction=False, arguments=tuple(arguments)))
        self.assertEqual([command[index + 1] for index, value in enumerate(command)
                          if value == "--home-file"],
                         ["policy/a.md=/s/home-files/0", "policy/b.md=/s/home-files/1"])
        self.assertEqual(command.count("--no-compaction"), 1)
        # A counterless catalog already disables compaction: the pair would
        # compare one configuration with itself, so it is not planned.
        with self.assertRaisesRegex(ValueError, "effective arms are identical"):
            self.plan(base_url="http://127.0.0.1:9999/deepseek/v1")


if __name__ == "__main__":
    unittest.main()
