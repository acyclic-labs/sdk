"""Prove regressions reject removed argument, identity and admission boundaries."""

import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


CONTROLS = [
    ("worker providers omitted from the catalog", "graphcoder_harbor.py",
     "adapter=adapter, providers=providers", "adapter=adapter, providers=None",
     "test_harbor.HarborContracts.test_mixed_providers_reach_install_doctor_and_execution"),
    ("root provider identity rewritten", "contracts.py",
     '"--model", model,', '"--model", "benchmark/" + model.split("/", 1)[1],',
     "test_harbor.HarborContracts.test_mixed_providers_reach_install_doctor_and_execution"),
    ("doctor loses counterless execution policy", "graphcoder_harbor.py",
     "self.binary, self.state, self.model_name, compaction=self.compaction",
     "self.binary, self.state, self.model_name",
     "test_harbor.HarborContracts.test_counterless_catalog_controls_doctor_and_execution"),
    ("execution loses counterless policy", "graphcoder_harbor.py",
     "instruction,\n            compaction=self.compaction", "instruction",
     "test_harbor.HarborContracts.test_counterless_catalog_controls_doctor_and_execution"),
    ("task skills omitted from the project", "graphcoder_harbor.py",
     "if self.skills_dir:", "if False:",
     "test_harbor.HarborContracts.test_task_skills_are_project_files_and_copy_failure_revokes_execution"),
    ("task skills capability omitted", "graphcoder_harbor.py",
     "AgentCapabilities(skills=True)", "AgentCapabilities(skills=False)",
     "test_harbor.HarborContracts.test_task_skills_are_project_files_and_copy_failure_revokes_execution"),
    ("task skills copy failure ignored", "graphcoder_harbor.py",
     '+ " .agents/skills/"))', '+ " .agents/skills/ || true"))',
     "test_harbor.HarborContracts.test_task_skills_are_project_files_and_copy_failure_revokes_execution"),
    ("inherited environment accepted", "graphcoder_harbor.py",
     "if self.extra_env:", "if False:",
     "test_harbor.HarborContracts.test_extra_environment_is_rejected_before_setup"),
    ("private signing custody selection omitted", "contracts.py",
     '        "ACYCLIC_SIGNING_CUSTODY=private-file",\n', '',
     "test_contracts.BenchmarkContracts.test_shell_metacharacters_roundtrip_as_prompt_data_and_env_is_empty"),
    ("comparator changes model identity", "plan.py",
     'if model.count("/") != 1:', 'if False:',
     "test_harbor.HarborContracts.test_comparator_projection_cannot_change_the_effective_model"),
    ("unchecked runner bootstrap", "bootstrap.py",
     "if hashlib.sha256(payload).hexdigest() != UV_SHA256:", "if False:",
     "test_contracts.BenchmarkContracts.test_bootstrap_rejects_unpinned_bytes_before_archive_parsing"),
    ("unframed shell arguments", "contracts.py", "shlex.join(arguments)", '" ".join(arguments)',
     "test_contracts.BenchmarkContracts.test_shell_metacharacters_roundtrip_as_prompt_data_and_env_is_empty"),
    ("unchecked artifact construction", "contracts.py",
     "if not self.contents or hashlib.sha256(self.contents).hexdigest() != self.sha256:", "if False:",
     "test_contracts.BenchmarkContracts.test_artifact_identity_is_verified_at_every_construction_entry"),
    ("incomplete task inventory", "plan.py", ' or len(dataset["tasks"]) != count', '',
     "test_contracts.BenchmarkContracts.test_incomplete_or_ambiguous_task_universes_cannot_be_planned"),
    ("logging masks failed agent", "contracts.py",
     'str(PurePosixPath(logs) / "graphcoder.stderr")))',
     'str(PurePosixPath(logs) / "graphcoder.stderr")) + " ; true")',
     "test_contracts.BenchmarkContracts.test_log_redirection_preserves_agent_failure_status"),
    ("failed reinstall retains authority", "graphcoder_harbor.py",
     "async def install(self, environment):\n        self._installed_environment = None",
     "async def install(self, environment):",
     "test_harbor.HarborContracts.test_install_run_and_failed_reinstall_fence_execution"),
    ("installation authority crosses environments", "graphcoder_harbor.py",
     "if self._installed_environment is not environment:",
     "if self._installed_environment is None:",
     "test_harbor.HarborContracts.test_install_run_and_failed_reinstall_fence_execution"),
    ("feature arms differ outside the arm", "feature_eval.py",
     "if strip(on) != strip(off) or len(on[\"agents\"]) != 1:",
     "if len(on[\"agents\"]) != 1:",
     "test_feature_eval.FeatureEval.test_scorer_refuses_arms_that_are_not_one_pair"),
    ("unpaired trials scored", "feature_eval.py",
     "if sorted(observed) != sorted(task[\"path\"] for task in feature[side][\"tasks\"]):",
     "if False:",
     "test_feature_eval.FeatureEval.test_scorer_refuses_arms_that_are_not_one_pair"),
    ("trials matched by Harbor's task name", "feature_eval.py",
     'name = task["path"]', 'name = record["task_name"]',
     "test_feature_eval.FeatureEval.test_mock_tier_runs_plan_commands_and_scores_paired_arms"),
    ("trials of another plan's agent scored", "feature_eval.py",
     "if planned_keys(agent, record[\"config\"].get(\"agent\") or {}) != agent:", "if False:",
     "test_feature_eval.FeatureEval.test_scorer_refuses_arms_that_are_not_one_pair"),
    ("trials of another dataset revision scored", "feature_eval.py",
     "if name in planned and planned_keys(planned[name], task) != planned[name]:", "if False:",
     "test_feature_eval.FeatureEval.test_scorer_refuses_arms_that_are_not_one_pair"),
    ("repeated home-file flags collapsed", "contracts.py",
     '"--no-compaction" in arguments else ["--no-compaction"]) + arguments',
     '"--no-compaction" in arguments else ["--no-compaction"]) + list(dict.fromkeys(arguments))',
     "test_feature_eval.FeatureEval.test_arms_admit_only_feature_flags_and_canonical_home_files"),
    ("unknown tokens counted as zero", "feature_eval.py",
     "if row[\"version\"] != 3 or not usage[\"completed\"]:", "if row[\"version\"] != 3:",
     "test_feature_eval.FeatureEval.test_mock_tier_runs_plan_commands_and_scores_paired_arms"),
    ("off-turn cost justified", "feature_eval.py",
     "if placement == \"in-turn\" and successes > 0:", "if successes > 0:",
     "test_feature_eval.FeatureEval.test_decision_rule_keeps_only_net_gains"),
    ("arm admits adapter-owned flags", "contracts.py",
     "or not set(flags) <= ARM_FLAGS):", "):",
     "test_feature_eval.FeatureEval.test_arms_admit_only_feature_flags_and_canonical_home_files"),
    ("control runner stops at the first problem", "negative_controls.py",
     "            problems.append((label, problem))\n",
     "            problems.append((label, problem))\n            break\n",
     "test_contracts.BenchmarkContracts.test_negative_control_runner_reports_every_stale_anchor"),
    ("failed outer setup retains authority", "graphcoder_harbor.py",
     "async def setup(self, environment):\n        self._installed_environment = None",
     "async def setup(self, environment):",
     "test_harbor.HarborContracts.test_install_run_and_failed_reinstall_fence_execution"),
]


def _apply(label: str, filename: str, old: str, new: str, test: str, source: Path,
           temporary: str) -> str | None:
    """One control in a private copy; None when it rejects, else what went wrong."""
    original = source / filename
    text = original.read_text() if original.is_file() else ""
    if text.count(old) != 1:
        return f"no longer targets one production boundary ({text.count(old)} matches)"
    package = Path(temporary) / "scripts" / "benchmarks"
    package.mkdir(parents=True)
    for file in [*source.glob("*.py"), *source.glob("*.json")]:
        shutil.copyfile(file, package / file.name)
    target = package / filename
    target.write_text(text.replace(old, new))
    # A syntax/import failure is not a valid negative control. Compile
    # source in memory before running the actual named regression.
    try:
        compile(target.read_text(), str(target), "exec")
    except SyntaxError as error:
        return "mutation does not compile: " + str(error)
    result = subprocess.run([sys.executable, "-m", "unittest", "scripts.benchmarks." + test],
        cwd=temporary, env={**os.environ, "PYTHONPATH": temporary, "PYTHONDONTWRITEBYTECODE": "1"},
        capture_output=True, text=True)
    if result.returncode == 0 or "FAIL:" not in result.stderr:
        return "failed to reject\n" + result.stderr
    return None


def evaluate(controls, source: Path) -> list[tuple[str, str]]:
    """Every control runs; the result lists each one that is stale or did not reject."""
    problems = []
    for label, filename, old, new, test in controls:
        with tempfile.TemporaryDirectory(prefix="predictor-negative-") as temporary:
            problem = _apply(label, filename, old, new, test, source, temporary)
        if problem is None:
            print("negative control REJECTED:", label)
        else:
            print("negative control PROBLEM:", label, "-", problem.splitlines()[0])
            problems.append((label, problem))
    return problems


def main():
    problems = evaluate(CONTROLS, Path(__file__).resolve().parent)
    print(f"negative controls: {len(CONTROLS) - len(problems)} of {len(CONTROLS)} rejected")
    if problems:
        raise SystemExit("negative controls with problems:\n" + "\n".join(
            f"- {label}: {problem}" for label, problem in problems))


if __name__ == "__main__":
    main()
