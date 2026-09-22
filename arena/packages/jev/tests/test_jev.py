import json
import os
import tempfile
import unittest

from jev import (Question, Decision, MockBackend, DecisionLog, ReplayBackend, Swarm, FunctionAgent)
from jev import format as fmt


STATE = "Pull request #12: fix lazy example caching so visitor requests are forwarded."


class TestTypes(unittest.TestCase):
    def test_question_dedups_and_forces_noul(self):
        q = Question("Is it risky?", ("a", "a", "b"), "noul")
        self.assertEqual(q.options, ("yes", "no"))
        with self.assertRaises(ValueError):
            Question("x", ("only",))

    def test_score_expected(self):
        q = Question.score("Effort?", 1, 3)
        d = Decision.from_probs(q, [0.2, 0.3, 0.5])
        self.assertAlmostEqual(d.expected, 2.3)
        self.assertEqual(d.chosen, "3")


class TestFormat(unittest.TestCase):
    def test_plan_accounting(self):
        qs = [Question("A?", ("x", "y")), Question("B?", ("x", "y", "z"))]
        p = fmt.plan(STATE, qs, len)  # chars as tokens for exactness
        P = len(fmt.head_text(STATE))
        suffix = sum(len(fmt.tail_text(q.question, o)) for q in qs for o in q.options)
        self.assertEqual(p.n_branches, 5)
        self.assertEqual(p.cached_tokens, P + suffix)
        self.assertEqual(p.naive_tokens, P * 5 + suffix)

    def test_validate_refuses_long(self):
        with self.assertRaises(ValueError):
            fmt.validate([Question("q " * 400, ("a", "b"))])


class TestMock(unittest.TestCase):
    def test_deterministic_and_state_sensitive(self):
        b = MockBackend()
        qs = [Question("Risk?", ("low", "high")), Question.yes_no("Breaking?")]
        r1, r2 = b.decide(STATE, qs), b.decide(STATE, qs)
        self.assertEqual([d.probs for d in r1], [d.probs for d in r2])
        for d in r1:
            self.assertAlmostEqual(sum(d.probs), 1.0)
        r3 = b.decide(STATE + " (changed)", qs)
        self.assertNotEqual([d.probs for d in r1], [d.probs for d in r3])

    def test_chunks_over_max_questions(self):
        b = MockBackend()
        qs = [Question(f"Q{i}?", ("a", "b")) for i in range(50)]
        r = b.decide(STATE, qs)
        self.assertEqual(len(r), 50)
        self.assertEqual(len(b.calls), 3)  # 24 + 24 + 2
        self.assertEqual(r.plan.n_questions, 50)


class TestLog(unittest.TestCase):
    def test_chain_persist_replay_verify(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "log.jsonl")
            b = MockBackend()
            qs = [Question("Risk?", ("low", "high")), Question.yes_no("Breaking?")]
            with DecisionLog(path) as log:
                log.append_result(STATE, "a1", b.decide(STATE, qs), step=0)
                log.verify()
                self.assertEqual(len(log), 2)
            log2 = DecisionLog(path)
            self.assertEqual(len(log2), 2)
            log2.verify()
            self.assertEqual(log2[1].prev, log2[0].hash)
            # replay reproduces the distributions without the backend
            rb = ReplayBackend(log2)
            r = rb.decide(STATE, qs)
            self.assertEqual([x.probs for x in r], [x.probs for x in b.decide(STATE, qs)])
            self.assertEqual(rb.hits, 2)
            with self.assertRaises(KeyError):
                rb.decide(STATE, [Question("Unseen?", ("a", "b"))])
            # a fallback extends the log
            rb2 = ReplayBackend(log2, fallback=b)
            rb2.decide(STATE, [Question("Unseen?", ("a", "b"))])
            self.assertEqual(len(log2), 3)
            self.assertTrue(log2[2].meta["replay_miss"])
            log2.close()
            # tampering is detected
            with open(path) as f:
                lines = f.read().splitlines()
            rec = json.loads(lines[0]); rec["data"]["chosen_index"] = 1 - rec["data"]["chosen_index"]
            lines[0] = json.dumps(rec)
            with open(path, "w") as f:
                f.write("\n".join(lines) + "\n")
            with DecisionLog(path) as tampered:
                with self.assertRaises(ValueError):
                    tampered.verify()


class TestSwarm(unittest.TestCase):
    def test_one_batch_per_step_and_dispatch(self):
        b = MockBackend()
        acted = {}
        reviewer = FunctionAgent("reviewer", [Question("Risk?", ("low", "medium", "high")),
                                             Question.yes_no("Needs tests?", id="tests")],
                                 act=lambda ds, mem: acted.setdefault("reviewer", [d.chosen for d in ds]))
        router = FunctionAgent("router", lambda s, mem: [Question("Area?", ("backend", "frontend", "docs"))])
        lurker = FunctionAgent("lurker", [])
        sw = Swarm([reviewer, router, lurker], b)
        r = sw.step(STATE)
        self.assertEqual(len(b.calls), 1)          # the state was encoded once for all agents
        self.assertEqual(len(b.calls[0][1]), 3)
        self.assertEqual(r.n_questions, 3)
        self.assertEqual(len(r["reviewer"].decisions), 2)
        self.assertEqual(len(r["router"].decisions), 1)
        self.assertEqual(r["lurker"].decisions, [])
        self.assertIs(r["reviewer"]["tests"], r["reviewer"].decisions[1])
        self.assertEqual(acted["reviewer"], [d.chosen for d in r["reviewer"]])
        self.assertEqual(len(sw.log), 4)              # 3 decisions + 1 step cost record
        self.assertEqual(r.log_range, (0, 4))
        self.assertEqual({x.agent for x in sw.log.decisions()}, {"reviewer", "router"})
        cost = sw.log.kind("step")[0].data
        self.assertEqual(cost["n_questions"], 3)
        self.assertEqual(cost["n_branches"], 8)
        self.assertEqual(cost["agents"], {"reviewer": 2, "router": 1})
        sw.log.verify()
        # a second step with the same state produces the same decisions and chains on
        r2 = sw.step(STATE)
        self.assertEqual(r2.chosen(), r.chosen())
        self.assertEqual(r2.log_range, (4, 8))
        self.assertEqual(sw.log[4].step, 1)
        self.assertEqual(sw.log.costs()["steps"], 2)
        self.assertEqual(sw.log.costs()["n_questions"], 6)

    def test_memory_and_run_until(self):
        b = MockBackend()

        def ask(state, mem):
            mem["seen"] = mem.get("seen", 0) + 1
            return [Question.yes_no("Done?")]

        sw = Swarm([FunctionAgent("a", ask)], b)
        out = sw.run(["s1", "s2", "s3"], until=lambda r: r.step == 1)
        self.assertEqual(len(out), 2)
        self.assertEqual(sw.memory["a"]["seen"], 2)

    def test_replay_swarm_matches_live(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "log.jsonl")
            agents = lambda: [FunctionAgent("r", [Question("Risk?", ("low", "high"))]),
                              FunctionAgent("t", [Question.yes_no("Tests?")])]
            live = Swarm(agents(), MockBackend(), DecisionLog(path))
            live_out = [s.chosen() for s in live.run(["s1", "s2"])]
            live.log.close()
            with DecisionLog(path) as replay_log:
                rep = Swarm(agents(), ReplayBackend(replay_log))
                rep_out = [s.chosen() for s in rep.run(["s1", "s2"])]
            self.assertEqual(live_out, rep_out)


class TestSpaceParsing(unittest.TestCase):
    def test_parses_snapshot(self):
        from jev.backends.space import SpaceBackend

        class FakeClient:
            def predict(self, state, wire, compare, verify, api_name):
                assert api_name == "/run" and compare is False
                return {"scorer": {"questions": [{"id": w["id"], "options": w["options"],
                                                  "probs": [1.0 / len(w["options"])] * len(w["options"])}
                                                 for w in wire],
                                   "prefill_ms": 10.0, "branch_ms": 20.0, "forwards": 2, "tokens": 99},
                        "models": {"temperature": 1.75}, "done": True}

        b = SpaceBackend(client=FakeClient())
        r = b.decide(STATE, [Question("Risk?", ("low", "high", "mid"))])
        self.assertEqual(r.timing.forwards, 2)
        self.assertAlmostEqual(r[0].probs[0], 1 / 3)
        self.assertEqual(r.backend, "space")


if __name__ == "__main__":
    unittest.main()


class TestEvals(unittest.TestCase):
    def test_metrics_perfect_and_uniform(self):
        from jev.evals import metrics
        q = Question("A?", ("x", "y"))
        perfect = [Decision.from_probs(q, [1.0, 0.0]), Decision.from_probs(q, [0.0, 1.0])]
        m = metrics(perfect, ["x", "y"])
        self.assertEqual(m["accuracy"], 1.0); self.assertEqual(m["ece"], 0.0); self.assertEqual(m["brier"], 0.0)
        uniform = [Decision.from_probs(q, [0.5, 0.5]), Decision.from_probs(q, [0.5, 0.5])]
        m = metrics(uniform, ["x", "y"])
        self.assertEqual(m["accuracy"], 0.5)   # argmax is index 0 -> "x": one right, one wrong
        self.assertAlmostEqual(m["ece"], 0.0)  # conf 0.5, acc 0.5: calibrated
        self.assertAlmostEqual(m["brier"], 0.5)

    def test_run_eval_logs_everything(self):
        from jev.evals import Case, Labeled, run_eval
        cases = [Case("s1", [Labeled(Question("A?", ("x", "y")), "x"), Labeled(Question.yes_no("B?"), "no")], id="c1"),
                 Case("s2", [Labeled(Question("A?", ("x", "y")), "y")], id="c2")]
        log = DecisionLog()
        r = run_eval(cases, MockBackend(), log, name="unit")
        self.assertEqual(r.metrics["n"], 3)
        self.assertEqual(len(log.kind("decision")), 3)
        self.assertEqual(len(log.kind("step")), 2)
        ev = log.kind("eval")
        self.assertEqual(len(ev), 1)
        self.assertEqual(ev[0].data["metrics"], r.metrics)
        self.assertEqual(ev[0].data["cost"]["steps"], 2)
        self.assertEqual(log.decisions()[0].data["meta"]["labels"], ["x", "no"])
        log.verify()


class FakeAcyclic:
    """Scripted `acyclic` outputs in the installed binary's format, with real fork dirs on disk."""

    def __init__(self, root):
        self.root = root
        self.repo = os.path.join(root, "repo"); os.makedirs(self.repo)
        with open(os.path.join(self.repo, "app.py"), "w") as f:
            f.write("def add(a, b):\n    return a - b\n")
        self.forks = {}
        self.calls = []

    def __call__(self, argv):
        self.calls.append(argv)
        cmd = argv[0]
        if cmd == "fork":
            n = int(argv[2]); lines = []
            for i in range(n):
                fid = f"{len(self.forks) + 1:012x}"; path = os.path.join(self.root, "mnt", fid)
                import shutil; shutil.copytree(self.repo, path)
                self.forks[fid] = path
                lines.append(f"fork {fid}  (mount)  {path}")
            return "\n".join(lines) + f"\n{n} fork(s) ready\n"
        if cmd == "forks":
            return "".join(f"{fid}  mount  0s ago  {p}  base bc62cc665bee\n" for fid, p in self.forks.items()) or "no live forks\n"
        if cmd == "fork-diff":
            p = self.forks[argv[1]]; out = []
            for name in sorted(set(os.listdir(p)) | set(os.listdir(self.repo))):
                a, b = os.path.join(self.repo, name), os.path.join(p, name)
                if not os.path.exists(a): out.append(f"A {name}")
                elif not os.path.exists(b): out.append(f"D {name}")
                elif open(a).read() != open(b).read(): out.append(f"M {name}")
            return ("\n".join(out) + f"\n{len(out)} paths changed\n") if out else "no changes\n"
        if cmd == "fork-drop":
            self.forks.pop(argv[1]); return "fork dropped; its changes evaporated\n"
        if cmd == "promote":
            return f"promoted {argv[1]} -> generation deadbeef\n"
        raise AssertionError(argv)


class TestArena(unittest.TestCase):
    def test_fork_judge_resolve(self):
        from jev.acyclic import AcyclicCLI, ForkArena
        with tempfile.TemporaryDirectory() as d:
            fake = FakeAcyclic(d)
            cli = AcyclicCLI(fake.repo, runner=fake)
            log = DecisionLog()
            arena = ForkArena(cli, MockBackend(), log, task="make add() add instead of subtract")
            forks = arena.open(2)
            self.assertEqual([f.label for f in forks], ["A", "B"])
            self.assertEqual(forks[0].base, "bc62cc665bee")
            with open(os.path.join(forks[0].path, "app.py"), "w") as f:
                f.write("def add(a, b):\n    return a + b\n")
            with open(os.path.join(forks[1].path, "notes.txt"), "w") as f:
                f.write("todo\n")
            v = arena.judge(probe=lambda f: f"tests: {'pass' if f.label == 'A' else 'fail'}")
            self.assertIn("=== fork A", v.state); self.assertIn("-    return a - b", v.state)
            self.assertIn("+    return a + b", v.state); self.assertIn("A notes.txt", v.state)
            self.assertIn("tests: fail", v.state)
            self.assertEqual(v.step.n_questions, 1 + 2 + 2)
            self.assertEqual(set(v.per_fork), {"A", "B"})
            self.assertIsNotNone(v.per_fork["A"]["safe"].yes)
            self.assertEqual(sorted(v.step.chosen()["picker"].values())[0] in ("fork A", "fork B"), True)
            arena.resolve(v)
            self.assertEqual(v.promoted, v.winner.id)
            self.assertEqual(len(v.dropped), 1)
            kinds = [r.kind for r in log]
            self.assertEqual(kinds.count("note"), 1 + 1 + 1)  # fork_open, promote, fork_drop
            self.assertEqual(kinds.count("decision"), 5)
            self.assertEqual(kinds.count("step"), 1)
            self.assertEqual(log.kind("decision")[0].data["meta"]["forks"]["A"], forks[0].id)
            log.verify()

    def test_resolve_guards(self):
        from jev.acyclic import AcyclicCLI, ForkArena
        with tempfile.TemporaryDirectory() as d:
            fake = FakeAcyclic(d)
            arena = ForkArena(AcyclicCLI(fake.repo, runner=fake), MockBackend(), task="t")
            arena.open(2)
            v = arena.judge()
            arena.resolve(v, min_conf=1.01)
            self.assertIsNone(v.promoted)
            self.assertEqual(len(v.dropped), 2)
            self.assertEqual(fake.forks, {})


class TestGate(unittest.TestCase):
    def payload(self, cmd):
        return {"session_id": "s", "tool_name": "Bash", "tool_use_id": "t1", "tool_input": {"command": cmd}}

    def test_gate_allow_ask_deny_and_log(self):
        from jev.gate import Gate, render_tool_call
        log = DecisionLog()
        g = Gate(MockBackend(), log, task="fix the tests", ask_at=0.0, deny_at=1.01)
        v = g.judge(self.payload("rm -rf /"))
        self.assertEqual(v.decision, "ask")               # ask_at=0 -> everything asks
        self.assertIn("risk", v.reason)
        self.assertEqual(v.hook_json()["hookSpecificOutput"]["permissionDecision"], "ask")
        g2 = Gate(MockBackend(), DecisionLog(), deny_at=0.0)
        self.assertEqual(g2.judge(self.payload("ls")).decision, "deny")
        g3 = Gate(MockBackend(), DecisionLog())
        self.assertIn(g3.judge(self.payload("ls")).decision, ("allow", "ask"))
        kinds = [r.kind for r in log]
        self.assertEqual(kinds.count("decision"), 3)
        self.assertEqual(kinds.count("step"), 1)
        self.assertEqual(log.kind("note")[-1].data["event"], "gate_verdict")
        self.assertIn("Task the agent is working on", render_tool_call(self.payload("ls"), "fix"))

    def test_gate_never_blocks(self):
        from jev.gate import Gate

        class Broken(MockBackend):
            def _decide(self, state, questions):
                raise RuntimeError("boom")

        v = Gate(Broken(), DecisionLog()).judge(self.payload("ls"))
        self.assertEqual(v.decision, "allow")
        slow = MockBackend(latency_ms=500)
        v = Gate(slow, DecisionLog(), timeout_s=0.05).judge(self.payload("ls"))
        self.assertEqual(v.decision, "allow")
        self.assertIn("did not answer", v.reason)

    def test_cli_gate_stdin(self):
        import subprocess, sys
        p = subprocess.run([sys.executable, "-m", "jev.cli", "gate", "--backend", "mock"],
                           input=json.dumps(self.payload("git status")), capture_output=True, text=True,
                           env={**os.environ, "PYTHONPATH": os.path.join(os.path.dirname(__file__), "..", "src")})
        self.assertEqual(p.returncode, 0, p.stderr)
        out = json.loads(p.stdout)
        self.assertEqual(out["hookSpecificOutput"]["hookEventName"], "PreToolUse")
        self.assertIn(out["hookSpecificOutput"]["permissionDecision"], ("allow", "ask", "deny"))


class TestHttp(unittest.TestCase):
    def test_serve_and_client_roundtrip(self):
        import threading
        from jev.server import make_server
        from jev.backends.http import HttpBackend
        srv = make_server(MockBackend(), "127.0.0.1", 0)
        th = threading.Thread(target=srv.serve_forever, daemon=True); th.start()
        try:
            b = HttpBackend(f"http://127.0.0.1:{srv.server_address[1]}")
            self.assertTrue(b.health()["ok"])
            qs = [Question("Risk?", ("low", "high")), Question.yes_no("Breaking?")]
            r = b.decide(STATE, qs)
            self.assertEqual([d.probs for d in r], [d.probs for d in MockBackend().decide(STATE, qs)])
            self.assertEqual(r.backend, "http:mock")
            with self.assertRaises(ValueError):
                b.decide(STATE, [])
        finally:
            srv.shutdown(); srv.server_close()


class TestOpenRouter(unittest.TestCase):
    def fake_post(self, url, headers, body):
        self.bodies.append(body)
        assert body["max_tokens"] == 1 and body["logprobs"] is True
        assert headers["Authorization"] == "Bearer k"
        text = body["messages"][1]["content"]
        # favour "B" when the question mentions "risky", else "A"; leave C out of top-k entirely
        top = [{"token": "A", "logprob": -0.2}, {"token": " B", "logprob": -1.8}, {"token": "The", "logprob": -5.0}]
        if "risky" in text:
            top[0], top[1] = {"token": "B", "logprob": -0.1}, {"token": "A", "logprob": -2.5}
        return {"choices": [{"logprobs": {"content": [{"token": "A", "logprob": -0.2, "top_logprobs": top}]}}],
                "usage": {"prompt_tokens": 40, "completion_tokens": 1, "cost": 0.000002}}

    def test_letter_logprobs_and_costs(self):
        from jev.backends.openrouter import OpenRouterBackend, letter_logprobs, to_logits, render
        self.bodies = []
        b = OpenRouterBackend(model="test/model", api_key="k", post=self.fake_post)
        qs = [Question("Is this risky?", ("no", "yes", "unsure")), Question("Which area?", ("backend", "frontend"))]
        r = b.decide(STATE, qs)
        self.assertEqual(r[0].chosen, "yes")            # B
        self.assertEqual(r[1].chosen, "backend")        # A
        self.assertAlmostEqual(sum(r[0].probs), 1.0)
        self.assertLess(r[0].probs[2], r[0].probs[1])   # C floored below the seen letters
        self.assertEqual(r.timing.forwards, 2)
        self.assertEqual(r.timing.tokens, 82)
        self.assertAlmostEqual(r.timing.usd, 0.000004)
        self.assertEqual(r.backend, "openrouter:test/model")
        self.assertEqual(len(self.bodies), 2)
        self.assertIn("A. no\nB. yes\nC. unsure", render(STATE, qs[0]))
        self.assertEqual(letter_logprobs([{"token": " b.", "logprob": -1.0}], 3), [None, -1.0, None])
        self.assertEqual(to_logits([None, -1.0, None], 3.0), [-4.0, -1.0, -4.0])
        # dollars flow into the log's step cost
        log = DecisionLog()
        log.step_cost(0, STATE, r, 12.0)
        self.assertAlmostEqual(log.kind("step")[0].data["usd"], 0.000004)
        self.assertIn("usd", log.costs())

    def test_requires_key(self):
        from jev.backends.openrouter import OpenRouterBackend
        import jev.backends.openrouter as m
        env = os.environ.pop("OPENROUTER_API_KEY", None); env2 = os.environ.pop("OPENAI_API_KEY", None)
        real = m._key_from_files; m._key_from_files = lambda: None
        try:
            with self.assertRaises(RuntimeError):
                OpenRouterBackend()
        finally:
            m._key_from_files = real
            if env: os.environ["OPENROUTER_API_KEY"] = env
            if env2: os.environ["OPENAI_API_KEY"] = env2


class TestOpenRouterRetries(unittest.TestCase):
    def test_top_logprobs_cap_and_429_retry(self):
        from jev.backends.openrouter import OpenRouterBackend
        calls = []
        def post(url, headers, body):
            calls.append(dict(body))
            if body["top_logprobs"] > 5:
                raise RuntimeError(f"{url} -> 400: Range of top_logprobs should be [0, 5]")
            if len(calls) == 2:
                raise RuntimeError(f"{url} -> 429: rate-limited upstream")
            return {"choices": [{"logprobs": {"content": [{"token": "B", "logprob": -0.1,
                     "top_logprobs": [{"token": "B", "logprob": -0.1}, {"token": "A", "logprob": -2.0}]}]}}],
                    "usage": {"prompt_tokens": 10, "completion_tokens": 1, "cost": 0.0}}
        import jev.backends.openrouter as m
        real_sleep = m.time.sleep; m.time.sleep = lambda s: None
        try:
            b = OpenRouterBackend(api_key="k", post=post, retries=2)
            r = b.decide(STATE, [Question("Q?", ("x", "y"))])
        finally:
            m.time.sleep = real_sleep
        self.assertEqual(r[0].chosen, "y")
        self.assertEqual([c["top_logprobs"] for c in calls], [20, 5, 5])


class TestJevBackend(unittest.TestCase):
    def test_wire_roundtrip_and_cost(self):
        from jev.backends.jev import JevBackend, to_wire, from_wire
        seen = {}
        def post(url, headers, body):
            seen.update(body); assert headers["Authorization"] == "Bearer k"
            return {"model": "typesafe/jev-1.13-x", "answers": {
                "q0": {"type": "choice", "choice": "billing", "probabilities": {"billing": 0.9, "shipping": 0.1}, "confidence": 0.9},
                "q1": {"type": "score", "score": 1.26, "legend": {"0": "1", "1": "2", "2": "3"},
                       "probabilities": {"0": 0.1, "1": 0.6, "2": 0.3}, "confidence": 0.6},
                "q2": {"type": "noul", "noul": 0.99}},
                "usage": {"input_tokens": 400, "output_tokens": 50, "cost": 0.0000178}}
        b = JevBackend(api_key="k", post=post)
        qs = [Question("Topic?", ("billing", "shipping")), Question.score("Urgency?", 1, 3), Question.yes_no("Refund?")]
        r = b.decide(STATE, qs)
        self.assertEqual(seen["questions"]["q0"], {"type": "choice", "instructions": "Topic?", "criteria": {"billing": "billing", "shipping": "shipping"}})
        self.assertEqual(seen["questions"]["q1"]["criteria"], ["1", "2", "3"])
        self.assertEqual(seen["questions"]["q2"], {"type": "noul", "instructions": "Refund?"})
        self.assertEqual(r[0].chosen, "billing"); self.assertAlmostEqual(r[0].probs[0], 0.9)
        self.assertEqual(r[1].chosen, "2"); self.assertAlmostEqual(r[1].expected, 2.2)
        self.assertTrue(r[2].yes); self.assertAlmostEqual(r[2].prob("no"), 0.01)
        self.assertEqual(r.timing.forwards, 1); self.assertEqual(r.timing.tokens, 450)
        self.assertAlmostEqual(r.timing.usd, 0.0000178); self.assertEqual(r.backend, "jev:typesafe/jev-1.13")
        self.assertEqual(from_wire(Question("Q", ("a", "b")), {"probabilities": {}}), [0.5, 0.5])
