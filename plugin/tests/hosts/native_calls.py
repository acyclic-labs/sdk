"""Byte-directed concurrent programs over the installed MCP session adapter.

Inputs select interleavings, queued sends, replayed spawns, duplicate waits and
reply consumption order. They are programs, not fabricated provider records;
the caller supplies the real McpClient and captured native completion stream.
"""
from collections import deque
from copy import deepcopy
import os
import signal
from native_mcp import McpFailure

# Retained input program, not a provider fixture: two sends, spawn replay,
# duplicate waits, alternating children, and reverse response consumption.
CORPUS = (bytes.fromhex("0eaa80"),)


def program(data):
    """Preserve each child's dependencies while interleaving the two chains."""
    data = bytes(data) or b"\0"
    flags = data[0]
    chains = []
    for side in ("left", "right"):
        chain = [("spawn", side, f"{side}:0")]
        chain.extend(("send", side, f"{side}:{turn}") for turn in range(1, 1 + flags % 3))
        if flags & 8:
            chain.append(("spawn", side, f"{side}:0"))
        chain.extend(("wait", side, None) for _ in range(1 + ((flags >> 2) & 1)))
        chains.append(deque(chain))
    calls = []
    for bit in range(sum(map(len, chains))):
        side = (data[(1 + bit // 8) % len(data)] >> (bit % 8)) & 1
        if not chains[side]:
            side ^= 1
        calls.append(chains[side].popleft())
    return calls


def verify(calls, replies, recovery):
    """Check real replies in submission order, independently of read order."""
    def require(condition, detail):
        if not condition:
            raise McpFailure("verify concurrent session calls", detail, recovery)

    require(len(calls) == len(replies), "an admitted call lost its reply")
    children = {}
    for (tool, reference, _), row in zip(calls, replies):
        require(isinstance(row, dict), "tool result is not an object")
        if reference not in children:
            require(tool == "spawn", "a child was used before spawn")
            require(isinstance(row.get("task"), str), "spawn lost its task identity")
            require(isinstance(row.get("cwd"), str), "spawn lost its physical view")
            require(all(row["task"] != other["task"] and row["cwd"] != other["cwd"]
                        for other in children.values()), "distinct children alias")
            children[reference] = {"task": row["task"], "cwd": row["cwd"],
                                   "accepted": [], "wait": None}
        child = children[reference]
        require(row.get("task") == child["task"], "reply changed its child's task")
        if tool in ("spawn", "send"):
            accepted = (row.get("id"), row.get("inbox"))
            require(isinstance(accepted[0], str) and type(accepted[1]) is int,
                    "acceptance lost its original prompt identity")
            if tool == "spawn" and child["accepted"]:
                require(accepted == child["accepted"][0], "spawn replay changed its prompt")
            else:
                require(all(accepted[0] != old[0] and accepted[1] > old[1]
                            for old in child["accepted"]), "accepted prompts alias or reorder")
                child["accepted"].append(accepted)
        else:
            reply = row.get("reply", {})
            require(isinstance(reply, dict), "wait reply is not an object")
            require(reply.get("kind") == "answered", "wait returned before settlement")
            outcome = reply.get("outcome") or {}
            require(isinstance(outcome, dict) and "Succeeded" in outcome,
                    "a generated input failed to complete")
            status = row.get("status", {})
            require(isinstance(status, dict) and status.get("settled") is True and
                    status.get("turn") == len(child["accepted"]),
                    "wait lost, duplicated or skipped an accepted turn")
            if child["wait"] is not None:
                require(reply == child["wait"], "concurrent waits disagree on the same cut")
            child["wait"] = reply
    require(len(children) == 2 and all(child["wait"] is not None
                                      for child in children.values()),
            "program did not join both original children")


def fuzz(data, client):
    """Submit the complete program before reading any of its responses."""
    calls = program(data)
    identifiers = [client.submit(*call) for call in calls]
    replies = [None] * len(calls)
    order = range(len(calls) - 1, -1, -1) if data and data[-1] & 128 else range(len(calls))
    for index in order:
        replies[index] = client.result(identifiers[index])
    verify(calls, replies, client.recovery)

    # These finite negative controls mutate actual production replies. They
    # check the oracle without planting a deadlock or inventing native wire.
    controls = []
    spawn = [i for i, call in enumerate(calls) if call[0] == "spawn"]
    second = next(i for i in spawn if calls[i][1] != calls[spawn[0]][1])
    aliased = deepcopy(replies)
    aliased[second]["task"] = replies[spawn[0]]["task"]
    controls.append(aliased)
    early = deepcopy(replies)
    wait = next(i for i, call in enumerate(calls) if call[0] == "wait")
    early[wait]["reply"] = {"kind": "pending"}
    controls.append(early)
    skipped = deepcopy(replies)
    skipped[wait]["status"]["turn"] += 1
    controls.append(skipped)
    for changed in controls:
        try:
            verify(calls, changed, client.recovery)
        except McpFailure:
            continue
        raise McpFailure("qualify concurrent session oracle",
                         "a planted identity, progress or turn-count defect escaped",
                         client.recovery)
    return {"input": bytes(data).hex(), "calls": calls, "replies": replies,
            "negative_controls": len(controls)}


def lost_producer(client):
    """Close the real MCP producer after admitting a call it cannot answer."""
    process = client.process
    try:
        # Wait for the kernel's stop event before writing. The outstanding
        # request then has no possible answer when its producer is killed.
        os.kill(process.pid, signal.SIGSTOP)
        _, status = os.waitpid(process.pid, os.WUNTRACED)
        if not os.WIFSTOPPED(status):
            raise McpFailure("stop original MCP producer", "no stop event", client.recovery)
        identifier = client.submit("wait", "left")
    except OSError as error:
        raise McpFailure("stop original MCP producer", str(error), client.recovery) from error
    finally:
        process.kill()
        process.wait()
    try:
        client.result(identifier)
    except McpFailure as error:
        if "producer closed before response" not in str(error):
            raise
        return {"request": identifier, "exit": process.returncode, "failure": str(error)}
    raise McpFailure("qualify lost MCP producer", "an unanswered call succeeded", client.recovery)
