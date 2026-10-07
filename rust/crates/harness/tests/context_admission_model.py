"""Bounded request-admission model; no unbounded correctness claim.

Two distinct payloads, one request, exhaustive reachable finite state space.
State: current projection, retained admission, storage intact, dispatch count,
durable completion. Linearizable journal CAS and immutable authenticated storage
are assumptions. Admission claims the attempt before provider dispatch; a crash
at that boundary may lose dispatch, never authorize redispatch. Correspondence:
executor::run_model_step ModelStarted/load_json/append_if_tail/Model admission.
"""
from collections import deque


def successors(state):
    projection, admitted, intact, dispatches, completed = state
    yield (1 - projection, admitted, intact, dispatches, completed)  # future projection
    if admitted is None:
        yield (projection, projection, True, 0, False)  # crash after claim
        yield (projection, projection, True, 1, False)  # claim then dispatch
    else:
        yield (projection, admitted, False, dispatches, completed)  # storage fault
        if dispatches and intact:
            yield (projection, admitted, intact, dispatches, True)  # durable observation


def check(replay):
    initial = (0, None, True, 0, False)
    pending, seen = deque([initial]), {initial}
    while pending:
        state = pending.popleft()
        projection, admitted, intact, dispatches, completed = state
        assert dispatches <= 1
        if admitted is not None:
            value = replay(state)
            assert value == (admitted if intact else None), state
            assert not (completed and value is None and intact), state
        for next_state in successors(state):
            if next_state not in seen:
                seen.add(next_state)
                pending.append(next_state)
    return len(seen)


def retained_replay(state):
    return state[1] if state[2] else None


if __name__ == "__main__":
    count = check(retained_replay)
    for mutant in (lambda s: s[0] if s[2] else None, lambda s: s[1]):
        try:
            check(mutant)
        except AssertionError:
            continue
        raise AssertionError("negative control did not fail")
    print(f"PASS: {count} reachable states; changed-projection and missing-storage controls rejected")
