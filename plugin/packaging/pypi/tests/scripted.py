"""Deterministic Pydantic AI models: no credentials, no network.

``scripted(plan)`` builds a FunctionModel that, on the first request of a run,
issues the tool calls ``plan(prompt)`` returns, then answers ``done`` once the
tool results come back.
"""

from __future__ import annotations

from collections.abc import Callable

from pydantic_ai.messages import (
    ModelMessage,
    ModelRequest,
    ModelResponse,
    TextPart,
    ToolCallPart,
    ToolReturnPart,
    UserPromptPart,
)
from pydantic_ai.models.function import AgentInfo, FunctionModel

Plan = Callable[[str], list[tuple[str, dict]]]


def scripted(plan: Plan) -> FunctionModel:
    def model(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        last = messages[-1]
        if isinstance(last, ModelRequest) and any(isinstance(p, ToolReturnPart) for p in last.parts):
            return ModelResponse(parts=[TextPart("done")])
        prompt = next(
            str(p.content)
            for m in messages
            if isinstance(m, ModelRequest)
            for p in m.parts
            if isinstance(p, UserPromptPart)
        )
        calls = plan(prompt)
        if not calls:
            return ModelResponse(parts=[TextPart("done")])
        return ModelResponse(parts=[ToolCallPart(name, args) for name, args in calls])

    return FunctionModel(model)


def writes(*files: tuple[str, str]) -> FunctionModel:
    """A worker that writes the given files and stops."""
    return scripted(lambda _prompt: [("write_file", {"path": p, "content": c}) for p, c in files])
