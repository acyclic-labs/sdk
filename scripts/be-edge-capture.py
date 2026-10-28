"""Capture actual credential-free llama.cpp HTTP output on a Linux host, without rewriting it."""
import hashlib
import json
import pathlib
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone


def main():
    endpoint, destination, rate_endpoint = sys.argv[1:]
    root = pathlib.Path(destination)
    tool = {"type": "function", "function": {"name": "echo", "description": "Echo a value",
            "parameters": {"type": "object", "properties": {"value": {"type": "integer"}},
                           "required": ["value"], "additionalProperties": False}}}
    base = {"model": "conformance-tiny", "stream": True, "stream_options": {"include_usage": True},
            "max_completion_tokens": 128, "temperature": 0, "seed": 1,
            "chat_template_kwargs": {"enable_thinking": False}}
    cases = {
        "text": {"messages": [{"role": "user", "content": "Reply with exactly: hello"}]},
        "tool": {"messages": [{"role": "user", "content": "Call echo with value 1."}],
                 "tools": [tool]},
        "result": {"messages": [{"role": "user", "content": "Call echo with value 1."},
                                {"role": "assistant", "content": None, "tool_calls": [
                                    {"id": "capture_echo", "type": "function", "function": {
                                        "name": "echo", "arguments": '{"value":1}'}}]},
                                {"role": "tool", "tool_call_id": "capture_echo", "content": '{"value":1}'}],
                   "tools": [tool]},
        "context": {"messages": [{"role": "user", "content": "overflow " * 3000}]},
        "auth": {"messages": [{"role": "user", "content": "hello"}]},
        "rate": {"messages": [{"role": "user", "content": "hello"}]},
    }
    captures = {}
    usage = {}
    for name, case in cases.items():
        request = dict(base, **case)
        request_bytes = json.dumps(request).encode()
        (root / f"{name}.request.json").write_bytes(request_bytes)
        headers = {"content-type": "application/json"}
        if name != "auth":
            headers["authorization"] = "Bearer conformance-public-test-key"
        req = urllib.request.Request(rate_endpoint if name == "rate" else endpoint + "/v1/chat/completions",
                                     data=request_bytes, headers=headers)
        try:
            response = urllib.request.urlopen(req, timeout=30)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            body = response.read()
            status = response.status
            (root / f"{name}.headers.txt").write_bytes(str(response.headers).encode())
        (root / f"{name}.body").write_bytes(body)
        captures[name] = {"status": status, "file": f"{name}.body",
                          "sha256": hashlib.sha256(body).hexdigest()}
        if name != "rate":
            captures[name]["request_url"] = req.full_url
        if name == "result":
            captures[name]["assertions"] = {"/messages": request["messages"]}
        print(f"CAPTURE {name} status={status} bytes={len(body)} sha256={captures[name]['sha256']}")
        if name in ("text", "tool", "result"):
            events = [json.loads(line[6:]) for line in body.decode().splitlines()
                      if line.startswith("data: ") and line != "data: [DONE]"]
            counter = next(event["usage"] for event in events if event.get("usage"))
            usage[name] = {"input": "/usage/prompt_tokens", "input_tokens": counter["prompt_tokens"],
                           "output": "/usage/completion_tokens", "output_tokens": counter["completion_tokens"]}
        if name == "tool":
            events = [json.loads(line[6:]) for line in body.decode().splitlines()
                      if line.startswith("data: ") and line != "data: [DONE]"]
            calls = [call for event in events for choice in event.get("choices", [])
                     for call in choice.get("delta", {}).get("tool_calls", [])]
            call_id = next(call["id"] for call in calls if call.get("id"))
            cases["result"]["messages"][1]["tool_calls"][0]["id"] = call_id
            cases["result"]["messages"][2]["tool_call_id"] = call_id
    manifest = {"captured_at": datetime.now(timezone.utc).isoformat(), "source": "bb2 llama.cpp",
                "model": "conformance-tiny", "adapter": "chat", "captures": captures,
                "provider_id": "llamacpp", "endpoint": {
                    "adapter": "chat", "authentication": "optional", "base_url": endpoint + "/v1",
                    "max_event_bytes": 1024 * 1024, "models": {"conformance-tiny": {
                        "capacity": {"context_tokens": 2048, "output_tokens": 128},
                        "features": {"tools": True, "images": False}}}},
                "options": {"temperature": 0, "seed": 1, "chat_template_kwargs": {"enable_thinking": False}},
                "usage": usage["text"], "tool_usage": [usage["tool"], usage["result"]],
                "context_input": cases["context"]["messages"][0]["content"],
                "rate_source": "real nginx limit_req configured at 1 request/minute, limit_req_status 429"}
    (root / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"STEP_FAILED capture: {error}; recovery: bash scripts/be-edge-local.sh",
              file=sys.stderr)
        sys.exit(1)
