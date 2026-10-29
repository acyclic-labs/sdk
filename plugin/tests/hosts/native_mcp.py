"""Initialized, event-driven MCP client for native POSIX process conformance."""
import json
import os
import selectors
import time


class McpFailure(RuntimeError):
    def __init__(self, step, detail, recovery):
        super().__init__(f"step={step}: {detail}; recovery={recovery}")


class McpClient:
    protocol = "2025-06-18"

    def __init__(self, process, recovery):
        self.process = process
        self.recovery = recovery
        self.sequence = 0
        self.pending = b""
        self.responses = {}
        self.ready = selectors.DefaultSelector()
        try:
            try:
                self.ready.register(process.stdout, selectors.EVENT_READ)
            except (OSError, ValueError) as error:
                raise McpFailure("observe MCP output", str(error), recovery) from error
            self.send({"jsonrpc": "2.0", "id": 0, "method": "initialize", "params": {
                "protocolVersion": self.protocol, "capabilities": {},
                "clientInfo": {"name": "native-session-conformance", "version": "1"}}})
            result = self.response(0)
            if result.get("protocolVersion") != self.protocol:
                raise McpFailure("initialize MCP", "unsupported negotiated version", recovery)
            self.send({"jsonrpc": "2.0", "method": "notifications/initialized"})
        except BaseException:
            self.close()
            raise

    def send(self, message):
        try:
            self.process.stdin.write((json.dumps(message) + "\n").encode())
            self.process.stdin.flush()
        except (OSError, ValueError) as error:
            raise McpFailure("write MCP request", str(error), self.recovery) from error

    def submit(self, name, reference, prompt=None):
        self.sequence += 1
        arguments = {"reference": reference}
        if prompt is not None:
            arguments["prompt"] = prompt
        self.send({"jsonrpc": "2.0", "id": self.sequence, "method": "tools/call",
                   "params": {"name": name, "arguments": arguments}})
        return self.sequence

    def response(self, identifier):
        deadline = time.monotonic() + 3
        while identifier not in self.responses:
            if b"\n" not in self.pending:
                remaining = deadline - time.monotonic()
                if remaining <= 0 or not self.ready.select(remaining):
                    raise McpFailure("read MCP response", "response deadline expired", self.recovery)
                try:
                    chunk = os.read(self.process.stdout.fileno(), 65536)
                except OSError as error:
                    raise McpFailure("read MCP output", str(error), self.recovery) from error
                if not chunk:
                    raise McpFailure("read MCP response", "producer closed before response", self.recovery)
                self.pending += chunk
            while b"\n" in self.pending:
                line, self.pending = self.pending.split(b"\n", 1)
                try:
                    message = json.loads(line)
                except ValueError as error:
                    raise McpFailure("decode MCP response", str(error), self.recovery) from error
                if not isinstance(message, dict):
                    raise McpFailure("decode MCP response", "expected an object", self.recovery)
                if "id" in message:
                    self.responses[message["id"]] = message
        message = self.responses.pop(identifier)
        if "error" in message:
            raise McpFailure("complete MCP request", str(message["error"]), self.recovery)
        if "result" not in message:
            raise McpFailure("complete MCP request", "response has no result", self.recovery)
        return message["result"]

    def result(self, identifier):
        result = self.response(identifier)
        if result.get("isError"):
            raise McpFailure("complete MCP tool", str(result), self.recovery)
        return json.loads(result["content"][0]["text"])

    def call(self, name, reference, prompt=None):
        return self.result(self.submit(name, reference, prompt))

    def close(self):
        self.ready.close()
