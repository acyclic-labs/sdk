#!/usr/bin/env python3
"""Render an ag-gRPC Common Lisp consumer from Rust-owned protobuf services."""
from __future__ import annotations

import re
import sys
from pathlib import Path


def lisp_name(value: str) -> str:
    value = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "-", value)
    value = re.sub(r"[^A-Za-z0-9]+", "-", value)
    return value.strip("-").lower()


def type_parts(value: str) -> tuple[str, str]:
    value = value.lstrip(".")
    parts = value.split(".")
    if len(parts) == 1:
        return "", lisp_name(parts[0])
    return ".".join(parts[:-1]).upper(), lisp_name(parts[-1])


def main() -> int:
    if len(sys.argv) < 4:
        print("usage: write-common-lisp-runtime-consumer.py GENERATED_ROOT PROTO... OUTPUT", file=sys.stderr)
        return 2
    output = Path(sys.argv[-1])
    proto_paths = [Path(path) for path in sys.argv[2:-1]]
    service_pattern = re.compile(r"\bservice\s+([A-Za-z_][A-Za-z0-9_]*)\s*\{")
    rpc_pattern = re.compile(
        r"\brpc\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(\s*(stream\s+)?([.A-Za-z_][.A-Za-z0-9_]*)\s*\)"
        r"\s*returns\s*\(\s*(stream\s+)?([.A-Za-z_][.A-Za-z0-9_]*)\s*\)",
        re.S,
    )
    calls = []
    for proto_path in sorted(proto_paths):
        source = proto_path.read_text(encoding="utf-8")
        package_match = re.search(r"\bpackage\s+([.A-Za-z_][A-Za-z0-9_]*)\s*;", source)
        package = package_match.group(1) if package_match else ""
        for service_match in service_pattern.finditer(source):
            depth = 1
            cursor = service_match.end()
            while depth and cursor < len(source):
                if source[cursor] == "{":
                    depth += 1
                elif source[cursor] == "}":
                    depth -= 1
                cursor += 1
            body = source[service_match.end() : cursor - 1]
            service = lisp_name(service_match.group(1))
            for rpc in rpc_pattern.finditer(body):
                method, client_stream, request, server_stream, response = rpc.groups()
                shape = "bidi" if client_stream and server_stream else (
                    "client-stream" if client_stream else "server-stream" if server_stream else "unary"
                )
                request_package, request_class = type_parts(request)
                response_package, response_class = type_parts(response)
                package_name = ".".join((package or request_package).split("."))
                calls.append((package_name.upper(), service, lisp_name(method), request_package.upper(), request_class, response_package.upper(), response_class, shape))
    if len(calls) != 106:
        raise SystemExit(f"Rust proto inventory produced {len(calls)} Common Lisp calls; expected 106")

    lines = [
        ";; Generated from the Rust authority protobuf products; do not edit.",
        "(defparameter *channel* nil)",
        "(defun required-function (package name)",
        "  (let ((symbol (intern (string-upcase name) (find-package package))))",
        "    (unless (fboundp symbol) (error \"generated client function ~A is missing\" symbol))",
        "    (symbol-function symbol)))",
        "",
        "(defun required-class (package name)",
        "  (let ((symbol (intern (string-upcase name) (find-package package))))",
        "    (unless (find-class symbol nil) (error \"generated protobuf class ~A is missing\" symbol))",
        "    symbol))",
        "",
        "(defun call-unary (stub package function request-package request-class)",
        "  (multiple-value-bind (_response status)",
        "      (funcall (required-function package function) stub (make-instance (required-class request-package request-class)))",
        "    (declare (ignore _response))",
        "    (unless (zerop status) (error \"unary RPC returned status ~A\" status))))",
        "",
        "(defun call-server-stream (stub package function request-package request-class)",
        "  (let ((stream (funcall (required-function package function) stub (make-instance (required-class request-package request-class)))))",
        "    (ag-grpc:collect-stream-messages stream :limit 1)",
        "    (unless (zerop (ag-grpc:stream-status stream)) (error \"server stream returned a nonzero status\"))))",
        "",
        "(defun call-client-stream (stub package function request-package request-class)",
        "  (let ((stream (funcall (required-function package function) stub)))",
        "    (ag-grpc:stream-send stream (make-instance (required-class request-package request-class)))",
        "    (multiple-value-bind (_response status) (ag-grpc:stream-close-and-recv stream)",
        "      (declare (ignore _response))",
        "      (unless (zerop status) (error \"client stream returned status ~A\" status)))))",
        "",
        "(defun call-bidi (stub package function request-package request-class)",
        "  (let ((stream (funcall (required-function package function) stub)))",
        "    (ag-grpc:stream-send stream (make-instance (required-class request-package request-class)))",
        "    (ag-grpc:stream-close-send stream)",
        "    (loop for response = (ag-grpc:stream-read-message stream) while response finally",
        "      (unless (zerop (ag-grpc:stream-status stream)) (error \"bidi stream returned a nonzero status\")))))",
        "",
        "(defun run ()",
        "  (let* ((endpoint (or (sb-ext:posix-getenv \"ACYCLIC_FIXTURE_GRPC_ENDPOINT\") \"127.0.0.1:18081\"))",
        "         (separator (position #\\: endpoint :from-end t))",
        "         (host (subseq endpoint 0 separator))",
        "         (port (parse-integer (subseq endpoint (1+ separator))))",
        "         (channel (ag-grpc:make-channel host port)))",
        "    (unwind-protect",
        "        (progn",
        "          (mapc #'run-call calls)",
        "          (format t \"Common Lisp generated stubs invoked all 106 Rust RPCs~%\"))",
        "      (ag-grpc:channel-close channel))))",
        "",
        "(defun run-call (call)",
        "  (destructuring-bind (package service method request-package request-class _response-package _response-class shape) call",
        "    (declare (ignore _response-package _response-class))",
        "    (let* ((stub (funcall (required-function package (format nil \"make-~A-stub\" service)) *channel*))",
        "           (function (format nil \"~A-~A~A\" service method (if (string= shape \"server-stream\") \"-stream\" \"\"))))",
        "      (case (intern (string-upcase shape) :keyword)",
        "        (:UNARY (call-unary stub package function (or request-package package) request-class))",
        "        (:SERVER-STREAM (call-server-stream stub package function (or request-package package) request-class))",
        "        (:CLIENT-STREAM (call-client-stream stub package function (or request-package package) request-class))",
        "        (:BIDI (call-bidi stub package function (or request-package package) request-class))))))",
        "",
        "(defparameter *calls* '(",
    ]
    for package, service, method, request_package, request_class, response_package, response_class, shape in calls:
        lines.append(f"  (\"{package}\" \"{service}\" \"{method}\" \"{request_package or package}\" \"{request_class}\" \"{response_package or package}\" \"{response_class}\" \"{shape}\")")
    lines.extend(["))", "", "(run)", ""])
    # run-call needs the active channel; bind it dynamically around the calls.
    text = "\n".join(lines).replace("(mapc #'run-call calls)", "(let ((*channel* channel)) (mapc #'run-call *calls*))")
    output.write_text(text, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
