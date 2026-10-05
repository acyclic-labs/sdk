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
    parts = value.lstrip(".").split(".")
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
        r"\brpc\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(\s*(stream\s+)?([.A-Za-z_][A-Za-z0-9_]*)\s*\)"
        r"\s*returns\s*\(\s*(stream\s+)?([.A-Za-z_][A-Za-z0-9_]*)\s*\)", re.S)
    calls = []
    for proto_path in sorted(proto_paths):
        source = proto_path.read_text(encoding="utf-8")
        package_match = re.search(r"\bpackage\s+([.A-Za-z_][A-Za-z0-9_]*)\s*;", source)
        package = package_match.group(1) if package_match else ""
        for service_match in service_pattern.finditer(source):
            depth = 1
            cursor = service_match.end()
            while depth and cursor < len(source):
                if source[cursor] == "{": depth += 1
                elif source[cursor] == "}": depth -= 1
                cursor += 1
            body = source[service_match.end():cursor - 1]
            service = lisp_name(service_match.group(1))
            for rpc in rpc_pattern.finditer(body):
                method, client_stream, request, server_stream, response = rpc.groups()
                shape = "bidi" if client_stream and server_stream else ("client-stream" if client_stream else "server-stream" if server_stream else "unary")
                request_package, request_class = type_parts(request)
                response_package, response_class = type_parts(response)
                calls.append((package.upper(), service, lisp_name(method), request_package.upper(), request_class, response_package.upper(), response_class, shape, f"{package}.{service_match.group(1)}/{method}"))

    lines = [
        ";; Generated from the Rust authority protobuf products; do not edit.",
        "(defparameter *channel* nil)",
        "(defparameter *observations* nil)",
        "(defparameter *observation-index* 0)",
        "(defun required-function (package name)",
        """  (let ((symbol (intern (string-upcase name) (find-package package))))
    (unless (fboundp symbol) (error \"generated client function ~A is missing\" symbol))
    (symbol-function symbol)))""",
        "(defun required-class (package name)",
        """  (let ((symbol (intern (string-upcase name) (find-package package))))
    (unless (find-class symbol nil) (error \"generated protobuf class ~A is missing\" symbol))
    symbol))""",
        "(defun message-bytes (message)",
        """  (unless message (error \"Rust fixture returned no protobuf message\"))
	  (ag-proto:serialize-to-bytes message))""",
        "(defun observation (rpc shape request responses status execution &optional error request-frames)",
        """  (let* ((request-bytes (message-bytes request))
         (request-messages (or request-frames (list request)))
         (request-frame-bytes (mapcar #'message-bytes request-messages))
         (response-bytes (mapcar #'message-bytes responses))
         (index (incf *observation-index*))
         (directory (or (sb-ext:posix-getenv \"ACYCLIC_RUNTIME_OBSERVATION_DIR\") \"runtime-observations\")))
    (ensure-directories-exist (merge-pathnames \"x\" (pathname (format nil \"~A/\" directory))))
     (loop for bytes in request-frame-bytes for frame from 0 do
       (with-open-file (stream (if (zerop frame) (format nil \"~A/~3,'0D.request.bin\" directory index) (format nil \"~A/~3,'0D.request.~D.bin\" directory index frame)) :direction :output :if-exists :supersede :element-type '(unsigned-byte 8)) (write-sequence bytes stream)))
    (loop for bytes in response-bytes for frame from 0 do
      (with-open-file (stream (format nil \"~A/~3,'0D.response.~D.bin\" directory index frame) :direction :output :if-exists :supersede :element-type '(unsigned-byte 8)) (write-sequence bytes stream)))
    (with-open-file (stream (format nil \"~A/~3,'0D.meta.sexp\" directory index) :direction :output :if-exists :supersede)
      (format stream \"~S\" (list :rpc rpc :shape shape :status status :execution execution :error (and error (princ-to-string error)) :request-frame-count (length request-frame-bytes))))
    (push (list :rpc rpc :shape shape :status status :execution execution :error (and error (princ-to-string error)) :request-file (format nil \"~3,'0D.request.bin\" index) :response-files (loop for frame from 0 below (length response-bytes) collect (format nil \"~3,'0D.response.~D.bin\" index frame))) *observations*)))""",
        "(defun base64-value (character)",
        "  (cond ((and (char>= character #\\A) (char<= character #\\Z)) (- (char-code character) (char-code #\\A))) ((and (char>= character #\\a) (char<= character #\\z)) (+ 26 (- (char-code character) (char-code #\\a)))) ((and (char>= character #\\0) (char<= character #\\9)) (+ 52 (- (char-code character) (char-code #\\0)))) ((char= character #\\+) 62) ((char= character #\\/) 63) (t nil))).",
        "(defun decode-base64 (value)",
        "  (let ((out (make-array 0 :element-type '(unsigned-byte 8) :adjustable t :fill-pointer 0)) (acc 0) (bits 0)) (loop for character across value unless (char= character #\\=) do (let ((part (base64-value character))) (when part (setf acc (logior (ash acc 6) part) bits (+ bits 6)) (loop while (>= bits 8) do (vector-push-extend (ldb (byte 8 (- bits 8)) acc) out) (setf bits (- bits 8) acc (logand acc (1- (ash 1 bits))))))) out)).",
        "(defun stream-scenario-frames (rpc request-class fallback)",
        "  (let ((path (sb-ext:posix-getenv \"ACYCLIC_RUST_STREAM_REQUEST_FRAMES\"))) (if (or (null path) (string= path \"\")) (list fallback) (or (loop for line in (uiop:read-file-lines path) for parts = (uiop:split-string (string-trim '(#\\Space #\\Tab #\\Return) line) :separator (string #\\Tab) :max 2) when (and (= (length parts) 2) (string= (first parts) rpc)) return (mapcar (lambda (encoded) (ag-proto:deserialize-from-bytes request-class (decode-base64 encoded))) (uiop:split-string (second parts) :separator \",\"))) (error \"missing Rust streaming scenario for ~A\" rpc)))).",
        "(defun send-client-frames (stream messages)",
        "  (mapcar (lambda (message) (ag-grpc:stream-send stream message) message) messages)).",
        "(defun call-unary (stub package function request-package request-class response-package response-class rpc)",
        """  (let ((request (make-instance (required-class request-package request-class))))
    (multiple-value-bind (response status) (funcall (required-function package function) stub request)
      (observation rpc \"unary\" request (if (zerop status) (list response) nil) status \"executed\" (unless (zerop status) status)))))""",
        "(defun call-server-stream (stub package function request-package request-class response-package response-class rpc)",
        """  (let ((request (make-instance (required-class request-package request-class))))
    (if (not (member rpc *stream-allow-list* :test #'string=))
        (observation rpc \"server-stream\" request nil nil \"deferred-rust-scenario\")
        (let ((stream (funcall (required-function package function) stub request)))
          (let ((responses (ag-grpc:collect-stream-messages stream)))
            (observation rpc \"server-stream\" request responses (ag-grpc:stream-status stream) \"executed\"))))))""",
        "(defun call-client-stream (stub package function request-package request-class response-package response-class rpc)",
        """  (let ((request (make-instance (required-class request-package request-class))))
    (if (not (member rpc *stream-allow-list* :test #'string=))
        (observation rpc \"client-stream\" request nil nil \"deferred-rust-scenario\")
        (let ((stream (funcall (required-function package function) stub)))
           (let ((requests (stream-scenario-frames rpc (required-class request-package request-class) request)))
             (send-client-frames stream requests)
          (multiple-value-bind (response status) (ag-grpc:stream-close-and-recv stream)
            (observation rpc \"client-stream\" (first requests) (if (zerop status) (list response) nil) status \"executed\" (unless (zerop status) status) requests))))))""",
        "(defun call-bidi (stub package function request-package request-class response-package response-class rpc)",
        """  (let ((request (make-instance (required-class request-package request-class))))
    (if (not (member rpc *stream-allow-list* :test #'string=))
        (observation rpc \"bidi\" request nil nil \"deferred-rust-scenario\")
        (let ((stream (funcall (required-function package function) stub)))
           (let ((requests (stream-scenario-frames rpc (required-class request-package request-class) request)))
             (send-client-frames stream requests)
          (ag-grpc:stream-close-send stream)
          (let ((responses (loop for response = (ag-grpc:stream-read-message stream) while response collect response)))
             (observation rpc \"bidi\" (first requests) responses (ag-grpc:stream-status stream) \"executed\" nil requests))))))""",
        "(defun run-call (call)",
        """  (destructuring-bind (package service method request-package request-class response-package response-class shape rpc) call
    (let* ((stub (funcall (required-function package (format nil \"make-~A-stub\" service)) *channel*))
           (function (format nil \"~A-~A~A\" service method (if (string= shape \"server-stream\") \"-stream\" \"\"))))
      (case (intern (string-upcase shape) :keyword)
        (:UNARY (call-unary stub package function (or request-package package) request-class (or response-package package) response-class rpc))
        (:SERVER-STREAM (call-server-stream stub package function (or request-package package) request-class (or response-package package) response-class rpc))
        (:CLIENT-STREAM (call-client-stream stub package function (or request-package package) request-class (or response-package package) response-class rpc))
        (:BIDI (call-bidi stub package function (or request-package package) request-class (or response-package package) response-class rpc))))))""",
        """(defun write-receipt ()
  (let ((source (or (sb-ext:posix-getenv \"ACYCLIC_RUST_SOURCE_REVISION\") \"unknown\"))
        (manifest (or (sb-ext:posix-getenv \"ACYCLIC_RUST_AUTHORITY_MANIFEST_SHA256\") \"unknown\"))
        (path (or (sb-ext:posix-getenv \"ACYCLIC_RUNTIME_RECEIPT\") \"runtime-consumer-receipt.sexp\")))
    (with-open-file (stream path :direction :output :if-exists :supersede)
      (format stream \"(:schema :acyclic.runtime-consumer-receipt.v1 :language :common-lisp :source-revision ~S :rust-authority-manifest-sha256 ~S :observations ~S)~%\" source manifest (nreverse *observations*)))))""",
        "(defparameter *stream-allow-list* (let ((value (or (sb-ext:posix-getenv \"ACYCLIC_STREAM_SCENARIO_RPCS\") \"\"))) (if (string= value \"\") nil (uiop:split-string value :separator \",\"))))",
        "(defparameter *calls* '(",
    ]
    for package, service, method, request_package, request_class, response_package, response_class, shape, rpc in calls:
        lines.append(f"  (\"{package}\" \"{service}\" \"{method}\" \"{request_package or package}\" \"{request_class}\" \"{response_package or package}\" \"{response_class}\" \"{shape}\" \"{rpc}\")")
    lines.extend([
        "))",
        """(let* ((endpoint (or (sb-ext:posix-getenv \"ACYCLIC_FIXTURE_GRPC_ENDPOINT\") \"127.0.0.1:18081\"))
       (separator (position #\\: endpoint :from-end t))
       (host (subseq endpoint 0 separator))
       (port (parse-integer (subseq endpoint (1+ separator))))
       (channel (ag-grpc:make-channel host port)))
  (unwind-protect
      (let ((*channel* channel))
        (mapc #'run-call *calls*)
        (write-receipt)
        (format t \"Common Lisp generated stubs recorded ~D Rust RPC observations~%\" (length *observations*)))
    (ag-grpc:channel-close channel)))""",
    ])
    output.write_text("\n".join(lines), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
