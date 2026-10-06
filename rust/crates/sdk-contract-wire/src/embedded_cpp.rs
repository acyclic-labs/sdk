//! Rust-owned C++ projection for the embedded C ABI.
//!
//! This is deliberately a lifetime adapter. The generated C header remains
//! the ABI authority, while this wrapper owns handles and returned buffers and
//! exposes copied diagnostics without reimplementing provider behavior.

/// Installable generated C++ source for the embedded ABI adapter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EmbeddedCppOutput {
    pub path: &'static str,
    pub source: String,
}

pub const CPP_EMBEDDED_PATH: &str = "cpp/embedded-consumer/include/acyclic/embedded.hpp";

/// Emit the C++ RAII adapter for the Rust-owned embedded C ABI.
pub fn generate_embedded_cpp() -> EmbeddedCppOutput {
    EmbeddedCppOutput {
        path: CPP_EMBEDDED_PATH,
        source: cpp_source(),
    }
}

fn cpp_source() -> String {
    String::from(
        r#"#pragma once

#include <stddef.h>
#include <stdint.h>
#include <string>

#include "acyclic_embedded_prototype.h"

namespace acyclic {

namespace detail {

inline std::string CopyDiagnostic(const AcyclicBuffer& buffer) {
  if (buffer.ptr == nullptr || buffer.len == 0) {
    return {};
  }
  return std::string(reinterpret_cast<const char*>(buffer.ptr), buffer.len);
}

}  // namespace detail

class AppendResult {
 public:
  explicit AppendResult(AcyclicAppendResult result) : result_(result) {}
  ~AppendResult() { acyclic_append_result_release(result_); }
  AppendResult(const AppendResult&) = delete;
  AppendResult& operator=(const AppendResult&) = delete;
  AppendResult(AppendResult&& other) noexcept : result_(other.result_) {
    other.result_.message = AcyclicBuffer{};
  }
  AppendResult& operator=(AppendResult&& other) noexcept {
    if (this != &other) {
      acyclic_append_result_release(result_);
      result_ = other.result_;
      other.result_.message = AcyclicBuffer{};
    }
    return *this;
  }

  AcyclicStatus status() const { return result_.status; }
  uint64_t start() const { return result_.start; }
  uint64_t end() const { return result_.end; }
  uint64_t tail() const { return result_.tail; }
  std::string message() const { return detail::CopyDiagnostic(result_.message); }

 private:
  AcyclicAppendResult result_{};
};

class NextResult {
 public:
  explicit NextResult(AcyclicNextResult result) : result_(result) {}
  ~NextResult() { acyclic_next_result_release(result_); }
  NextResult(const NextResult&) = delete;
  NextResult& operator=(const NextResult&) = delete;
  NextResult(NextResult&& other) noexcept : result_(other.result_) {
    other.result_.value = AcyclicBuffer{};
    other.result_.message = AcyclicBuffer{};
  }
  NextResult& operator=(NextResult&& other) noexcept {
    if (this != &other) {
      acyclic_next_result_release(result_);
      result_ = other.result_;
      other.result_.value = AcyclicBuffer{};
      other.result_.message = AcyclicBuffer{};
    }
    return *this;
  }

  AcyclicStatus status() const { return result_.status; }
  uint64_t sequence() const { return result_.sequence; }
  const uint8_t* data() const { return result_.value.ptr; }
  size_t size() const { return result_.value.len; }
  std::string message() const { return detail::CopyDiagnostic(result_.message); }

 private:
  AcyclicNextResult result_{};
};

class WireResult {
 public:
  explicit WireResult(AcyclicWireResult result) : result_(result) {}
  ~WireResult() { acyclic_wire_result_release(result_); }
  WireResult(const WireResult&) = delete;
  WireResult& operator=(const WireResult&) = delete;
  WireResult(WireResult&& other) noexcept : result_(other.result_) {
    other.result_.response = AcyclicBuffer{};
    other.result_.message = AcyclicBuffer{};
  }
  WireResult& operator=(WireResult&& other) noexcept {
    if (this != &other) {
      acyclic_wire_result_release(result_);
      result_ = other.result_;
      other.result_.response = AcyclicBuffer{};
      other.result_.message = AcyclicBuffer{};
    }
    return *this;
  }

  AcyclicStatus status() const { return result_.status; }
  const uint8_t* data() const { return result_.response.ptr; }
  size_t size() const { return result_.response.len; }
  std::string message() const { return detail::CopyDiagnostic(result_.message); }

 private:
  AcyclicWireResult result_{};
};

class Reader {
 public:
  Reader() = default;
  explicit Reader(AcyclicOpenResult result) : result_(result) {}
  ~Reader() { acyclic_open_result_release(result_); }
  Reader(const Reader&) = delete;
  Reader& operator=(const Reader&) = delete;
  Reader(Reader&& other) noexcept : result_(other.result_) {
    other.result_.reader = 0;
    other.result_.message = AcyclicBuffer{};
  }
  Reader& operator=(Reader&& other) noexcept {
    if (this != &other) {
      acyclic_open_result_release(result_);
      result_ = other.result_;
      other.result_.reader = 0;
      other.result_.message = AcyclicBuffer{};
    }
    return *this;
  }

  bool valid() const { return result_.reader != 0; }
  AcyclicStatus status() const { return result_.status; }
  std::string message() const { return detail::CopyDiagnostic(result_.message); }
  NextResult next() const {
    return NextResult(acyclic_embedded_reader_next(result_.reader));
  }
  void cancel() const { acyclic_embedded_reader_cancel(result_.reader); }

 private:
  AcyclicOpenResult result_{InvalidArgument, 0, AcyclicBuffer{}};
};

class Engine {
 public:
  static Engine open() { return Engine(acyclic_embedded_engine_open()); }
  explicit Engine(uint64_t id) : id_(id) {}
  ~Engine() { acyclic_embedded_engine_close(id_); }
  Engine(const Engine&) = delete;
  Engine& operator=(const Engine&) = delete;
  Engine(Engine&& other) noexcept : id_(other.id_) { other.id_ = 0; }
  Engine& operator=(Engine&& other) noexcept {
    if (this != &other) {
      acyclic_embedded_engine_close(id_);
      id_ = other.id_;
      other.id_ = 0;
    }
    return *this;
  }

  bool valid() const { return id_ != 0; }
  AppendResult append(const uint8_t* path, size_t path_len,
                      const uint8_t* value, size_t value_len) const {
    return AppendResult(acyclic_embedded_engine_append(
        id_, path, path_len, value, value_len));
  }
  Reader read(const uint8_t* path, size_t path_len, uint64_t from,
              uint32_t limit) const {
    return Reader(acyclic_embedded_reader_open(
        id_, path, path_len, from, limit, 0));
  }
  Reader follow(const uint8_t* path, size_t path_len, uint64_t from) const {
    return Reader(acyclic_embedded_reader_open(
        id_, path, path_len, from, 0, 1));
  }
  WireResult wire_call(const uint8_t* operation, size_t operation_len,
                       const uint8_t* request, size_t request_len) const {
    return WireResult(acyclic_embedded_engine_wire_call(
        id_, operation, operation_len, request, request_len));
  }

 private:
  uint64_t id_ = 0;
};

}  // namespace acyclic
"#,
    )
}

#[cfg(test)]
mod tests {
    use super::{CPP_EMBEDDED_PATH, generate_embedded_cpp};

    #[test]
    fn embedded_cpp_is_rust_owned_and_preserves_raii_ownership() {
        let output = generate_embedded_cpp();
        assert_eq!(output.path, CPP_EMBEDDED_PATH);
        assert!(output
            .source
            .starts_with("#pragma once\n\n#include <stddef.h>"));
        for marker in [
            "class AppendResult",
            "class NextResult",
            "class WireResult",
            "class Reader",
            "class Engine",
            "acyclic_append_result_release",
            "acyclic_next_result_release",
            "acyclic_wire_result_release",
            "acyclic_open_result_release",
            "acyclic_open_result_take_reader",
            "std::string message() const",
        ] {
            assert!(output.source.contains(marker), "missing {marker}");
        }
        assert!(output.source.contains("AppendResult(const AppendResult&) = delete;"));
        assert!(output.source.contains("NextResult(const NextResult&) = delete;"));
        assert!(output.source.contains("WireResult(const WireResult&) = delete;"));
        assert!(output.source.contains("Reader(const Reader&) = delete;"));
        assert!(output.source.contains("Engine(const Engine&) = delete;"));
        assert!(!output.source.contains("extern \"C\""));
    }

    #[test]
    fn embedded_cpp_diagnostics_copy_before_result_release() {
        let source = generate_embedded_cpp().source;
        assert!(source.contains("return std::string(reinterpret_cast<const char*>(buffer.ptr), buffer.len);"));
        assert!(source.contains("class WireResult"));
        assert!(source.contains("wire_call"));
    }
}
