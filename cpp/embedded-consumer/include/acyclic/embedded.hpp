#pragma once

#include <stddef.h>
#include <stdint.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdlib.h>

// cbindgen currently emits a C header. Keep the import C-compatible while
// allowing the generated C23 typedef branch to parse as C++.
#define __STDC_VERSION__ 202311L
extern "C" {
#include "acyclic_embedded_prototype.h"
}
#undef __STDC_VERSION__

namespace acyclic {

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

 private:
  AcyclicNextResult result_;
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
  NextResult next() const { return NextResult(acyclic_embedded_reader_next(result_.reader)); }
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
  uint64_t raw_handle_for_testing() const { return id_; }
  AppendResult append(const uint8_t* path, size_t path_len,
                      const uint8_t* value, size_t value_len) const {
    return AppendResult(acyclic_embedded_engine_append(
        id_, path, path_len, value, value_len));
  }
  Reader read(const uint8_t* path, size_t path_len, uint64_t from,
              uint32_t limit) const {
    return Reader(acyclic_embedded_reader_open(id_, path, path_len, from, limit, 0));
  }

 private:
  uint64_t id_;
};

}  // namespace acyclic
