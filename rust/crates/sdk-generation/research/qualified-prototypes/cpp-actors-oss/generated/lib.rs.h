#pragma once
#include <array>
#include <cstddef>
#include <cstdint>
#include <new>
#include <string>
#include <type_traits>
#include <utility>
#if __cplusplus >= 201703L
#include <string_view>
#endif

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdollar-in-identifier-extension"
#endif // __clang__

namespace rust {
inline namespace cxxbridge1 {
// #include "rust/cxx.h"

struct unsafe_bitcopy_t;

namespace {
template <typename T>
class impl;
} // namespace

#ifndef CXXBRIDGE1_RUST_STRING
#define CXXBRIDGE1_RUST_STRING
class String final {
public:
  String() noexcept;
  String(const String &) noexcept;
  String(String &&) noexcept;
  ~String() noexcept;

  String(const std::string &);
  String(const char *);
  String(const char *, std::size_t);
  String(const char16_t *);
  String(const char16_t *, std::size_t);
#ifdef __cpp_char8_t
  String(const char8_t *s);
  String(const char8_t *s, std::size_t len);
#endif

  static String lossy(const std::string &) noexcept;
  static String lossy(const char *) noexcept;
  static String lossy(const char *, std::size_t) noexcept;
  static String lossy(const char16_t *) noexcept;
  static String lossy(const char16_t *, std::size_t) noexcept;

  String &operator=(const String &) & noexcept;
  String &operator=(String &&) & noexcept;

  explicit operator std::string() const;

  const char *data() const noexcept;
  std::size_t size() const noexcept;
  std::size_t length() const noexcept;
  bool empty() const noexcept;

  const char *c_str() noexcept;

  std::size_t capacity() const noexcept;
  void reserve(size_t new_cap) noexcept;

  using iterator = char *;
  iterator begin() noexcept;
  iterator end() noexcept;

  using const_iterator = const char *;
  const_iterator begin() const noexcept;
  const_iterator end() const noexcept;
  const_iterator cbegin() const noexcept;
  const_iterator cend() const noexcept;

  bool operator==(const String &) const noexcept;
  bool operator!=(const String &) const noexcept;
  bool operator<(const String &) const noexcept;
  bool operator<=(const String &) const noexcept;
  bool operator>(const String &) const noexcept;
  bool operator>=(const String &) const noexcept;

  void swap(String &) noexcept;

  String(unsafe_bitcopy_t, const String &) noexcept;

private:
  struct lossy_t;
  String(lossy_t, const char *, std::size_t) noexcept;
  String(lossy_t, const char16_t *, std::size_t) noexcept;
  friend void swap(String &lhs, String &rhs) noexcept { lhs.swap(rhs); }

  std::array<std::uintptr_t, 3> repr;
};
#endif // CXXBRIDGE1_RUST_STRING

#ifndef CXXBRIDGE1_RUST_STR
#define CXXBRIDGE1_RUST_STR
class Str final {
public:
  Str() noexcept;
  Str(const String &) noexcept;
  Str(const std::string &);
  Str(const char *);
  Str(const char *, std::size_t);

  Str &operator=(const Str &) & noexcept = default;

  explicit operator std::string() const;
#if __cplusplus >= 201703L
  explicit operator std::string_view() const;
#endif

  const char *data() const noexcept;
  std::size_t size() const noexcept;
  std::size_t length() const noexcept;
  bool empty() const noexcept;

  Str(const Str &) noexcept = default;
  ~Str() noexcept = default;

  using iterator = const char *;
  using const_iterator = const char *;
  const_iterator begin() const noexcept;
  const_iterator end() const noexcept;
  const_iterator cbegin() const noexcept;
  const_iterator cend() const noexcept;

  bool operator==(const Str &) const noexcept;
  bool operator!=(const Str &) const noexcept;
  bool operator<(const Str &) const noexcept;
  bool operator<=(const Str &) const noexcept;
  bool operator>(const Str &) const noexcept;
  bool operator>=(const Str &) const noexcept;

  void swap(Str &) noexcept;

private:
  class uninit;
  Str(uninit) noexcept;
  friend impl<Str>;

  std::array<std::uintptr_t, 2> repr;
};
#endif // CXXBRIDGE1_RUST_STR

#ifndef CXXBRIDGE1_RUST_BOX
#define CXXBRIDGE1_RUST_BOX
template <typename T>
class Box final {
public:
  using element_type = T;
  using const_pointer =
      typename std::add_pointer<typename std::add_const<T>::type>::type;
  using pointer = typename std::add_pointer<T>::type;

  Box() = delete;
  Box(Box &&) noexcept;
  ~Box() noexcept;

  explicit Box(const T &);
  explicit Box(T &&);

  Box &operator=(Box &&) & noexcept;

  const T *operator->() const noexcept;
  const T &operator*() const noexcept;
  T *operator->() noexcept;
  T &operator*() noexcept;

  template <typename... Fields>
  static Box in_place(Fields &&...);

  void swap(Box &) noexcept;

  static Box from_raw(T *) noexcept;

  T *into_raw() noexcept;

  /* Deprecated */ using value_type = element_type;

private:
  class uninit;
  class allocation;
  Box(uninit) noexcept;
  void drop() noexcept;

  friend void swap(Box &lhs, Box &rhs) noexcept { lhs.swap(rhs); }

  T *ptr;
};

template <typename T>
class Box<T>::uninit {};

template <typename T>
class Box<T>::allocation {
  static T *alloc() noexcept;
  static void dealloc(T *) noexcept;

public:
  allocation() noexcept : ptr(alloc()) {}
  ~allocation() noexcept {
    if (this->ptr) {
      dealloc(this->ptr);
    }
  }
  T *ptr;
};

template <typename T>
Box<T>::Box(Box &&other) noexcept : ptr(other.ptr) {
  other.ptr = nullptr;
}

template <typename T>
Box<T>::Box(const T &val) {
  allocation alloc;
  ::new (alloc.ptr) T(val);
  this->ptr = alloc.ptr;
  alloc.ptr = nullptr;
}

template <typename T>
Box<T>::Box(T &&val) {
  allocation alloc;
  ::new (alloc.ptr) T(std::move(val));
  this->ptr = alloc.ptr;
  alloc.ptr = nullptr;
}

template <typename T>
Box<T>::~Box() noexcept {
  if (this->ptr) {
    this->drop();
  }
}

template <typename T>
Box<T> &Box<T>::operator=(Box &&other) & noexcept {
  if (this->ptr) {
    this->drop();
  }
  this->ptr = other.ptr;
  other.ptr = nullptr;
  return *this;
}

template <typename T>
const T *Box<T>::operator->() const noexcept {
  return this->ptr;
}

template <typename T>
const T &Box<T>::operator*() const noexcept {
  return *this->ptr;
}

template <typename T>
T *Box<T>::operator->() noexcept {
  return this->ptr;
}

template <typename T>
T &Box<T>::operator*() noexcept {
  return *this->ptr;
}

template <typename T>
template <typename... Fields>
Box<T> Box<T>::in_place(Fields &&...fields) {
  allocation alloc;
  auto ptr = alloc.ptr;
  ::new (ptr) T{std::forward<Fields>(fields)...};
  alloc.ptr = nullptr;
  return from_raw(ptr);
}

template <typename T>
void Box<T>::swap(Box &rhs) noexcept {
  using std::swap;
  swap(this->ptr, rhs.ptr);
}

template <typename T>
Box<T> Box<T>::from_raw(T *raw) noexcept {
  Box box = uninit{};
  box.ptr = raw;
  return box;
}

template <typename T>
T *Box<T>::into_raw() noexcept {
  T *raw = this->ptr;
  this->ptr = nullptr;
  return raw;
}

template <typename T>
Box<T>::Box(uninit) noexcept {}
#endif // CXXBRIDGE1_RUST_BOX

#ifndef CXXBRIDGE1_RUST_OPAQUE
#define CXXBRIDGE1_RUST_OPAQUE
class Opaque {
public:
  Opaque() = delete;
  Opaque(const Opaque &) = delete;
  ~Opaque() = delete;
};
#endif // CXXBRIDGE1_RUST_OPAQUE

#ifndef CXXBRIDGE1_IS_COMPLETE
#define CXXBRIDGE1_IS_COMPLETE
namespace detail {
namespace {
template <typename T, typename = std::size_t>
struct is_complete : std::false_type {};
template <typename T>
struct is_complete<T, decltype(sizeof(T))> : std::true_type {};
} // namespace
} // namespace detail
#endif // CXXBRIDGE1_IS_COMPLETE

#ifndef CXXBRIDGE1_LAYOUT
#define CXXBRIDGE1_LAYOUT
class layout {
  template <typename T>
  friend std::size_t size_of();
  template <typename T>
  friend std::size_t align_of();
  template <typename T>
  static typename std::enable_if<std::is_base_of<Opaque, T>::value,
                                 std::size_t>::type
  do_size_of() {
    return T::layout::size();
  }
  template <typename T>
  static typename std::enable_if<!std::is_base_of<Opaque, T>::value,
                                 std::size_t>::type
  do_size_of() {
    return sizeof(T);
  }
  template <typename T>
  static
      typename std::enable_if<detail::is_complete<T>::value, std::size_t>::type
      size_of() {
    return do_size_of<T>();
  }
  template <typename T>
  static typename std::enable_if<std::is_base_of<Opaque, T>::value,
                                 std::size_t>::type
  do_align_of() {
    return T::layout::align();
  }
  template <typename T>
  static typename std::enable_if<!std::is_base_of<Opaque, T>::value,
                                 std::size_t>::type
  do_align_of() {
    return alignof(T);
  }
  template <typename T>
  static
      typename std::enable_if<detail::is_complete<T>::value, std::size_t>::type
      align_of() {
    return do_align_of<T>();
  }
};

template <typename T>
std::size_t size_of() {
  return layout::size_of<T>();
}

template <typename T>
std::size_t align_of() {
  return layout::align_of<T>();
}
#endif // CXXBRIDGE1_LAYOUT
} // namespace cxxbridge1
} // namespace rust

#if __cplusplus >= 201402L
#define CXX_DEFAULT_VALUE(value) = value
#else
#define CXX_DEFAULT_VALUE(value)
#endif

namespace acyclic {
  namespace actors {
    enum class ErrorKind : ::std::uint8_t;
    struct PositiveU64Result;
    struct ClientConnectResult;
    struct ActorOperationResult;
    struct RemoteConformanceResult;
    struct ActorsClient;
    struct ActorsError;
    struct ActorObservationView;
    struct PositiveU64View;
    struct ActorsOperation;
  }
}

namespace acyclic {
namespace actors {
#ifndef CXXBRIDGE1_ENUM_acyclic$actors$ErrorKind
#define CXXBRIDGE1_ENUM_acyclic$actors$ErrorKind
// Structured boundary error category; payload and validation remain Rust-owned.
enum class ErrorKind : ::std::uint8_t {
  NoError = 0,
  InvalidArgument = 1,
  LimitExceeded = 2,
  DuplicateName = 3,
  Cancelled = 4,
  Configuration = 5,
  Transport = 6,
  Service = 7,
  Semantic = 8,
};
#endif // CXXBRIDGE1_ENUM_acyclic$actors$ErrorKind

#ifndef CXXBRIDGE1_STRUCT_acyclic$actors$PositiveU64Result
#define CXXBRIDGE1_STRUCT_acyclic$actors$PositiveU64Result
struct PositiveU64Result final {
  bool ok CXX_DEFAULT_VALUE(false);
  ::std::uint64_t value CXX_DEFAULT_VALUE(0);
  ::acyclic::actors::ErrorKind error;

  using IsRelocatable = ::std::true_type;
};
#endif // CXXBRIDGE1_STRUCT_acyclic$actors$PositiveU64Result

#ifndef CXXBRIDGE1_STRUCT_acyclic$actors$ClientConnectResult
#define CXXBRIDGE1_STRUCT_acyclic$actors$ClientConnectResult
struct ClientConnectResult final {
  bool connected CXX_DEFAULT_VALUE(false);
  ::acyclic::actors::ErrorKind error;

  using IsRelocatable = ::std::true_type;
};
#endif // CXXBRIDGE1_STRUCT_acyclic$actors$ClientConnectResult

#ifndef CXXBRIDGE1_STRUCT_acyclic$actors$ActorOperationResult
#define CXXBRIDGE1_STRUCT_acyclic$actors$ActorOperationResult
// One typed result per operation. The Rust domain object remains opaque;
// these are validated projections, not a second request/response model.
struct ActorOperationResult final {
  bool ok CXX_DEFAULT_VALUE(false);
  ::acyclic::actors::ErrorKind error;
  ::rust::String message;
  ::rust::String actor_id;
  ::rust::String home_region;
  bool active CXX_DEFAULT_VALUE(false);
  bool subscriptions_empty CXX_DEFAULT_VALUE(false);
  ::std::uint64_t configuration_revision CXX_DEFAULT_VALUE(0);
  ::std::uint64_t checkpoint_epoch CXX_DEFAULT_VALUE(0);
  bool has_checkpoint CXX_DEFAULT_VALUE(false);
  ::std::uint64_t checkpoint CXX_DEFAULT_VALUE(0);
  ::std::uint32_t status CXX_DEFAULT_VALUE(0);
  bool has_location_header CXX_DEFAULT_VALUE(false);

  using IsRelocatable = ::std::true_type;
};
#endif // CXXBRIDGE1_STRUCT_acyclic$actors$ActorOperationResult

#ifndef CXXBRIDGE1_STRUCT_acyclic$actors$RemoteConformanceResult
#define CXXBRIDGE1_STRUCT_acyclic$actors$RemoteConformanceResult
struct RemoteConformanceResult final {
  bool ok CXX_DEFAULT_VALUE(false);
  ::std::uint32_t operations_completed CXX_DEFAULT_VALUE(0);
  bool authentication_rejected CXX_DEFAULT_VALUE(false);
  ::acyclic::actors::ErrorKind error;
  ::rust::String message;

  using IsRelocatable = ::std::true_type;
};
#endif // CXXBRIDGE1_STRUCT_acyclic$actors$RemoteConformanceResult

#ifndef CXXBRIDGE1_STRUCT_acyclic$actors$ActorsClient
#define CXXBRIDGE1_STRUCT_acyclic$actors$ActorsClient
struct ActorsClient final : public ::rust::Opaque {
  ~ActorsClient() = delete;

private:
  friend ::rust::layout;
  struct layout {
    static ::std::size_t size() noexcept;
    static ::std::size_t align() noexcept;
  };
};
#endif // CXXBRIDGE1_STRUCT_acyclic$actors$ActorsClient

#ifndef CXXBRIDGE1_STRUCT_acyclic$actors$ActorsError
#define CXXBRIDGE1_STRUCT_acyclic$actors$ActorsError
struct ActorsError final : public ::rust::Opaque {
  ~ActorsError() = delete;

private:
  friend ::rust::layout;
  struct layout {
    static ::std::size_t size() noexcept;
    static ::std::size_t align() noexcept;
  };
};
#endif // CXXBRIDGE1_STRUCT_acyclic$actors$ActorsError

#ifndef CXXBRIDGE1_STRUCT_acyclic$actors$ActorObservationView
#define CXXBRIDGE1_STRUCT_acyclic$actors$ActorObservationView
struct ActorObservationView final : public ::rust::Opaque {
  ~ActorObservationView() = delete;

private:
  friend ::rust::layout;
  struct layout {
    static ::std::size_t size() noexcept;
    static ::std::size_t align() noexcept;
  };
};
#endif // CXXBRIDGE1_STRUCT_acyclic$actors$ActorObservationView

#ifndef CXXBRIDGE1_STRUCT_acyclic$actors$PositiveU64View
#define CXXBRIDGE1_STRUCT_acyclic$actors$PositiveU64View
struct PositiveU64View final : public ::rust::Opaque {
  ~PositiveU64View() = delete;

private:
  friend ::rust::layout;
  struct layout {
    static ::std::size_t size() noexcept;
    static ::std::size_t align() noexcept;
  };
};
#endif // CXXBRIDGE1_STRUCT_acyclic$actors$PositiveU64View

#ifndef CXXBRIDGE1_STRUCT_acyclic$actors$ActorsOperation
#define CXXBRIDGE1_STRUCT_acyclic$actors$ActorsOperation
struct ActorsOperation final : public ::rust::Opaque {
  ~ActorsOperation() = delete;

private:
  friend ::rust::layout;
  struct layout {
    static ::std::size_t size() noexcept;
    static ::std::size_t align() noexcept;
  };
};
#endif // CXXBRIDGE1_STRUCT_acyclic$actors$ActorsOperation

::rust::Box<::acyclic::actors::ActorsClient> actors_client_new() noexcept;

::rust::Box<::acyclic::actors::ActorsClient> actors_client_connect(::rust::Str endpoint, ::rust::Str token, ::rust::Str ca_certificate) noexcept;

bool actors_client_is_connected(::acyclic::actors::ActorsClient const &client) noexcept;

::acyclic::actors::ErrorKind actors_client_error_kind(::acyclic::actors::ActorsClient const &client) noexcept;

::rust::String actors_client_error_message(::acyclic::actors::ActorsClient const &client) noexcept;

::acyclic::actors::ClientConnectResult actors_client_connect_probe(::rust::Str endpoint, ::rust::Str token) noexcept;

::acyclic::actors::ActorOperationResult actors_create_actor(::acyclic::actors::ActorsClient const &client) noexcept;

::acyclic::actors::ActorOperationResult actors_update_actor(::acyclic::actors::ActorsClient const &client) noexcept;

::acyclic::actors::ActorOperationResult actors_inspect_actor(::acyclic::actors::ActorsClient const &client) noexcept;

::acyclic::actors::ActorOperationResult actors_add_subscription(::acyclic::actors::ActorsClient const &client) noexcept;

::acyclic::actors::ActorOperationResult actors_remove_subscription(::acyclic::actors::ActorsClient const &client) noexcept;

::acyclic::actors::ActorOperationResult actors_resume_subscription(::acyclic::actors::ActorsClient const &client) noexcept;

::acyclic::actors::ActorOperationResult actors_checkpoint_actor(::acyclic::actors::ActorsClient const &client) noexcept;

::acyclic::actors::ActorOperationResult actors_invoke_actor(::acyclic::actors::ActorsClient const &client) noexcept;

::acyclic::actors::ActorOperationResult actors_inspect_actor_with_cancel(::acyclic::actors::ActorsClient const &client, ::acyclic::actors::ActorsOperation const &operation) noexcept;

::acyclic::actors::RemoteConformanceResult actors_live_conformance_probe(::rust::Str endpoint, ::rust::Str token, ::rust::Str ca_certificate) noexcept;

::rust::Box<::acyclic::actors::ActorsError> actors_cancelled_error() noexcept;

::acyclic::actors::ErrorKind actors_error_kind(::acyclic::actors::ActorsError const &error) noexcept;

::rust::String actors_error_message(::acyclic::actors::ActorsError const &error) noexcept;

::rust::Box<::acyclic::actors::ActorObservationView> actors_sample_observation() noexcept;

::std::uint64_t actor_observation_checkpoint_epoch(::acyclic::actors::ActorObservationView const &observation) noexcept;

bool actor_observation_has_checkpoint(::acyclic::actors::ActorObservationView const &observation) noexcept;

::std::uint64_t actor_observation_checkpoint(::acyclic::actors::ActorObservationView const &observation) noexcept;

::acyclic::actors::PositiveU64Result actors_validate_positive_u64(::std::uint64_t value) noexcept;

::std::uint64_t positive_u64_value(::acyclic::actors::PositiveU64View const &value) noexcept;

::rust::Box<::acyclic::actors::ActorsOperation> actors_operation_new() noexcept;

void actors_operation_cancel(::acyclic::actors::ActorsOperation const &operation) noexcept;

bool actors_operation_is_cancelled(::acyclic::actors::ActorsOperation const &operation) noexcept;
} // namespace actors
} // namespace acyclic

#ifdef __clang__
#pragma clang diagnostic pop
#endif // __clang__
