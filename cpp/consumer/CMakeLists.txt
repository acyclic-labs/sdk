cmake_minimum_required(VERSION 3.26)
project(acyclic_sdk_cpp_consumer LANGUAGES CXX)

if(NOT DEFINED ACYCLIC_GENERATED_ROOT)
  message(FATAL_ERROR "ACYCLIC_GENERATED_ROOT must point at the source-bound generated C++ tree")
endif()
if(NOT EXISTS "${ACYCLIC_GENERATED_ROOT}/rust-authority.json")
  message(FATAL_ERROR "The generated C++ tree must carry rust-authority.json from the Rust wire exporter")
endif()

find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)

file(GLOB_RECURSE ACYCLIC_GENERATED_SOURCES CONFIGURE_DEPENDS
  "${ACYCLIC_GENERATED_ROOT}/*.cc")
file(GLOB_RECURSE ACYCLIC_GENERATED_HEADERS CONFIGURE_DEPENDS
  "${ACYCLIC_GENERATED_ROOT}/*.h")

add_executable(acyclic_sdk_cpp_consumer
  consumer.cpp
  ${ACYCLIC_GENERATED_SOURCES}
  ${ACYCLIC_GENERATED_HEADERS})
target_compile_features(acyclic_sdk_cpp_consumer PRIVATE cxx_std_17)
target_include_directories(acyclic_sdk_cpp_consumer PRIVATE "${ACYCLIC_GENERATED_ROOT}")
target_link_libraries(acyclic_sdk_cpp_consumer PRIVATE protobuf::libprotobuf gRPC::grpc++ gRPC::crypto)

add_executable(acyclic_sdk_cpp_envelope_tests negative-envelope-tests.cpp)
target_compile_features(acyclic_sdk_cpp_envelope_tests PRIVATE cxx_std_17)
target_link_libraries(acyclic_sdk_cpp_envelope_tests PRIVATE protobuf::libprotobuf gRPC::grpc++ gRPC::crypto)

enable_testing()
add_test(NAME acyclic_sdk_cpp_consumer COMMAND acyclic_sdk_cpp_consumer)
add_test(NAME acyclic_sdk_cpp_envelope_tests COMMAND acyclic_sdk_cpp_envelope_tests)
