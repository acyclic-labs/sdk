cmake_minimum_required(VERSION 3.26)
project(acyclic_sdk_cpp_consumer LANGUAGES CXX)

if(NOT DEFINED ACYCLIC_GENERATED_ROOT)
  message(FATAL_ERROR "ACYCLIC_GENERATED_ROOT must point at the source-bound generated C++ tree")
endif()

find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)

file(GLOB_RECURSE ACYCLIC_GENERATED_SOURCES CONFIGURE_DEPENDS
  "${ACYCLIC_GENERATED_ROOT}/*.cc")
file(GLOB_RECURSE ACYCLIC_GENERATED_HEADERS CONFIGURE_DEPENDS
  "${ACYCLIC_GENERATED_ROOT}/*.h")

add_library(acyclic_sdk_cpp_generated STATIC
  ${ACYCLIC_GENERATED_SOURCES}
  ${ACYCLIC_GENERATED_HEADERS})
target_compile_features(acyclic_sdk_cpp_generated PUBLIC cxx_std_17)
target_include_directories(acyclic_sdk_cpp_generated PUBLIC "${ACYCLIC_GENERATED_ROOT}")
target_link_libraries(acyclic_sdk_cpp_generated PUBLIC protobuf::libprotobuf gRPC::grpc++)

add_executable(acyclic_sdk_cpp_consumer consumer.cpp)
target_compile_features(acyclic_sdk_cpp_consumer PRIVATE cxx_std_17)
target_link_libraries(acyclic_sdk_cpp_consumer PRIVATE acyclic_sdk_cpp_generated)

enable_testing()
add_test(NAME acyclic_sdk_cpp_consumer COMMAND acyclic_sdk_cpp_consumer)
