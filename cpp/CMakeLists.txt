cmake_minimum_required(VERSION 3.26)

project(acyclic_sdk_cpp_transport LANGUAGES CXX)

if(NOT DEFINED ACYCLIC_CPP_GENERATED_DIR)
  message(FATAL_ERROR
    "Generated transport sources are required. Run cpp/Generate.ps1 with the "
    "pinned protoc 36.2 and grpc_cpp_plugin 1.80.0, then pass "
    "-DACYCLIC_CPP_GENERATED_DIR=<generated-directory>.")
endif()

set(ACYCLIC_CPP_RECEIPT "${ACYCLIC_CPP_GENERATED_DIR}/generation-receipt.json")
if(NOT EXISTS "${ACYCLIC_CPP_RECEIPT}")
  message(FATAL_ERROR "The generated directory must contain generation-receipt.json from cpp/Generate.ps1")
endif()
file(READ "${ACYCLIC_CPP_RECEIPT}" ACYCLIC_CPP_RECEIPT_JSON)
string(JSON ACYCLIC_CPP_PROTOBUF_PIN GET "${ACYCLIC_CPP_RECEIPT_JSON}" protobuf_pin)
string(JSON ACYCLIC_CPP_GRPC_PIN GET "${ACYCLIC_CPP_RECEIPT_JSON}" grpc_pin)
if(NOT ACYCLIC_CPP_PROTOBUF_PIN STREQUAL "36.2" OR NOT ACYCLIC_CPP_GRPC_PIN STREQUAL "1.80.0")
  message(FATAL_ERROR "generation-receipt.json does not prove the pinned Protobuf 36.2/gRPC 1.80.0 toolchain")
endif()

file(GLOB_RECURSE ACYCLIC_CPP_SOURCES CONFIGURE_DEPENDS
  "${ACYCLIC_CPP_GENERATED_DIR}/*.cc")
file(GLOB_RECURSE ACYCLIC_CPP_HEADERS CONFIGURE_DEPENDS
  "${ACYCLIC_CPP_GENERATED_DIR}/*.h")
if(NOT ACYCLIC_CPP_SOURCES OR NOT ACYCLIC_CPP_HEADERS)
  message(FATAL_ERROR "The generated directory must contain .cc and .h output from the pinned generators")
endif()

# Protobuf 36.2's upstream package metadata currently reports its OSS version
# as major 7, so asking find_package for version 36.2 rejects the matching
# runtime. Resolve the config package, then validate the installed runtime
# header against the generator pin recorded above.
find_package(Protobuf CONFIG REQUIRED)
find_path(ACYCLIC_CPP_PROTOBUF_RUNTIME_INCLUDE
  NAMES google/protobuf/runtime_version.h
  PATHS ${Protobuf_INCLUDE_DIRS} ${CMAKE_PREFIX_PATH}
  PATH_SUFFIXES include
  NO_DEFAULT_PATH)
if(NOT ACYCLIC_CPP_PROTOBUF_RUNTIME_INCLUDE)
  message(FATAL_ERROR "The configured Protobuf package does not expose runtime_version.h")
endif()
file(READ
  "${ACYCLIC_CPP_PROTOBUF_RUNTIME_INCLUDE}/google/protobuf/runtime_version.h"
  ACYCLIC_CPP_RUNTIME_VERSION_H)
string(REGEX MATCH
  "#define[ \t]+PROTOBUF_OSS_VERSION[ \t]+([0-9]+)"
  ACYCLIC_CPP_RUNTIME_VERSION_MATCH
  "${ACYCLIC_CPP_RUNTIME_VERSION_H}")
if(NOT CMAKE_MATCH_1 STREQUAL "7036002")
  message(FATAL_ERROR
    "Expected Protobuf OSS runtime 36.2 (7036002); found '${CMAKE_MATCH_1}'")
endif()
find_package(gRPC 1.80.0 CONFIG REQUIRED)

add_library(acyclic_sdk_cpp_transport STATIC ${ACYCLIC_CPP_SOURCES} ${ACYCLIC_CPP_HEADERS})
target_compile_features(acyclic_sdk_cpp_transport PUBLIC cxx_std_17)
target_include_directories(acyclic_sdk_cpp_transport PUBLIC "${ACYCLIC_CPP_GENERATED_DIR}")
target_link_libraries(acyclic_sdk_cpp_transport PUBLIC protobuf::libprotobuf gRPC::grpc++)

set_target_properties(acyclic_sdk_cpp_transport PROPERTIES EXPORT_NAME transport)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

install(TARGETS acyclic_sdk_cpp_transport
  EXPORT AcyclicSdkCppTargets
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
  INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(DIRECTORY "${ACYCLIC_CPP_GENERATED_DIR}/"
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/acyclic/cpp"
  FILES_MATCHING PATTERN "*.h")
install(EXPORT AcyclicSdkCppTargets
  FILE AcyclicSdkCppTargets.cmake
  NAMESPACE AcyclicSdkCpp::
  DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/AcyclicSdkCpp")

set(ACYCLIC_CPP_CONFIG "${CMAKE_CURRENT_BINARY_DIR}/AcyclicSdkCppConfig.cmake")
file(WRITE "${ACYCLIC_CPP_CONFIG}" [=[
include(CMakeFindDependencyMacro)
find_dependency(Protobuf CONFIG)
find_dependency(gRPC 1.80.0 CONFIG)
include("${CMAKE_CURRENT_LIST_DIR}/AcyclicSdkCppTargets.cmake")
]=])
install(FILES "${ACYCLIC_CPP_CONFIG}"
  DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/AcyclicSdkCpp")
