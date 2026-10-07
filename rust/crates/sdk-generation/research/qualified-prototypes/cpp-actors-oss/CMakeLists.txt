cmake_minimum_required(VERSION 3.21)
project(cpp-actors-oss-qualification LANGUAGES CXX)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

find_program(CPP_ACTORS_CARGO_EXECUTABLE cargo REQUIRED)
set(CPP_ACTORS_CARGO_MANIFEST "${CMAKE_CURRENT_SOURCE_DIR}/Cargo.toml")
execute_process(
  COMMAND "${CPP_ACTORS_CARGO_EXECUTABLE}" metadata --no-deps --format-version 1
          --manifest-path "${CPP_ACTORS_CARGO_MANIFEST}"
  OUTPUT_VARIABLE CPP_ACTORS_CARGO_METADATA
  ERROR_VARIABLE CPP_ACTORS_CARGO_METADATA_ERROR
  RESULT_VARIABLE CPP_ACTORS_CARGO_METADATA_RESULT
  OUTPUT_STRIP_TRAILING_WHITESPACE)
if(NOT CPP_ACTORS_CARGO_METADATA_RESULT EQUAL 0)
  message(FATAL_ERROR
    "cargo metadata failed while deriving the CXX package identity: "
    "${CPP_ACTORS_CARGO_METADATA_ERROR}")
endif()
string(JSON CPP_ACTORS_CARGO_PACKAGE_COUNT LENGTH "${CPP_ACTORS_CARGO_METADATA}" packages)
set(CPP_ACTORS_CARGO_PACKAGE_VERSION "")
math(EXPR CPP_ACTORS_CARGO_PACKAGE_LAST "${CPP_ACTORS_CARGO_PACKAGE_COUNT} - 1")
foreach(CPP_ACTORS_CARGO_PACKAGE_INDEX RANGE 0 ${CPP_ACTORS_CARGO_PACKAGE_LAST})
  string(JSON CPP_ACTORS_CARGO_PACKAGE_NAME GET "${CPP_ACTORS_CARGO_METADATA}"
         packages ${CPP_ACTORS_CARGO_PACKAGE_INDEX} name)
  if(CPP_ACTORS_CARGO_PACKAGE_NAME STREQUAL "cpp-actors-oss-qualification")
    string(JSON CPP_ACTORS_CARGO_PACKAGE_VERSION GET "${CPP_ACTORS_CARGO_METADATA}"
           packages ${CPP_ACTORS_CARGO_PACKAGE_INDEX} version)
  endif()
endforeach()
if(CPP_ACTORS_CARGO_PACKAGE_VERSION STREQUAL "")
  message(FATAL_ERROR "cargo metadata did not contain cpp-actors-oss-qualification")
endif()
file(SHA256 "${CMAKE_CURRENT_SOURCE_DIR}/Cargo.toml" CPP_ACTORS_CARGO_SOURCE_SHA256)
file(SHA256 "${CMAKE_CURRENT_SOURCE_DIR}/generated/lib.rs.h"
     CPP_ACTORS_GENERATED_HEADER_SHA256)
message(STATUS "CXX Actors Cargo package version: ${CPP_ACTORS_CARGO_PACKAGE_VERSION}")
message(STATUS "CXX Actors Cargo source SHA-256: ${CPP_ACTORS_CARGO_SOURCE_SHA256}")
message(STATUS "CXX Actors generated header SHA-256: ${CPP_ACTORS_GENERATED_HEADER_SHA256}")
set(CPP_ACTORS_NATIVE_LIBRARY "" CACHE FILEPATH
    "Cargo-produced static library to install")
if(NOT CPP_ACTORS_NATIVE_LIBRARY)
  message(FATAL_ERROR "Set CPP_ACTORS_NATIVE_LIBRARY to the Cargo-produced static library")
endif()
if(NOT EXISTS "${CPP_ACTORS_NATIVE_LIBRARY}")
  message(FATAL_ERROR "CPP_ACTORS_NATIVE_LIBRARY does not exist: ${CPP_ACTORS_NATIVE_LIBRARY}")
endif()

set(CPP_ACTORS_PACKAGE_LIBNAME "cpp_actors_oss_qualification${CMAKE_STATIC_LIBRARY_SUFFIX}")
configure_package_config_file(
  cmake/AcyclicActorsCXXConfig.cmake.in
  "${CMAKE_CURRENT_BINARY_DIR}/AcyclicActorsCXXConfig.cmake"
  INSTALL_DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/AcyclicActorsCXX"
  NO_CHECK_REQUIRED_COMPONENTS_MACRO)
write_basic_package_version_file(
  "${CMAKE_CURRENT_BINARY_DIR}/AcyclicActorsCXXConfigVersion.cmake"
  VERSION "${CPP_ACTORS_CARGO_PACKAGE_VERSION}"
  COMPATIBILITY SameMajorVersion)

install(FILES generated/lib.rs.h DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}")
install(FILES generated/rust/cxx.h
        DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/rust")
install(FILES "${CPP_ACTORS_NATIVE_LIBRARY}"
        DESTINATION "${CMAKE_INSTALL_LIBDIR}"
        RENAME "${CPP_ACTORS_PACKAGE_LIBNAME}")
install(FILES
  "${CMAKE_CURRENT_BINARY_DIR}/AcyclicActorsCXXConfig.cmake"
  "${CMAKE_CURRENT_BINARY_DIR}/AcyclicActorsCXXConfigVersion.cmake"
  DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/AcyclicActorsCXX")
