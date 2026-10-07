cmake_minimum_required(VERSION 3.21)
project(cpp-actors-oss-qualification LANGUAGES CXX)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

set(CPP_ACTORS_CARGO_PACKAGE_VERSION "0.1.0" CACHE STRING
    "Version from the Rust Cargo package metadata")
set(CPP_ACTORS_GENERATED_HEADER_SHA256
    "F302DDDE6C4D2FE594B3C41EA45505C2EDCEA85DECD591EBD0AF45935BC5A713"
    CACHE STRING "SHA-256 of generated/lib.rs.h")
set(CPP_ACTORS_CARGO_SOURCE_SHA256
    "8C0802E0BA0273C3E701B6AA841DD31E26B83C08735EB886FB5F2FE534F709BA"
    CACHE STRING "SHA-256 of Cargo.toml source metadata")
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
install(FILES "${CPP_ACTORS_NATIVE_LIBRARY}"
        DESTINATION "${CMAKE_INSTALL_LIBDIR}"
        RENAME "${CPP_ACTORS_PACKAGE_LIBNAME}")
install(FILES
  "${CMAKE_CURRENT_BINARY_DIR}/AcyclicActorsCXXConfig.cmake"
  "${CMAKE_CURRENT_BINARY_DIR}/AcyclicActorsCXXConfigVersion.cmake"
  DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/AcyclicActorsCXX")
