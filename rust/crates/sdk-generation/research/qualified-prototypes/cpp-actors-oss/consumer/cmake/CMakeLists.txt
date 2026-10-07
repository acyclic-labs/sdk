cmake_minimum_required(VERSION 3.21)
project(cpp_actors_external_consumer LANGUAGES CXX)

find_package(AcyclicActorsCXX CONFIG REQUIRED)
if(NOT ACYCLIC_ACTORS_CXX_CARGO_PACKAGE_VERSION OR
   NOT ACYCLIC_ACTORS_CXX_CARGO_SOURCE_SHA256 OR
   NOT ACYCLIC_ACTORS_CXX_GENERATED_HEADER_SHA256)
  message(FATAL_ERROR "CXX package did not expose its Cargo-derived identity")
endif()

add_executable(cpp-actors-external-positive "${CMAKE_CURRENT_LIST_DIR}/../positive.cc")
target_compile_features(cpp-actors-external-positive PRIVATE cxx_std_11)
target_link_libraries(cpp-actors-external-positive PRIVATE AcyclicActors::CXX)

# Compile and link the full operation consumer as part of the installed-package
# check. Runtime conformance supplies its endpoint, token, and CA separately.
add_executable(cpp-actors-external-live "${CMAKE_CURRENT_LIST_DIR}/../live-remote.cc")
target_compile_features(cpp-actors-external-live PRIVATE cxx_std_11)
target_link_libraries(cpp-actors-external-live PRIVATE AcyclicActors::CXX)
