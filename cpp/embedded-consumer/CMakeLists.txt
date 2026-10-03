cmake_minimum_required(VERSION 3.26)

project(acyclic_cpp_embedded_consumer LANGUAGES CXX)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

set(ACYCLIC_EMBEDDED_ROOT "" CACHE PATH
  "Release output directory of sdk-embedded-prototype")
set(ACYCLIC_EMBEDDED_HEADER "" CACHE FILEPATH
  "Generated cbindgen header from sdk-embedded-prototype")

if(WIN32)
  set(ACYCLIC_RUNTIME_NAME "acyclic_sdk_embedded_prototype.dll")
  set(ACYCLIC_RUNTIME_INSTALL_DIR "${CMAKE_INSTALL_BINDIR}")
  set(ACYCLIC_IMPORT_NAME "acyclic_sdk_embedded_prototype.dll.lib")
elseif(APPLE)
  set(ACYCLIC_RUNTIME_NAME "libacyclic_sdk_embedded_prototype.dylib")
  set(ACYCLIC_RUNTIME_INSTALL_DIR "${CMAKE_INSTALL_LIBDIR}")
else()
  set(ACYCLIC_RUNTIME_NAME "libacyclic_sdk_embedded_prototype.so")
  set(ACYCLIC_RUNTIME_INSTALL_DIR "${CMAKE_INSTALL_LIBDIR}")
endif()

set(ACYCLIC_RUNTIME_PATH "${ACYCLIC_EMBEDDED_ROOT}/${ACYCLIC_RUNTIME_NAME}")
if(NOT EXISTS "${ACYCLIC_RUNTIME_PATH}")
  message(FATAL_ERROR
    "Set ACYCLIC_EMBEDDED_ROOT to sdk-embedded-prototype/target/release; "
    "missing ${ACYCLIC_RUNTIME_NAME}")
endif()
if(WIN32 AND NOT EXISTS "${ACYCLIC_EMBEDDED_ROOT}/${ACYCLIC_IMPORT_NAME}")
  message(FATAL_ERROR
    "Missing ${ACYCLIC_IMPORT_NAME} beside the Windows DLL")
endif()
if(NOT EXISTS "${ACYCLIC_EMBEDDED_HEADER}")
  message(FATAL_ERROR "Set ACYCLIC_EMBEDDED_HEADER to the generated cbindgen header")
endif()

add_library(acyclic_embedded_runtime SHARED IMPORTED GLOBAL)
set_target_properties(acyclic_embedded_runtime PROPERTIES
  IMPORTED_LOCATION "${ACYCLIC_RUNTIME_PATH}"
  INTERFACE_INCLUDE_DIRECTORIES "${CMAKE_CURRENT_BINARY_DIR}/include")
if(WIN32)
  set_property(TARGET acyclic_embedded_runtime APPEND PROPERTY
    IMPORTED_IMPLIB "${ACYCLIC_EMBEDDED_ROOT}/${ACYCLIC_IMPORT_NAME}")
endif()

file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/include")
configure_file("${ACYCLIC_EMBEDDED_HEADER}"
  "${CMAKE_CURRENT_BINARY_DIR}/include/acyclic_embedded_prototype.h" COPYONLY)

add_library(acyclic_sdk_cpp_embedded INTERFACE)
target_compile_features(acyclic_sdk_cpp_embedded INTERFACE cxx_std_17)
target_include_directories(acyclic_sdk_cpp_embedded INTERFACE
  "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
  "$<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}/include>")
target_link_libraries(acyclic_sdk_cpp_embedded INTERFACE acyclic_embedded_runtime)

add_executable(acyclic_cpp_embedded_consumer main.cpp)
target_compile_features(acyclic_cpp_embedded_consumer PRIVATE cxx_std_17)
target_link_libraries(acyclic_cpp_embedded_consumer PRIVATE acyclic_sdk_cpp_embedded)

add_executable(acyclic_cpp_embedded_negative negative_lifetime_smoke.cpp)
target_compile_features(acyclic_cpp_embedded_negative PRIVATE cxx_std_17)
target_include_directories(acyclic_cpp_embedded_negative PRIVATE
  "${CMAKE_CURRENT_BINARY_DIR}/include")
target_link_libraries(acyclic_cpp_embedded_negative PRIVATE acyclic_embedded_runtime)

add_executable(acyclic_cpp_embedded_cross_thread cross_thread_cancel_smoke.cpp)
target_compile_features(acyclic_cpp_embedded_cross_thread PRIVATE cxx_std_17)
target_include_directories(acyclic_cpp_embedded_cross_thread PRIVATE
  "${CMAKE_CURRENT_BINARY_DIR}/include")
target_link_libraries(acyclic_cpp_embedded_cross_thread PRIVATE acyclic_embedded_runtime)
if(NOT WIN32)
  find_package(Threads REQUIRED)
  target_link_libraries(acyclic_cpp_embedded_cross_thread PRIVATE Threads::Threads)
endif()

add_custom_command(TARGET acyclic_cpp_embedded_consumer POST_BUILD
  COMMAND ${CMAKE_COMMAND} -E copy_if_different
    "${ACYCLIC_RUNTIME_PATH}"
    "$<TARGET_FILE_DIR:acyclic_cpp_embedded_consumer>")
add_custom_command(TARGET acyclic_cpp_embedded_negative POST_BUILD
  COMMAND ${CMAKE_COMMAND} -E copy_if_different
    "${ACYCLIC_RUNTIME_PATH}"
    "$<TARGET_FILE_DIR:acyclic_cpp_embedded_negative>")
add_custom_command(TARGET acyclic_cpp_embedded_cross_thread POST_BUILD
  COMMAND ${CMAKE_COMMAND} -E copy_if_different
    "${ACYCLIC_RUNTIME_PATH}"
    "$<TARGET_FILE_DIR:acyclic_cpp_embedded_cross_thread>")

enable_testing()
add_test(NAME embedded_abi_cpp_smoke COMMAND acyclic_cpp_embedded_consumer)
add_test(NAME embedded_abi_cpp_lifetime_negative COMMAND acyclic_cpp_embedded_negative)
add_test(NAME embedded_abi_cpp_cross_thread_cancel COMMAND acyclic_cpp_embedded_cross_thread)

install(DIRECTORY include/ DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}")
install(FILES "${CMAKE_CURRENT_BINARY_DIR}/include/acyclic_embedded_prototype.h"
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}")
install(FILES "${ACYCLIC_RUNTIME_PATH}"
  DESTINATION "${ACYCLIC_RUNTIME_INSTALL_DIR}")
if(WIN32)
  install(FILES "${ACYCLIC_EMBEDDED_ROOT}/${ACYCLIC_IMPORT_NAME}"
    DESTINATION "${CMAKE_INSTALL_LIBDIR}")
endif()

configure_package_config_file(AcyclicEmbeddedConfig.cmake.in
  "${CMAKE_CURRENT_BINARY_DIR}/AcyclicEmbeddedConfig.cmake"
  INSTALL_DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/AcyclicEmbedded")
write_basic_package_version_file(
  "${CMAKE_CURRENT_BINARY_DIR}/AcyclicEmbeddedConfigVersion.cmake"
  VERSION 0.1.0
  COMPATIBILITY SameMajorVersion)
install(FILES
  "${CMAKE_CURRENT_BINARY_DIR}/AcyclicEmbeddedConfig.cmake"
  "${CMAKE_CURRENT_BINARY_DIR}/AcyclicEmbeddedConfigVersion.cmake"
  DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/AcyclicEmbedded")
