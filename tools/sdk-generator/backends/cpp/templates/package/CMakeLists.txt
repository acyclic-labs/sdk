cmake_minimum_required(VERSION 3.20)
project(AcyclicTransport VERSION 0.2.0 LANGUAGES CXX)
include(GNUInstallDirs)
include(CMakePackageConfigHelpers)
find_package(Protobuf 28.3 EXACT CONFIG REQUIRED)
find_package(gRPC 1.82.0 EXACT CONFIG REQUIRED)
file(GLOB_RECURSE TRANSPORT_SOURCES CONFIGURE_DEPENDS "src/*.pb.cc")
add_library(acyclic_sdk_transport STATIC ${TRANSPORT_SOURCES})
add_library(Acyclic::Transport ALIAS acyclic_sdk_transport)
set_target_properties(acyclic_sdk_transport PROPERTIES EXPORT_NAME Transport)
target_compile_features(acyclic_sdk_transport PUBLIC cxx_std_17)
target_include_directories(acyclic_sdk_transport PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
target_link_libraries(acyclic_sdk_transport PUBLIC protobuf::libprotobuf gRPC::grpc++)
install(TARGETS acyclic_sdk_transport EXPORT AcyclicTransportTargets
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(FILES LICENSE NOTICE DESTINATION ${CMAKE_INSTALL_DATADIR}/acyclic-sdk-transport)
install(DIRECTORY authority/ DESTINATION ${CMAKE_INSTALL_DATADIR}/acyclic-sdk-transport/authority)
configure_package_config_file(AcyclicTransportConfig.cmake.in
  "${CMAKE_CURRENT_BINARY_DIR}/AcyclicTransportConfig.cmake"
  INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/AcyclicTransport)
write_basic_package_version_file("${CMAKE_CURRENT_BINARY_DIR}/AcyclicTransportConfigVersion.cmake"
  VERSION ${PROJECT_VERSION} COMPATIBILITY SameMajorVersion)
install(EXPORT AcyclicTransportTargets NAMESPACE Acyclic::
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/AcyclicTransport)
install(FILES "${CMAKE_CURRENT_BINARY_DIR}/AcyclicTransportConfig.cmake"
  "${CMAKE_CURRENT_BINARY_DIR}/AcyclicTransportConfigVersion.cmake"
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/AcyclicTransport)
