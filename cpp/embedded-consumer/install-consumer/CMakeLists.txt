cmake_minimum_required(VERSION 3.26)
project(acyclic_cpp_installed_consumer LANGUAGES CXX)

find_package(AcyclicEmbedded 0.1 CONFIG REQUIRED)
add_executable(acyclic_cpp_installed_consumer main.cpp)
target_compile_features(acyclic_cpp_installed_consumer PRIVATE cxx_std_17)
target_link_libraries(acyclic_cpp_installed_consumer PRIVATE Acyclic::Embedded)

