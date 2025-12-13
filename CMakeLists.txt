cmake_minimum_required(VERSION 3.31)
project(pg_anonymous)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include(FetchContent)

FetchContent_Declare(
  yaml-cpp
  GIT_REPOSITORY https://github.com/jbeder/yaml-cpp.git
  GIT_TAG 0.8.0
)
FetchContent_MakeAvailable(yaml-cpp)

find_package(OpenSSL REQUIRED)

# Add executable
add_executable(pg_anonymous src/main.cpp)

add_subdirectory(src)

target_include_directories(pg_anonymous PRIVATE include)

# Link libraries
target_link_libraries(pg_anonymous PRIVATE OpenSSL::Crypto yaml-cpp::yaml-cpp)

# Installation
install(TARGETS pg_anonymous DESTINATION bin)