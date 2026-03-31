cmake_minimum_required(VERSION 3.25)
project(mini2)

# ============================================================================
# Platform-specific paths (macOS Homebrew)
# ============================================================================
if(APPLE)
    # Auto-detect Homebrew installation paths
    execute_process(
        COMMAND brew --prefix yaml-cpp
        OUTPUT_VARIABLE HOMEBREW_YAML_CPP
        OUTPUT_STRIP_TRAILING_WHITESPACE
    )
    execute_process(
        COMMAND brew --prefix grpc
        OUTPUT_VARIABLE HOMEBREW_GRPC
        OUTPUT_STRIP_TRAILING_WHITESPACE
    )
    
    if(HOMEBREW_YAML_CPP)
        list(APPEND CMAKE_PREFIX_PATH "${HOMEBREW_YAML_CPP}")
        message(STATUS "Found Homebrew yaml-cpp: ${HOMEBREW_YAML_CPP}")
    endif()
    
    if(HOMEBREW_GRPC)
        list(APPEND CMAKE_PREFIX_PATH "${HOMEBREW_GRPC}")
        message(STATUS "Found Homebrew gRPC: ${HOMEBREW_GRPC}")
    endif()
endif()

# C++ Standard
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra")

# Build type
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE) # Release is -O3. Debug is -O0 -g. RelWithDebInfo is -O2 -g.
endif()
message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")

# Organize output files
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)

# Find required packages
find_package(Threads REQUIRED)

# Protobuf
find_package(Protobuf CONFIG REQUIRED)
message(STATUS "Using protobuf ${Protobuf_VERSION}")
set(_PROTOBUF_LIBPROTOBUF protobuf::libprotobuf)
set(_REFLECTION gRPC::grpc++_reflection)

if(CMAKE_CROSSCOMPILING)
    find_program(_PROTOBUF_PROTOC protoc)
else()
    set(_PROTOBUF_PROTOC $<TARGET_FILE:protobuf::protoc>)
endif()

# gRPC
find_package(gRPC CONFIG REQUIRED)
message(STATUS "Using gRPC ${gRPC_VERSION}")
set(_GRPC_GRPCPP gRPC::grpc++)
if(CMAKE_CROSSCOMPILING)
    find_program(_GRPC_CPP_PLUGIN_EXECUTABLE grpc_cpp_plugin)
else()
    set(_GRPC_CPP_PLUGIN_EXECUTABLE $<TARGET_FILE:gRPC::grpc_cpp_plugin>)
endif()

# yaml-cpp
find_package(yaml-cpp REQUIRED)

# OpenMP (optional)
find_package(OpenMP)

# Subdirectories
add_subdirectory(server_cpp)

# Display configuration summary
message(STATUS "========================================")
message(STATUS "Mini2 gRPC Project Configuration")
message(STATUS "========================================")
message(STATUS "Protobuf version: ${Protobuf_VERSION}")
message(STATUS "gRPC version: ${gRPC_VERSION}")
message(STATUS "C++ Standard: ${CMAKE_CXX_STANDARD}")
message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")
if(OpenMP_FOUND)
  message(STATUS "OpenMP: Enabled")
else()
  message(STATUS "OpenMP: Disabled")
endif()
message(STATUS "Output directory: ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}")
message(STATUS "========================================")
