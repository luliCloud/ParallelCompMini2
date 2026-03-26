cmake_minimum_required(VERSION 4.0)
project(mini2_server)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find required packages
find_package(Threads REQUIRED)

# Protobuf
find_package(Protobuf CONFIG REQUIRED)
option(protobuf_MODULE_COMPATIBLE TRUE)
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

# Include directories
include_directories(${CMAKE_CURRENT_BINARY_DIR})
include_directories(${CMAKE_SOURCE_DIR}/../common/include)
include_directories(${CMAKE_SOURCE_DIR}/../proto)

# Generate protobuf files
get_filename_component(mini2_proto "../proto/mini2.proto" ABSOLUTE)
get_filename_component(mini2_proto_path "${mini2_proto}" PATH)

set(mini2_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/mini2.pb.cc")
set(mini2_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/mini2.pb.h")
set(mini2_grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/mini2.grpc.pb.cc")
set(mini2_grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/mini2.grpc.pb.h")

add_custom_command(
    OUTPUT "${mini2_proto_srcs}" "${mini2_proto_hdrs}" "${mini2_grpc_srcs}" "${mini2_grpc_hdrs}"
    COMMAND ${_PROTOBUF_PROTOC}
    ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
         --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
         -I "${mini2_proto_path}"
         --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
         "${mini2_proto}"
    DEPENDS "${mini2_proto}")

# Include generated .pb.h files
include_directories("${CMAKE_CURRENT_BINARY_DIR}")

# Add executable
add_executable(server server.cpp
    ${mini2_proto_srcs}
    ${mini2_grpc_srcs})

# Link libraries
target_link_libraries(server
    ${_REFLECTION}
    ${_GRPC_GRPCPP}
    ${_PROTOBUF_LIBPROTOBUF}
    yaml-cpp
    Threads::Threads)
