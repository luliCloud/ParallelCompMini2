# ============================================================================
# Server CMakeLists.txt - Mini2 gRPC Server
# ============================================================================

# Proto Message and Service Code Generation
# Generate protobuf files
get_filename_component(mini2_proto "../proto/mini2.proto" ABSOLUTE)
get_filename_component(mini2_proto_path "${mini2_proto}" PATH)

message(STATUS "Proto file: ${mini2_proto}")
message(STATUS "Proto path: ${mini2_proto_path}")

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
    DEPENDS "${mini2_proto}"
    COMMENT "Generating gRPC code from proto"
)

# ============================================================================
# Common Library
# ============================================================================

file(GLOB common_srcs "${CMAKE_SOURCE_DIR}/common/src/*.cpp")

add_library(mini2_common STATIC ${common_srcs})
target_include_directories(mini2_common PUBLIC
    ${CMAKE_SOURCE_DIR}/common/include
)
target_link_libraries(mini2_common PUBLIC Threads::Threads)
if(OpenMP_FOUND)
    target_link_libraries(mini2_common PUBLIC OpenMP::OpenMP_CXX)
endif()

message(STATUS "Common library sources: ${common_srcs}")

# ============================================================================
# gRPC Proto Library
# ============================================================================

add_library(mini2_grpc_proto STATIC
    ${mini2_proto_srcs}
    ${mini2_proto_hdrs}
    ${mini2_grpc_srcs}
    ${mini2_grpc_hdrs}
)

target_link_libraries(mini2_grpc_proto
    PUBLIC
    ${_REFLECTION}
    ${_GRPC_GRPCPP}
    ${_PROTOBUF_LIBPROTOBUF}
)

target_include_directories(mini2_grpc_proto PUBLIC
    ${CMAKE_CURRENT_BINARY_DIR}
)

# ============================================================================
# Server Executable
# ============================================================================

add_executable(server 
    server.cpp
    ForwardResponseCache.cc
    RequestJobQueue.cc
    Mini2ServiceImpl.cc
)

target_include_directories(server PRIVATE
    ${CMAKE_CURRENT_BINARY_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/common/include
)

target_compile_definitions(server PRIVATE
    MINI2_SOURCE_DIR="${CMAKE_SOURCE_DIR}"
)

target_link_libraries(server
    PRIVATE
    mini2_grpc_proto
    mini2_common
    ${_REFLECTION}
    ${_GRPC_GRPCPP}
    ${_PROTOBUF_LIBPROTOBUF}
    yaml-cpp::yaml-cpp
    Threads::Threads
)

if(OpenMP_FOUND)
    target_link_libraries(server PRIVATE OpenMP::OpenMP_CXX)
endif()

message(STATUS "Server executable: ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/server")
