get_filename_component(mini2_proto "../proto/mini2.proto" ABSOLUTE)
get_filename_component(mini2_proto_path "${mini2_proto}" PATH)

set(client_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/mini2.pb.cc")
set(client_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/mini2.pb.h")
set(client_grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/mini2.grpc.pb.cc")
set(client_grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/mini2.grpc.pb.h")

add_custom_command(
    OUTPUT "${client_proto_srcs}" "${client_proto_hdrs}" "${client_grpc_srcs}" "${client_grpc_hdrs}"
    COMMAND ${_PROTOBUF_PROTOC}
    ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
         --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
         -I "${mini2_proto_path}"
         --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
         "${mini2_proto}"
    DEPENDS "${mini2_proto}"
    COMMENT "Generating client gRPC code from proto"
)

add_library(mini2_client_proto STATIC
    ${client_proto_srcs}
    ${client_proto_hdrs}
    ${client_grpc_srcs}
    ${client_grpc_hdrs}
)

target_link_libraries(mini2_client_proto
    PUBLIC
    ${_REFLECTION}
    ${_GRPC_GRPCPP}
    ${_PROTOBUF_LIBPROTOBUF}
)

target_include_directories(mini2_client_proto PUBLIC
    ${CMAKE_CURRENT_BINARY_DIR}
)

add_executable(client
    client.cpp
)

target_include_directories(client PRIVATE
    ${CMAKE_CURRENT_BINARY_DIR}
)

target_link_libraries(client PRIVATE
    mini2_client_proto
    ${_REFLECTION}
    ${_GRPC_GRPCPP}
    ${_PROTOBUF_LIBPROTOBUF}
)
