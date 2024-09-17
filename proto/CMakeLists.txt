set(PROTO_DIR "${CMAKE_CURRENT_LIST_DIR}")
set(PROTO_BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}")

file(GLOB PROTO_FILES "${PROTO_DIR}/*.proto")

add_library(proto-objects OBJECT)

target_link_libraries(proto-objects PUBLIC protobuf::libprotobuf gRPC::grpc++)

target_include_directories(proto-objects PUBLIC "$<BUILD_INTERFACE:${PROTO_BINARY_DIR}>")

protobuf_generate(
    TARGET proto-objects
    IMPORT_DIRS "${PROTO_DIR}"
    PROTOC_OUT_DIR "${PROTO_BINARY_DIR}"
    PROTOS ${PROTO_FILES}
)

protobuf_generate(
    TARGET proto-objects
    LANGUAGE grpc
    GENERATE_EXTENSIONS .grpc.pb.h .grpc.pb.cc
    PLUGIN "protoc-gen-grpc=\$<TARGET_FILE:gRPC::grpc_cpp_plugin>"
    IMPORT_DIRS "${PROTO_DIR}"
    PROTOC_OUT_DIR "${PROTO_BINARY_DIR}"
    PROTOS ${PROTO_FILES}
)
