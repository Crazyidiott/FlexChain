PROJECT(flexchain)
CMAKE_MINIMUM_REQUIRED (VERSION 3.0.0)
SET(CMAKE_CXX_FLAGS "-g -Wall -std=c++17")

find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)
find_package(leveldb REQUIRED)

find_library(VERBS_LIB NAMES ibverbs libverbs HINTS /usr/lib /usr/local/lib)
if(NOT VERBS_LIB)
    message(FATAL_ERROR "libverbs library not found")
endif()

ADD_SUBDIRECTORY(proto)

INCLUDE_DIRECTORIES(${CMAKE_CURRENT_BINARY_DIR})
INCLUDE_DIRECTORIES(${Protobuf_INCLUDE_DIRS})

add_library(benchmark OBJECT benchmark.cc)
add_library(setup_ib OBJECT setup_ib.cc)
add_library(utils OBJECT utils.cc)
add_library(statistics OBJECT statistics.cc)


add_executable(compute_server compute_server.cc $<TARGET_OBJECTS:benchmark> $<TARGET_OBJECTS:setup_ib> $<TARGET_OBJECTS:utils> $<TARGET_OBJECTS:statistics>)
add_executable(memory_server memory_server.cc $<TARGET_OBJECTS:setup_ib> $<TARGET_OBJECTS:utils>)
add_executable(storage_server storage_server.cc)
add_executable(orderer orderer.cc $<TARGET_OBJECTS:utils>)

foreach(_target
compute_server memory_server storage_server orderer)
  target_link_libraries(${_target} PRIVATE
    proto-objects
    absl::check
    absl::flags
    absl::flags_parse
    absl::log
    leveldb::leveldb
    gRPC::grpc++
    gRPC::grpc++_reflection
    protobuf::libprotobuf
    ${VERBS_LIB})
endforeach()
