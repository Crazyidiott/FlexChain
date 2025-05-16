#include <grpc/grpc.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>

#include <cassert>
#include <filesystem>
#include <iostream>
#include <fstream>
#include <string>

//for test
#include <iomanip>
#include <sstream>

#include "leveldb/db.h"
#include "leveldb/write_batch.h"
#include "log.h"
#include "storage.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

leveldb::DB* db;
leveldb::Options options;
pthread_mutex_t logger_lock;

class KVStableImpl final : public KVStable::Service {
   // 这是一个继承自 KVStable::Service 的最终类，不能被进一步继承
   // 实现了 KVStable 服务的具体功能，这个服务是通过 gRPC 定义的
    public:
    explicit KVStableImpl() : block_store("blockchain.log", std::ios::out | std::ios::binary) {}
    // 构造函数，初始化 block_store 文件流，用于将区块链数据存储到名为"blockchain.log"的文件中
    // 以二进制和输出模式打开文件
    Status write_sstables(ServerContext* context, const EvictedBuffers* request, EvictionResponse* response) override {
        // 覆盖(override)基类的方法，用于将被驱逐的缓冲数据写入SSTable存储
        // ServerContext是gRPC的上下文，request包含要写入的数据，response是返回的响应
        leveldb::WriteBatch batch;
        // 创建一个WriteBatch对象，用于批量写入操作，提高性能
        for (auto it = request->eviction().begin(); it != request->eviction().end(); it++) {
            // 遍历所有需要驱逐的键值对

            batch.Put(it->first, it->second);
            // 将键值对添加到WriteBatch中

            std::string actual_value;
            unsigned long offset = sizeof(uint64_t) * 2 + sizeof(uint8_t) + sizeof(uint32_t) + it->first.length();
            // 计算值的实际数据的偏移量，跳过元数据部分
            // 元数据包括：两个uint64_t(版本号)、一个uint8_t(标志位)、一个uint32_t(键长度)、键本身

            actual_value = it->second.substr(offset);
            // 从完整值中提取实际值部分，去除元数据

            log_info(stderr, "write[key = %s]: complete value = %s", it->first.c_str(), it->second.c_str());
            
            std::stringstream hex_stream;
            hex_stream << "0x";
            for (size_t i = 0; i < std::min(it->second.size(), size_t(30)); ++i) {
                hex_stream << std::hex << std::setw(2) << std::setfill('0') 
                        << static_cast<int>(static_cast<unsigned char>(it->second[i]));
                if ((i + 1) % 4 == 0) hex_stream << " "; // 每4个字节一个空格，增强可读性
            }
            log_info(stderr, "write[key = %s]: binary value (first 30 bytes): %s%s", 
                    it->first.c_str(), hex_stream.str().c_str(), 
                    it->second.size() > 30 ? "..." : "");

            log_debug(stderr, "write[key = %s]: value = %s is add to the batch.", it->first.c_str(), actual_value.c_str());
            // 记录调试信息，显示添加到批处理的键和值
        }

        leveldb::Status s = db->Write(leveldb::WriteOptions(), &batch);
        // 执行批量写入操作，将所有键值对写入LevelDB
        if (!s.ok()) {
            log_err("error %s occurred in writing the batch.", s.ToString().c_str());
            // 如果写入失败，记录错误信息
        }
        return Status::OK;
        // 返回成功状态
    }

    Status read_sstables(ServerContext* context, const GetRequest* request, GetResponse* response) override {
        // 从SSTable存储中读取数据的方法

        std::string value;
        leveldb::Status s = db->Get(leveldb::ReadOptions(), request->key(), &value);
        // 从LevelDB中读取指定键的值

        if (s.ok()) {
            // 如果读取成功
            response->set_value(value);
            // 设置响应中的值
            response->set_status(GetResponse_Status_FOUND);
            // 设置响应状态为"找到"

            std::string actual_value;
            unsigned long offset = sizeof(uint64_t) * 2 + sizeof(uint8_t) + sizeof(uint32_t) + request->key().length();
            // 计算值的实际部分的偏移量，跳过元数据

            // log_info(stderr, "read[key = %s]: complete value = %s", request->key().c_str(), value.c_str());

            actual_value = value.substr(offset);
            // 从完整值中提取实际值

            // log_info(stderr, "read[key = %s]: found value = %s.", request->key().c_str(), actual_value.c_str());
            // 记录信息，显示找到的键和值
        }
        if (s.IsNotFound()) {
            // 如果键不存在
            response->set_status(GetResponse_Status_NOTFOUND);
            // 设置响应状态为"未找到"

            log_info(stderr, "read[key = %s]: key not found in leveldb.", request->key().c_str());
            // 记录未找到的键信息
        }
        if ((!s.ok()) && (!s.IsNotFound())) {
            // 如果发生其他错误
            response->set_status(GetResponse_Status_ERROR);
            log_err("error '%s' occurred in reading key = %s.", s.ToString().c_str(), request->key().c_str());
        }

        return Status::OK;
    }

    Status write_blocks(ServerContext* context, const SerialisedBlock* request, google::protobuf::Empty* response) override {
        std::string serialised_block;
        request->SerializeToString(&serialised_block);
        uint32_t size = serialised_block.size();
        block_store.write((char *)&size, sizeof(uint32_t));
        block_store.write(serialised_block.c_str(), size);
        block_store.flush();

        return Status::OK;
    }
    private:
     std::ofstream block_store;
};

void run_server(const std::string& db_name, const std::string& server_address) {
    std::filesystem::remove_all(db_name);

    options.create_if_missing = true;
    options.error_if_exists = true;
    leveldb::Status status = leveldb::DB::Open(options, db_name, &db);
    assert(status.ok());

    KVStableImpl service;
    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    log_info(stderr, "Server listening on %s", server_address.c_str());
    server->Wait();
}

int main(int argc, char* argv[]) {
    int opt;
    std::string db_name = "../mydata/testdb";
    std::string server_address = "0.0.0.0:50051";

    while ((opt = getopt(argc, argv, "ha:d:")) != -1) {
        switch (opt) {
            case 'h':
                fprintf(stderr, "storage server usage:\n");
                fprintf(stderr, "\t-h: print this help message\n");
                fprintf(stderr, "\t-a <server_ip:server_port>: the listening addr of grpc server\n");
                fprintf(stderr, "\t-d <directory>: the directory for leveldb\n");
                exit(0);
            case 'a':
                server_address = std::string(optarg);
                break;
            case 'd':
                db_name = std::string(optarg);
                break;
            default:
                fprintf(stderr, "Invalid option -%c\n", opt);
                exit(1);
                break;
        }
    }

    /* init logger */
    pthread_mutex_init(&logger_lock, NULL);

    run_server(db_name, server_address);

    return 0;
}
