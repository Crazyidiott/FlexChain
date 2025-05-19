#include "orderer.h"
#include "log.h"
#include "utils.h"
#include <sys/stat.h>

atomic<unsigned long> commit_index(0);
atomic<unsigned long> last_log_index(0);
deque<atomic<unsigned long>> next_index;
deque<atomic<unsigned long>> match_index;
vector<string> follower_grpc_endpoints;
TransactionQueue tq;
Role role;
pthread_mutex_t logger_lock;
atomic<bool> ready_flag = false;
atomic<bool> end_flag = false;
atomic<long> total_ops = 0;


bool has_rw_conflicts(int target_trans, int current_trans,
                      const vector<unordered_set<string>> &read_sets, const vector<unordered_set<string>> &write_sets) {
    if (read_sets[target_trans].size() == 0 || write_sets[current_trans].size() == 0) {
        return false;
    } else {
        bool has_conflicts = false;
        for (auto it = write_sets[current_trans].begin(); it != write_sets[current_trans].end(); it++) {
            if (read_sets[target_trans].find(*it) != read_sets[target_trans].end()) {
                has_conflicts = true;
                break;
            }
        }
        return has_conflicts;
    }
}

bool has_ww_conflicts(int target_trans, int current_trans, const vector<unordered_set<string>> &write_sets) {
    if (write_sets[target_trans].size() == 0 || write_sets[current_trans].size() == 0) {
        return false;
    } else {
        bool has_conflicts = false;
        for (auto it = write_sets[current_trans].begin(); it != write_sets[current_trans].end(); it++) {
            if (write_sets[target_trans].find(*it) != write_sets[target_trans].end()) {
                has_conflicts = true;
                break;
            }
        }
        return has_conflicts;
    }
}

bool has_wr_conflicts(int target_trans, int current_trans,
                      const vector<unordered_set<string>> &read_sets, const vector<unordered_set<string>> &write_sets) {
    if (write_sets[target_trans].size() == 0 || read_sets[current_trans].size() == 0) {
        return false;
    } else {
        bool has_conflicts = false;
        for (auto it = write_sets[target_trans].begin(); it != write_sets[target_trans].end(); it++) {
            if (read_sets[current_trans].find(*it) != read_sets[current_trans].end()) {
                has_conflicts = true;
                break;
            }
        }
        return has_conflicts;
    }
}

void *block_formation_thread(void *arg) {
    log_info(stderr, "Block formation thread is running.");
    /* set up grpc channels to validators */
    shared_ptr<grpc::Channel> channel;
    unique_ptr<ComputeComm::Stub> stub;

    if (role == LEADER) {
        string validator_grpc_endpoint;
        fstream fs;
        string configfile = *(string *)(arg);
        fs.open(configfile, fstream::in);
        for (string line; getline(fs, line);) {
            vector<string> tmp = split(line, "=");
            assert(tmp.size() == 2);
            if (tmp[0] == "validator") {
                validator_grpc_endpoint = tmp[1];
            }
        }
        channel = grpc::CreateChannel(validator_grpc_endpoint, grpc::InsecureChannelCredentials());
        // while (channel->GetState(true) != GRPC_CHANNEL_READY)
        //     ;
        // log_info(stderr, "block formation thread: channel for validator is in ready state.");
        stub = ComputeComm::NewStub(channel);
    }

    // 打开日志文件函数
    auto open_current_log_file = [](const string& path) -> ifstream {
        ifstream logi(path, ios::in | ios::binary);
        if (!logi.is_open()) {
            log_err("Failed to open log file: %s", path.c_str());
        }
        return logi;
    };

    std::string log_file_path = "./consensus/raft.log";
    ifstream logi = open_current_log_file(log_file_path);
    // std::string log_file_path = "./consensus/raft.log";
    // ifstream logi("./consensus/raft.log", ios::in);
    // assert(logi.is_open());

    // 重新打开文件函数
    auto reopen_log_file = [&](bool check_rotated = true) {
        logi.close();
        
        // 如果需要，检查是否存在轮转后的文件
        if (check_rotated) {
            string rotated_log = "./consensus/raft.log.1";
            if (std::filesystem::exists(rotated_log)) {
                // 获取当前读取位置相对于文件大小的比例
                struct stat current_stat, rotated_stat;
                if (stat(log_file_path.c_str(), &current_stat) == 0 && 
                    stat(rotated_log.c_str(), &rotated_stat) == 0) {
                    // 如果我们接近当前文件的末尾，切换到轮转后的文件
                    if (logi.tellg() > current_stat.st_size - 1024) {
                        log_info(stderr, "Switching to rotated log file");
                        log_file_path = rotated_log;
                    }
                }
            }
        }
        
        logi = open_current_log_file(log_file_path);
    };



    unsigned long last_applied = 0;
    int majority = follower_grpc_endpoints.size() / 2;
    int block_index = 0;
    int trans_index = 0;
    size_t max_block_size = 200 * 1024;
    size_t curr_size = 0;
    int local_ops = 0;

    Block block;
    google::protobuf::Empty rsp;
    vector<unordered_set<string>> read_sets;
    vector<unordered_set<string>> write_sets;
    
    ClientContext context;
    context.set_wait_for_ready(true);
    CompletionQueue cq;
    unique_ptr<ClientAsyncWriter<Block>> validator_stream;
    if (role == LEADER)
    {
        validator_stream = stub->Asyncsend_to_validator_stream(&context, &rsp, &cq, (void *)1);
    }
    bool ok;
    void *got_tag;
    cq.Next(&got_tag, &ok);

    while (!end_flag) {
        if (role == LEADER) {
            int N = commit_index + 1;
            int count = 0;
            for (int i = 0; i < match_index.size(); i++) {
                if (match_index[i] >= N) {
                    count++;
                }
            }
            if (count >= majority) {
                commit_index = N;
                // log_debug(stderr, "commit_index is updated to %ld.", commit_index.load());
            }
        }

        if (commit_index > last_applied) {
            last_applied++;

            // 获取当前文件写入的位置
            std::streampos current_read_pos = logi.tellg();
            logi.clear(); // 清理任何错误状态，不清除会影响seekg

            struct stat file_stat;
            if (stat(log_file_path.c_str(), &file_stat) != 0) {
                perror("stat failed");
                std::this_thread::sleep_for(std::chrono::microseconds(10));
                continue;
            }
            off_t file_size = file_stat.st_size;

            // 判断是否可以读取size
            if (current_read_pos + sizeof(uint32_t) > file_size) {
                 // 检查是否存在轮转后的新日志文件
                string new_log = "./consensus/raft.log";
                if (log_file_path != new_log && std::filesystem::exists(new_log)) {
                    // 如果当前使用的是旧的日志文件，并且新的日志文件已创建，切换到新文件
                    log_info(stderr, "Switching to new log file after rotation");
                    log_file_path = new_log;
                    reopen_log_file(false);
                    continue;
                }

                // size都读不了，说明写线程没写完，等
                std::this_thread::sleep_for(std::chrono::microseconds(10));
                continue;
            }

            uint32_t size;
            logi.read((char *)&size, sizeof(uint32_t));
            if (!logi.good()) {
                // 读取错误
                logi.clear();
                std::this_thread::sleep_for(std::chrono::microseconds(10));
                continue;
            }

            // 判断是否可以读取size字节
            current_read_pos = logi.tellg(); // 更新读指针
            if (current_read_pos + size > file_size) {
                // 检查是否需要切换到新文件
                string new_log = "./consensus/raft.log";
                if (log_file_path != new_log && std::filesystem::exists(new_log)) {
                    log_info(stderr, "Switching to new log file after rotation");
                    log_file_path = new_log;
                    reopen_log_file(false);
                    continue;
                }

                // 说明size部分还没写完
                // 回退
                logi.seekg(-(std::streamoff)sizeof(uint32_t), std::ios::cur);
                std::this_thread::sleep_for(std::chrono::microseconds(10));
                continue;
            }

            // 真正读取数据
            char *entry_ptr = (char *)malloc(size);
            logi.read(entry_ptr, size);
            if (!logi.good()) {
                free(entry_ptr);
                logi.clear();
                std::this_thread::sleep_for(std::chrono::microseconds(10));
                continue;
            }

            curr_size += size;
            string serialized_transaction(entry_ptr, size);
            free(entry_ptr);

            log_debug(stderr, "[block_id = %d, trans_id = %d]: added transaction to block.", block_index, trans_index);
            Endorsement *transaction = block.add_transactions();
            if (!transaction->ParseFromString(serialized_transaction)) {
                log_err("block formation thread: error in deserialising transaction.");
            }
            local_ops++;
        

            // unordered_set<string> read_set;
            // unordered_set<string> write_set;
            // for (int i = 0; i < transaction->read_set_size(); i++) {
            //     read_set.insert(transaction->read_set(i).read_key());
            // }
            // for (int i = 0; i < transaction->write_set_size(); i++) {
            //     write_set.insert(transaction->write_set(i).write_key());
            // }
            // read_sets.push_back(read_set);
            // write_sets.push_back(write_set);

            // transaction->clear_adjacency_list();
            // for (int target_index = 0; target_index < trans_index; target_index++) {
            //     /* check read-write write-write write-read conflict */
            //     if (has_rw_conflicts(target_index, trans_index, read_sets, write_sets) ||
            //         has_ww_conflicts(target_index, trans_index, write_sets) ||
            //         has_wr_conflicts(target_index, trans_index, read_sets, write_sets)) {
            //         transaction->add_adjacency_list(target_index);
            //     }
            // }
            // trans_index++;

            if (curr_size >= max_block_size) {
                /* cut the block and send it to all validators */
                block.set_block_id(block_index);
                if (role == LEADER) {
                    // ClientContext context;
                    // context.set_wait_for_ready(true);

                    validator_stream->Write(block, (void *)1);
                    bool ok;
                    void *got_tag;
                    // cq.AsyncNext(&got_tag, &ok, gpr_time_0(GPR_CLOCK_REALTIME));
                    cq.Next(&got_tag, &ok);

                    // stub->async()->send_to_validator(&context, &block, &rsp, [](Status s){});
                    // Status status = stub->send_to_validator(&context, block, &rsp);  // TODO: use client stream + async
                    // if (!status.ok()) {
                    //     log_err("block formation thread: gRPC failed with error message: %s.", status.error_message().c_str());
                    // } else {
                    //     log_debug(stderr, "block formation thread: block #%d is sent to validator.", block_index);
                    // }
                }

                curr_size = 0;
                block_index++;
                trans_index = 0;

                block.clear_block_id();
                block.clear_transactions();

                read_sets.clear();
                write_sets.clear();
            }
        }
    }
    total_ops = local_ops;
    return NULL;
}

class ConsensusCommImpl final : public ConsensusComm::Service {
   public:
    explicit ConsensusCommImpl() : logoo("./consensus/raft.log", ios::out | ios::binary) {
        max_log_size = static_cast<uint64_t>(1024) * 1024 * 1024; // 1GB
        max_log_files = 5; // 保留5个历史日志
        current_log_size = 0;
        struct stat file_stat;
        if (stat("./consensus/raft.log", &file_stat) == 0) {
            current_log_size = file_stat.st_size;
        }
    }
    

    /* implementation of AppendEntriesRPC */
    Status append_entries(ServerContext *context, const AppendRequest *request, AppendResponse *response) override {
        int i = 0;
        for (; i < request->log_entries_size(); i++) {
            uint32_t size = request->log_entries(i).size();

            // 检查是否需要轮转日志
            if (current_log_size + sizeof(uint32_t) + size > max_log_size) {
                rotate_log();
            }

            logoo.write((char *)&size, sizeof(uint32_t));
            logoo.write(request->log_entries(i).c_str(), size);

            current_log_size += sizeof(uint32_t) + size;
            last_log_index++;
        }
        logoo.flush();

        uint64_t leader_commit = request->leader_commit();
        if (leader_commit > commit_index) {
            if (leader_commit > last_log_index) {
                commit_index = last_log_index.load();
            } else {
                commit_index = leader_commit;
            }
        }

        log_debug(stderr, "AppendEntriesRPC finished: last_log_index = %ld, commit_index = %ld.", last_log_index.load(), commit_index.load());

        return Status::OK;
    }

    Status send_to_leader(ServerContext *context, const Endorsement* endorsement, google::protobuf::Empty *response) override {
        pthread_mutex_lock(&tq.mutex);
        tq.trans_queue.emplace(endorsement->SerializeAsString());
        pthread_mutex_unlock(&tq.mutex);

        return Status::OK;
    }

    Status send_to_leader_stream(ServerContext *context, ServerReader<Endorsement>* reader, google::protobuf::Empty *response) override {
        Endorsement endorsement;

        while (reader->Read(&endorsement)) {
            pthread_mutex_lock(&tq.mutex);
            tq.trans_queue.emplace(endorsement.SerializeAsString());
            pthread_mutex_unlock(&tq.mutex);
        }

        return Status::OK;
    }

   private:
    ofstream logoo;
    size_t max_log_size;  // 最大日志文件大小
    size_t current_log_size;  // 当前日志文件大小
    int max_log_files;  // 最大保留的日志文件数量
    // 日志轮转函数
    void rotate_log() {
        // 关闭当前日志文件
        logoo.close();
        
        // 删除最老的日志文件（如果超过最大数量）
        string oldest_log = "./consensus/raft.log." + to_string(max_log_files);
        if (std::filesystem::exists(oldest_log)) {
            std::filesystem::remove(oldest_log);
        }
        
        // 重命名现有的日志文件，从最老的开始
        for (int i = max_log_files - 1; i >= 1; i--) {
            string old_name = "./consensus/raft.log." + to_string(i);
            string new_name = "./consensus/raft.log." + to_string(i + 1);
            if (std::filesystem::exists(old_name)) {
                std::filesystem::rename(old_name, new_name);
            }
        }
        
        // 重命名当前日志文件
        std::filesystem::rename("./consensus/raft.log", "./consensus/raft.log.1");
        
        // 打开新的日志文件
        logoo.open("./consensus/raft.log", ios::out | ios::binary);
        if (!logoo.is_open()) {
            log_err("Failed to open new log file after rotation");
        }
        
        // 重置当前日志大小
        current_log_size = 0;
        
        log_info(stderr, "Log rotation completed, created new raft.log");
    }
};

void run_leader(const std::string &server_address, std::string configfile) {
    std::filesystem::remove_all("./consensus");
    std::filesystem::create_directory("./consensus");

    ofstream logo("./consensus/raft.log", ios::out | ios::binary);
    assert(logo.is_open());
    size_t max_log_size = 1024 * 1024 * 1024; // 1GB
    size_t current_log_size = 0;
    int max_log_files = 5;

    pthread_t block_form_tid;
    pthread_create(&block_form_tid, NULL, block_formation_thread, &configfile);
    pthread_detach(block_form_tid);

    /* spawn replication threads and the block formation thread */
    pthread_t *repl_tids;
    repl_tids = (pthread_t *)malloc(sizeof(pthread_t) * follower_grpc_endpoints.size());
    struct ThreadContext *ctxs = (struct ThreadContext *)calloc(follower_grpc_endpoints.size(), sizeof(struct ThreadContext));
    for (int i = 0; i < follower_grpc_endpoints.size(); i++) {
        next_index.emplace_back(1);
        match_index.emplace_back(0);
        ctxs[i].grpc_endpoint = follower_grpc_endpoints[i];
        ctxs[i].server_index = i;
        // pthread_create(&repl_tids[i], NULL, log_replication_thread, &ctxs[i]);
        // pthread_detach(repl_tids[i]);
    }

    /* start the grpc server for ConsensusComm */
    ConsensusCommImpl service;
    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    log_info(stderr, "RPC server listening on %s", server_address.c_str());

    ready_flag = true;
    auto rotate_log = [&]() {
        // 关闭当前日志文件
        logo.close();
        
        // 删除最老的日志文件（如果超过最大数量）
        string oldest_log = "./consensus/raft.log." + to_string(max_log_files);
        if (std::filesystem::exists(oldest_log)) {
            std::filesystem::remove(oldest_log);
        }
        
        // 重命名现有的日志文件，从最老的开始
        for (int i = max_log_files - 1; i >= 1; i--) {
            string old_name = "./consensus/raft.log." + to_string(i);
            string new_name = "./consensus/raft.log." + to_string(i + 1);
            if (std::filesystem::exists(old_name)) {
                std::filesystem::rename(old_name, new_name);
            }
        }
        
        // 重命名当前日志文件
        std::filesystem::rename("./consensus/raft.log", "./consensus/raft.log.1");
        
        // 打开新的日志文件
        logo.open("./consensus/raft.log", ios::out | ios::binary);
        if (!logo.is_open()) {
            log_err("Failed to open new log file after rotation");
        }
        
        // 重置当前日志大小
        current_log_size = 0;
        
        log_info(stderr, "Log rotation completed, created new raft.log");
    };

    while (true) {
        pthread_mutex_lock(&tq.mutex);
        int i = 0;
        for (; (!tq.trans_queue.empty()) && i < LOG_ENTRY_BATCH; i++) {
            uint32_t size = tq.trans_queue.front().size();

            // 检查是否需要轮转日志
            if (current_log_size + sizeof(uint32_t) + size > max_log_size) {
                pthread_mutex_unlock(&tq.mutex); // 先解锁，避免长时间持有锁
                rotate_log();
                pthread_mutex_lock(&tq.mutex); // 再次获取锁，继续处理
                
                // 重新检查队列，因为在轮转过程中可能已经改变
                if (tq.trans_queue.empty()) {
                    break;
                }
                size = tq.trans_queue.front().size();
            }

            // std::cout << "Before write: tellP=" << logo.tellp() << std::endl;  
            logo.write((char *)&size, sizeof(uint32_t));
            // std::cout << "After write: tellp=" << logo.tellp() << ",size=" << size << ",good=" << logo.good() << ",fail=" << logo.fail() << std::endl;

            
            logo.write(tq.trans_queue.front().c_str(), tq.trans_queue.front().size());

            current_log_size += sizeof(uint32_t) + size;

            tq.trans_queue.pop();
        }
        logo.flush();
        last_log_index += i;
        pthread_mutex_unlock(&tq.mutex);
    }
}

void *client_thread(void *arg) {
    int trans_per_interval = 2000;
    int interval = 20000;

    default_random_engine generator;
    uniform_int_distribution<int> distribution(0, 200);

    while (!ready_flag)
        ;

    char *value = (char *)malloc(10 * 1024);
    while (!end_flag) {
        usleep(interval);

        for (int i = 0; i < trans_per_interval; i++) {
            int number = distribution(generator);
            string str = "value" + to_string(number);
            bzero(value, 10 * 1024);
            strcpy(value, str.c_str());
            pthread_mutex_lock(&tq.mutex);
            tq.trans_queue.emplace(value, 10 * 1024);
            pthread_mutex_unlock(&tq.mutex);
        }
    }
    free(value);
    return NULL;
}

void *run_client(void *arg) {
    pthread_t client_tid;
    // pthread_create(&client_tid, NULL, client_thread, NULL);

    while (!ready_flag)
        ;

    log_info(stderr, "*******************************benchmarking started*******************************");
    chrono::milliseconds before, after;
    before = chrono::duration_cast<chrono::milliseconds>(chrono::system_clock::now().time_since_epoch());
    sleep(10);
    end_flag = 1;
    after = chrono::duration_cast<chrono::milliseconds>(chrono::system_clock::now().time_since_epoch());

    void *status;
    // pthread_join(client_tid, &status);

    while (total_ops == 0)
        log_info(stderr,"stuck in no total_ops");
        

    log_info(stderr, "*******************************benchmarking completed*******************************");
    uint64_t time = (after - before).count();
    log_info(stderr, "throughput = %f /seconds.", ((float)total_ops.load() / time) * 1000);
    return NULL;
}

void run_follower(const std::string &server_address) {
    std::filesystem::remove_all("./consensus");
    std::filesystem::create_directory("./consensus");

    /* start the grpc server for ConsensusComm */
    ConsensusCommImpl service;
    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    log_info(stderr, "RPC server listening on %s", server_address.c_str());

    pthread_t block_form_tid;
    pthread_create(&block_form_tid, NULL, block_formation_thread, NULL);
    pthread_detach(block_form_tid);

    server->Wait();
}

int main(int argc, char *argv[]) {
    int opt;
    string configfile = "config/consensus.config";
    string server_addr;
    while ((opt = getopt(argc, argv, "hlfa:c:")) != -1) {
        switch (opt) {
            case 'h':
                fprintf(stderr, "compute server usage:\n");
                fprintf(stderr, "\t-h: print this help message\n");
                fprintf(stderr, "\t-l: set role to be the leader\n");
                fprintf(stderr, "\t-f: set role to be a follower\n");
                fprintf(stderr, "\t-a <server_ip:server_port>: the listening addr of grpc server\n");
                fprintf(stderr, "\t-c <path_to_config_file>: path to the configuration file\n");
                exit(0);
            case 'c':
                configfile = string(optarg);
                break;
            case 'l':
                role = LEADER;
                break;
            case 'f':
                role = FOLLOWER;
                break;
            case 'a':
                server_addr = std::string(optarg);
                break;
            default:
                fprintf(stderr, "Invalid option -%c\n", opt);
                exit(1);
                break;
        }
    }
    assert(!server_addr.empty());
    /* init logger */
    pthread_mutex_init(&logger_lock, NULL);

    if (role == LEADER) {
        fstream fs;
        fs.open(configfile, fstream::in);
        for (string line; getline(fs, line);) {
            vector<string> tmp = split(line, "=");
            assert(tmp.size() == 2);
            if (tmp[0] == "follower") {
                follower_grpc_endpoints.push_back(tmp[1]);
            }
        }

        // pthread_t client_id;
        // pthread_create(&client_id, NULL, run_client, NULL);

        run_leader(server_addr, configfile);
    } else if (role == FOLLOWER) {
        run_follower(server_addr);
    }

    return 0;
}