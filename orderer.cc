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

// 环形日志文件配置
const size_t RAFT_LOG_MAX_SIZE = 1024 * 1024 * 1024; // 1GB
const size_t RAFT_LOG_HEADER_SIZE = 16; // 文件头部大小
const size_t RAFT_LOG_DATA_START = RAFT_LOG_HEADER_SIZE; // 数据区开始位置

// 文件头结构
struct RaftLogHeader {
    uint64_t write_pos;    // 当前写入位置
    uint64_t valid_data;   // 有效数据量
};

// 初始化环形日志文件
bool initialize_raft_log(const std::string& log_path) {
    std::ofstream log_file(log_path, std::ios::out | std::ios::binary);
    if (!log_file.is_open()) {
        log_err("Failed to create raft log file");
        return false;
    }
    
    // 初始化头部
    RaftLogHeader header;
    header.write_pos = RAFT_LOG_DATA_START;
    header.valid_data = 0;
    
    log_file.write(reinterpret_cast<char*>(&header), sizeof(header));
    
    // 预分配空间
    log_file.seekp(RAFT_LOG_MAX_SIZE - 1);
    log_file.put('\0');
    
    log_file.close();
    log_info(stderr, "Initialized circular raft log file with size: %zu bytes", RAFT_LOG_MAX_SIZE);
    return true;
}

// 更新头部信息
bool update_raft_log_header(std::fstream& log_file, uint64_t write_pos, uint64_t valid_data) {
    log_file.seekp(0, std::ios::beg);
    RaftLogHeader header;
    header.write_pos = write_pos;
    header.valid_data = valid_data;
    
    log_file.write(reinterpret_cast<char*>(&header), sizeof(header));
    return log_file.good();
}

// 读取头部信息
bool read_raft_log_header(std::fstream& log_file, uint64_t& write_pos, uint64_t& valid_data) {
    log_file.seekg(0, std::ios::beg);
    RaftLogHeader header;
    
    log_file.read(reinterpret_cast<char*>(&header), sizeof(header));
    if (!log_file.good()) {
        return false;
    }
    
    write_pos = header.write_pos;
    valid_data = header.valid_data;
    return true;
}

bool has_rw_conflicts(int target_trans, int current_trans,
                      const vector<unordered_set<string>> &read_sets, const vector<unordered_set<string>> &write_sets) {
    // 现有实现保持不变
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
    // 现有实现保持不变
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
    // 现有实现保持不变
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
        stub = ComputeComm::NewStub(channel);
    }

    std::string log_file_path = "./consensus/raft.log";
    std::fstream logi(log_file_path, std::ios::in | std::ios::out | std::ios::binary);
    if (!logi.is_open()) {
        log_err("Failed to open raft log file for reading");
        return NULL;
    }
    
    // 读取当前的写位置和有效数据量
    uint64_t write_pos = RAFT_LOG_DATA_START;
    uint64_t valid_data = 0;
    if (!read_raft_log_header(logi, write_pos, valid_data)) {
        log_err("Failed to read raft log header");
        return NULL;
    }
    
    // 设置初始读位置
    uint64_t read_pos = RAFT_LOG_DATA_START;

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
    if (role == LEADER) {
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
            }
        }

        if (commit_index > last_applied) {
            last_applied++;
            
            // 重新读取写位置，确保获取最新状态
            logi.clear(); // 清理任何错误状态
            if (!read_raft_log_header(logi, write_pos, valid_data)) {
                log_err("Failed to read updated header");
                std::this_thread::sleep_for(std::chrono::microseconds(10));
                continue;
            }
            
            // 计算可读取的数据量
            uint64_t readable_bytes;
            if (write_pos >= read_pos) {
                readable_bytes = write_pos - read_pos;
            } else {
                // 写指针已环绕
                readable_bytes = (RAFT_LOG_MAX_SIZE - read_pos) + (write_pos - RAFT_LOG_DATA_START);
            }
            
            // 检查读指针是否已经追上写指针
            if (readable_bytes == 0) {
                log_debug(stderr, "No data available to read, waiting...");
                std::this_thread::sleep_for(std::chrono::microseconds(10));
                continue;
            }
            
            // 确保至少能读取数据大小字段
            if (readable_bytes < sizeof(uint32_t)) {
                log_debug(stderr, "Not enough data to read size field, waiting...");
                std::this_thread::sleep_for(std::chrono::microseconds(10));
                continue;
            }
            
            // 读取数据大小
            uint32_t size;
            logi.seekg(read_pos);
            logi.read(reinterpret_cast<char*>(&size), sizeof(size));
            
            if (!logi.good()) {
                log_err("Failed to read data size");
                logi.clear();
                std::this_thread::sleep_for(std::chrono::microseconds(10));
                continue;
            }
            
            // 确保有足够的数据可读取完整记录
            if (readable_bytes < sizeof(uint32_t) + size) {
                log_debug(stderr, "Not enough data to read complete record, waiting...");
                std::this_thread::sleep_for(std::chrono::microseconds(10));
                continue;
            }
            
            // 读取数据内容
            char* entry_ptr = (char*)malloc(size);
            if (!entry_ptr) {
                log_err("Failed to allocate memory for entry");
                std::this_thread::sleep_for(std::chrono::microseconds(10));
                continue;
            }
            
            logi.read(entry_ptr, size);
            
            if (!logi.good()) {
                free(entry_ptr);
                logi.clear();
                std::this_thread::sleep_for(std::chrono::microseconds(10));
                continue;
            }
            
            // 成功读取完整记录，更新读位置
            read_pos += sizeof(uint32_t) + size;
            if (read_pos >= RAFT_LOG_MAX_SIZE) {
                read_pos = RAFT_LOG_DATA_START;
            }

            // 处理读取的数据
            curr_size += size;
            string serialized_transaction(entry_ptr, size);
            free(entry_ptr);

            log_debug(stderr, "[block_id = %d, trans_id = %d]: added transaction to block.", block_index, trans_index);
            Endorsement *transaction = block.add_transactions();
            if (!transaction->ParseFromString(serialized_transaction)) {
                log_err("block formation thread: error in deserialising transaction.");
            }
            local_ops++;
            trans_index++;

            if (curr_size >= max_block_size) {
                /* cut the block and send it to all validators */
                block.set_block_id(block_index);
                if (role == LEADER) {
                    validator_stream->Write(block, (void *)1);
                    bool ok;
                    void *got_tag;
                    cq.Next(&got_tag, &ok);
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
    explicit ConsensusCommImpl() : logoo("./consensus/raft.log", ios::in | ios::out | ios::binary) {
        // 读取当前写位置
        if (!read_raft_log_header(logoo, write_pos_, valid_data_)) {
            write_pos_ = RAFT_LOG_DATA_START;
            valid_data_ = 0;
            update_raft_log_header(logoo, write_pos_, valid_data_);
        }
    }

    /* implementation of AppendEntriesRPC */
    Status append_entries(ServerContext *context, const AppendRequest *request, AppendResponse *response) override {
        int i = 0;
        for (; i < request->log_entries_size(); i++) {
            uint32_t size = request->log_entries(i).size();
            const std::string& entry_data = request->log_entries(i);
            
            
            
            // 写入数据大小
            logoo.seekp(write_pos_);
            logoo.write(reinterpret_cast<const char*>(&size), sizeof(size));
            
            // 写入数据
            logoo.write(entry_data.c_str(), entry_data.size());
            
            if (!logoo.good()) {
                log_err("Failed to write to raft log");
                return Status(grpc::StatusCode::INTERNAL, "Failed to write to raft log"); // 修复
                // return Status::INTERNAL;
            }
            
            // 更新写位置
            write_pos_ += sizeof(size) + entry_data.size();
            valid_data_ += sizeof(size) + entry_data.size();

            // 检查是否需要环绕
            if (write_pos_  > RAFT_LOG_MAX_SIZE) {
                write_pos_ = RAFT_LOG_DATA_START;
                update_raft_log_header(logoo, write_pos_, valid_data_);
                logoo.flush();
            }
            
            last_log_index++;
        }
        
        // 更新头部
        update_raft_log_header(logoo, write_pos_, valid_data_);
        logoo.flush();

        uint64_t leader_commit = request->leader_commit();
        if (leader_commit > commit_index) {
            if (leader_commit > last_log_index) {
                commit_index = last_log_index.load();
            } else {
                commit_index = leader_commit;
            }
        }

        log_debug(stderr, "AppendEntriesRPC finished: last_log_index = %ld, commit_index = %ld.", 
                 last_log_index.load(), commit_index.load());

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
    std::fstream logoo;
    uint64_t write_pos_ = RAFT_LOG_DATA_START;
    uint64_t valid_data_ = 0;
};

void run_leader(const std::string &server_address, std::string configfile) {
    std::filesystem::remove_all("./consensus");
    std::filesystem::create_directory("./consensus");

    std::string log_path = "./consensus/raft.log";
    initialize_raft_log(log_path);
    
    std::fstream logo(log_path, std::ios::in | std::ios::out | std::ios::binary);
    if (!logo.is_open()) {
        log_err("Failed to open raft log file for writing");
        return;
    }
    
    // 读取当前的写位置
    uint64_t write_pos = RAFT_LOG_DATA_START;
    uint64_t valid_data = 0;
    if (!read_raft_log_header(logo, write_pos, valid_data)) {
        write_pos = RAFT_LOG_DATA_START;
        valid_data = 0;
        update_raft_log_header(logo, write_pos, valid_data);
    }

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

    while (true) {
        pthread_mutex_lock(&tq.mutex);
        int i = 0;
        for (; (!tq.trans_queue.empty()) && i < LOG_ENTRY_BATCH; i++) {
            uint32_t size = tq.trans_queue.front().size();
            const std::string& data = tq.trans_queue.front();
            
            // 写入大小
            logo.seekp(write_pos);
            logo.write(reinterpret_cast<char*>(&size), sizeof(size));
            
            // 写入数据
            logo.write(data.c_str(), data.size());
            
            if (!logo.good()) {
                log_err("Failed to write to raft log");
                pthread_mutex_unlock(&tq.mutex);
                sleep(1); // 避免快速失败的循环
                continue;
            }
            
            // 更新写位置
            write_pos += sizeof(size) + size;
            valid_data += sizeof(size) + size;

            // 检查是否需要环绕
            if (write_pos > RAFT_LOG_MAX_SIZE) {
                log_info(stderr, "Writer wrapping around: write_pos from %lu to %lu", write_pos, RAFT_LOG_DATA_START);
                write_pos = RAFT_LOG_DATA_START;
                // 立即强制刷新头部信息
                update_raft_log_header(logo, write_pos, valid_data);
                logo.flush();
            }
            
            tq.trans_queue.pop();
        }
        
        // 更新头部信息
        if (i > 0) {
            update_raft_log_header(logo, write_pos, valid_data);
            logo.flush();
        }
        
        last_log_index += i;
        pthread_mutex_unlock(&tq.mutex);
    }
}

void *client_thread(void *arg) {
    // 现有实现保持不变
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
    // 现有实现保持不变
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

    std::string log_path = "./consensus/raft.log";
    initialize_raft_log(log_path);

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