#include "orderer.h"

#include "log.h"
#include "utils.h"

// --- Global Variables for Raft Consensus ---

// Index of highest log entry known to be committed (replicated on majority of servers)
atomic<unsigned long> commit_index(0); 

// Index of last log entry (maintained by the leader)
atomic<unsigned long> last_log_index(0);

// For each follower, the index of next log entry to send
deque<atomic<unsigned long>> next_index;

// For each follower, index of highest log entry known to be replicated
deque<atomic<unsigned long>> match_index;

// Network addresses of follower nodes
vector<string> follower_grpc_endpoints;

// Queue for storing incoming transactions from clients
TransactionQueue tq;

// Current node role (LEADER, FOLLOWER, CANDIDATE)
Role role;

// Mutex for thread-safe logging
pthread_mutex_t logger_lock;

// Flag to indicate when the system is ready to process client requests
atomic<bool> ready_flag = false;

// Flag to signal system shutdown
atomic<bool> end_flag = false;

// Counter for total transactions processed
atomic<long> total_ops = 0;

/**
 * Thread function that replicates log entries to followers.
 * This implements the log replication mechanism of Raft consensus.
 * Each follower has a dedicated replication thread.
 */
void *log_replication_thread(void *arg) {
    // Get thread context containing follower information
    struct ThreadContext ctx = *(struct ThreadContext *)arg;
    log_info(stderr, "[server_index = %d]log replication thread is running for follower %s.", 
             ctx.server_index, ctx.grpc_endpoint.c_str());
    
    // Set up gRPC channel to the follower
    shared_ptr<grpc::Channel> channel = grpc::CreateChannel(ctx.grpc_endpoint, grpc::InsecureChannelCredentials());
    unique_ptr<ConsensusComm::Stub> stub(ConsensusComm::NewStub(channel));

    // Open the log file for reading
    ifstream log("./consensus/raft.log", ios::in);
    assert(log.is_open());

    // Main replication loop
    while (true) {
        // Check if there are new entries to send to this follower
        if (last_log_index >= next_index[ctx.server_index]) {
            // Prepare AppendEntries RPC
            ClientContext context;
            AppendRequest app_req;
            AppendResponse app_rsp;

            // Set leader's commit index
            app_req.set_leader_commit(commit_index);
            
            // Add log entries to the request (up to LOG_ENTRY_BATCH entries)
            int index = 0;
            for (; index < LOG_ENTRY_BATCH && next_index[ctx.server_index] + index <= last_log_index; index++) {
                // Read entry size
                uint32_t size;
                log.read((char *)&size, sizeof(uint32_t));
                
                // Read entry content
                char *entry_ptr = (char *)malloc(size);
                log.read(entry_ptr, size);
                app_req.add_log_entries(entry_ptr, size);
                free(entry_ptr);
            }

            // Send AppendEntries RPC to follower
            Status status = stub->append_entries(&context, app_req, &app_rsp);
            if (!status.ok()) {
                log_err("[server_index = %d]gRPC failed with error message: %s.", 
                        ctx.server_index, status.error_message().c_str());
                continue;
            } else {
                log_debug(stderr, "[server_index = %d]send append_entries RPC. last_log_index = %ld. next_index = %ld. commit_index = %ld.",
                          ctx.server_index, last_log_index.load(), next_index[ctx.server_index].load(), commit_index.load());
            }

            // Update next_index and match_index for this follower
            next_index[ctx.server_index] += index;
            match_index[ctx.server_index] = next_index[ctx.server_index] - 1;
        }
    }
}

/**
 * Checks for read-write conflicts between transactions.
 * Returns true if current_trans writes to any key that target_trans reads.
 */
bool has_rw_conflicts(int target_trans, int current_trans,
                      const vector<unordered_set<string>> &read_sets, 
                      const vector<unordered_set<string>> &write_sets) {
    // If either read set or write set is empty, no conflict
    if (read_sets[target_trans].size() == 0 || write_sets[current_trans].size() == 0) {
        return false;
    } else {
        // Check if any key in current transaction's write set is in target transaction's read set
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

/**
 * Checks for write-write conflicts between transactions.
 * Returns true if both transactions write to the same key.
 */
bool has_ww_conflicts(int target_trans, int current_trans, 
                     const vector<unordered_set<string>> &write_sets) {
    // If either write set is empty, no conflict
    if (write_sets[target_trans].size() == 0 || write_sets[current_trans].size() == 0) {
        return false;
    } else {
        // Check if any key in current transaction's write set is in target transaction's write set
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

/**
 * Checks for write-read conflicts between transactions.
 * Returns true if target_trans writes to any key that current_trans reads.
 */
bool has_wr_conflicts(int target_trans, int current_trans,
                      const vector<unordered_set<string>> &read_sets, 
                      const vector<unordered_set<string>> &write_sets) {
    // If either write set or read set is empty, no conflict
    if (write_sets[target_trans].size() == 0 || read_sets[current_trans].size() == 0) {
        return false;
    } else {
        // Check if any key in target transaction's write set is in current transaction's read set
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

/**
 * Thread function that forms blocks from committed log entries.
 * Creates dependency graphs for transactions to enable parallel validation.
 */
void *block_formation_thread(void *arg) {
    log_info(stderr, "Block formation thread is running.");
    
    // Set up gRPC connections to validator nodes (compute servers)
    shared_ptr<grpc::Channel> channel;
    unique_ptr<ComputeComm::Stub> stub;

    // Only the leader needs to connect to validator nodes
    if (role == LEADER) {
        string validator_grpc_endpoint;
        fstream fs;
        string configfile = *(string *)(arg);
        fs.open(configfile, fstream::in);
        
        // Parse configuration file to get validator endpoint
        for (string line; getline(fs, line);) {
            vector<string> tmp = split(line, "=");
            assert(tmp.size() == 2);
            if (tmp[0] == "validator") {
                validator_grpc_endpoint = tmp[1];
            }
        }
        
        // Create gRPC channel to validator
        channel = grpc::CreateChannel(validator_grpc_endpoint, grpc::InsecureChannelCredentials());
        stub = ComputeComm::NewStub(channel);
    }

    // Open log file to read committed entries
    ifstream log("./consensus/raft.log", ios::in);
    assert(log.is_open());

    // State variables for block formation
    unsigned long last_applied = 0;  // Last log entry that has been applied
    int majority = follower_grpc_endpoints.size() / 2;  // Majority of nodes (for commit determination)
    int block_index = 0;  // Current block index
    int trans_index = 0;  // Current transaction index within the block
    size_t max_block_size = 200 * 1024;  // Maximum block size (200KB)
    size_t curr_size = 0;  // Current block size
    int local_ops = 0;  // Local operation counter

    // Block building variables
    Block block;  // Current block being built
    google::protobuf::Empty rsp;  // Empty response for gRPC
    vector<unordered_set<string>> read_sets;  // Read sets for transactions in current block
    vector<unordered_set<string>> write_sets;  // Write sets for transactions in current block
    
    // Set up async gRPC for streaming blocks to validator (only for leader)
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

    // Main block formation loop
    while (!end_flag) {
        // Leader updates commit_index based on match_index of followers
        if (role == LEADER) {
            int N = commit_index + 1;
            int count = 0;
            for (int i = 0; i < match_index.size(); i++) {
                if (match_index[i] >= N) {
                    count++;
                }
            }
            // If majority of followers have replicated entry N, commit it
            if (count >= majority) {
                commit_index = N;
            }
        }

        // Process committed but not yet applied entries
        if (commit_index > last_applied) {
            last_applied++;
            
            // Read entry from log
            uint32_t size;
            log.read((char *)&size, sizeof(uint32_t));
            char *entry_ptr = (char *)malloc(size);
            log.read(entry_ptr, size);
            curr_size += size;
            string serialized_transaction(entry_ptr, size);
            free(entry_ptr);

            // Add transaction to current block
            log_debug(stderr, "[block_id = %d, trans_id = %d]: added transaction to block.", 
                      block_index, trans_index);
            Endorsement *transaction = block.add_transactions();
            if (!transaction->ParseFromString(serialized_transaction)) {
                log_err("block formation thread: error in deserialising transaction.");
            }
            local_ops++;

            // The following section for building dependency graph is commented out
            // but can be enabled for parallel validation
            /*
            // Extract read and write sets
            unordered_set<string> read_set;
            unordered_set<string> write_set;
            for (int i = 0; i < transaction->read_set_size(); i++) {
                read_set.insert(transaction->read_set(i).read_key());
            }
            for (int i = 0; i < transaction->write_set_size(); i++) {
                write_set.insert(transaction->write_set(i).write_key());
            }
            read_sets.push_back(read_set);
            write_sets.push_back(write_set);

            // Build dependency graph for parallel validation
            transaction->clear_adjacency_list();
            for (int target_index = 0; target_index < trans_index; target_index++) {
                // Check read-write, write-write, write-read conflicts
                if (has_rw_conflicts(target_index, trans_index, read_sets, write_sets) ||
                    has_ww_conflicts(target_index, trans_index, write_sets) ||
                    has_wr_conflicts(target_index, trans_index, read_sets, write_sets)) {
                    transaction->add_adjacency_list(target_index);
                }
            }
            trans_index++;
            */

            // Check if block has reached maximum size
            if (curr_size >= max_block_size) {
                // Set block ID
                block.set_block_id(block_index);
                
                // Send block to validator (if leader)
                if (role == LEADER) {
                    validator_stream->Write(block, (void *)1);
                    bool ok;
                    void *got_tag;
                    cq.Next(&got_tag, &ok);
                }

                // Reset state for next block
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
    
    // Update total operations counter
    total_ops = local_ops;
    return NULL;
}

/**
 * gRPC service implementation for consensus communication.
 * Handles AppendEntries RPCs and transaction submissions.
 */
class ConsensusCommImpl final : public ConsensusComm::Service {
   public:
    // Constructor creates/opens log file
    explicit ConsensusCommImpl() : log("./consensus/raft.log", ios::out | ios::binary) {}

    /**
     * Implementation of AppendEntries RPC (Raft consensus).
     * Appends entries from leader to local log and updates commit_index.
     */
    Status append_entries(ServerContext *context, const AppendRequest *request, AppendResponse *response) override {
        // Process each log entry in the request
        int i = 0;
        for (; i < request->log_entries_size(); i++) {
            // Write entry size and content to log
            uint32_t size = request->log_entries(i).size();
            log.write((char *)&size, sizeof(uint32_t));
            log.write(request->log_entries(i).c_str(), size);
            last_log_index++;
        }
        log.flush();

        // Update commit_index based on leader's commit_index
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

    /**
     * Handles single transaction submission to leader.
     * Adds the transaction to the transaction queue.
     */
    Status send_to_leader(ServerContext *context, const Endorsement* endorsement, 
                         google::protobuf::Empty *response) override {
        pthread_mutex_lock(&tq.mutex);
        tq.trans_queue.emplace(endorsement->SerializeAsString());
        pthread_mutex_unlock(&tq.mutex);

        return Status::OK;
    }

    /**
     * Handles transaction stream submission to leader.
     * Adds multiple transactions to the transaction queue.
     */
    Status send_to_leader_stream(ServerContext *context, ServerReader<Endorsement>* reader, 
                                google::protobuf::Empty *response) override {
        Endorsement endorsement;

        // Read each transaction from the stream
        while (reader->Read(&endorsement)) {
            pthread_mutex_lock(&tq.mutex);
            tq.trans_queue.emplace(endorsement.SerializeAsString());
            pthread_mutex_unlock(&tq.mutex);
        }

        return Status::OK;
    }

   private:
    ofstream log;  // Log file stream
};

/**
 * Main function for running a leader node.
 * Sets up threads for log replication and block formation.
 */
void run_leader(const std::string &server_address, std::string configfile) {
    // Clean up and create consensus directory
    std::filesystem::remove_all("./consensus");
    std::filesystem::create_directory("./consensus");

    // Open log file
    ofstream log("./consensus/raft.log", ios::out | ios::binary);

    // Create threads for log replication (one per follower)
    pthread_t *repl_tids;
    repl_tids = (pthread_t *)malloc(sizeof(pthread_t) * follower_grpc_endpoints.size());
    struct ThreadContext *ctxs = (struct ThreadContext *)calloc(
        follower_grpc_endpoints.size(), sizeof(struct ThreadContext));
    
    // Initialize and start replication threads
    for (int i = 0; i < follower_grpc_endpoints.size(); i++) {
        next_index.emplace_back(1);
        match_index.emplace_back(0);
        ctxs[i].grpc_endpoint = follower_grpc_endpoints[i];
        ctxs[i].server_index = i;
        pthread_create(&repl_tids[i], NULL, log_replication_thread, &ctxs[i]);
        pthread_detach(repl_tids[i]);
    }
    
    // Create block formation thread
    pthread_t block_form_tid;
    pthread_create(&block_form_tid, NULL, block_formation_thread, &configfile);
    pthread_detach(block_form_tid);

    // Set up gRPC server for consensus communication
    ConsensusCommImpl service;
    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    log_info(stderr, "RPC server listening on %s", server_address.c_str());

    // Set ready flag to start processing client requests
    ready_flag = true;

    // Main loop to process transactions from queue and append to log
    while (true) {
        pthread_mutex_lock(&tq.mutex);
        int i = 0;
        for (; (!tq.trans_queue.empty()) && i < LOG_ENTRY_BATCH; i++) {
            // Write transaction size and content to log
            uint32_t size = tq.trans_queue.front().size();
            log.write((char *)&size, sizeof(uint32_t));
            log.write(tq.trans_queue.front().c_str(), tq.trans_queue.front().size());
            tq.trans_queue.pop();
        }
        log.flush();
        last_log_index += i;  // Update last log index
        pthread_mutex_unlock(&tq.mutex);
    }
}

/**
 * Thread function that simulates client requests for benchmarking.
 */
void *client_thread(void *arg) {
    int trans_per_interval = 2000;  // Number of transactions per interval
    int interval = 20000;  // Interval in microseconds (20ms)

    // Random generator for creating random values
    default_random_engine generator;
    uniform_int_distribution<int> distribution(0, 200);

    // Wait until system is ready
    while (!ready_flag)
        ;

    // Buffer for transaction values
    char *value = (char *)malloc(10 * 1024);
    
    // Main loop for generating transactions
    while (!end_flag) {
        usleep(interval);

        // Generate batch of transactions
        for (int i = 0; i < trans_per_interval; i++) {
            int number = distribution(generator);
            string str = "value" + to_string(number);
            bzero(value, 10 * 1024);
            strcpy(value, str.c_str());
            
            // Add transaction to queue
            pthread_mutex_lock(&tq.mutex);
            tq.trans_queue.emplace(value, 10 * 1024);
            pthread_mutex_unlock(&tq.mutex);
        }
    }
    free(value);
    return NULL;
}

/**
 * Thread function that runs benchmarking client and measures throughput.
 */
void *run_client(void *arg) {
    // Create client thread
    pthread_t client_tid;
    pthread_create(&client_tid, NULL, client_thread, NULL);

    // Wait until system is ready
    while (!ready_flag)
        ;

    // Start benchmarking
    log_info(stderr, "*******************************benchmarking started*******************************");
    chrono::milliseconds before, after;
    before = chrono::duration_cast<chrono::milliseconds>(chrono::system_clock::now().time_since_epoch());
    sleep(10);  // Run benchmark for 10 seconds
    end_flag = 1;  // Signal shutdown
    after = chrono::duration_cast<chrono::milliseconds>(chrono::system_clock::now().time_since_epoch());

    // Wait for client thread to finish
    void *status;
    pthread_join(client_tid, &status);

    // Wait for all operations to be counted
    while (total_ops == 0)
        ;

    // Calculate and report throughput
    log_info(stderr, "*******************************benchmarking completed*******************************");
    uint64_t time = (after - before).count();
    log_info(stderr, "throughput = %f /seconds.", ((float)total_ops.load() / time) * 1000);
    return NULL;
}

/**
 * Main function for running a follower node.
 * Sets up block formation thread and gRPC server for consensus.
 */
void run_follower(const std::string &server_address) {
    // Clean up and create consensus directory
    std::filesystem::remove_all("./consensus");
    std::filesystem::create_directory("./consensus");

    // Set up gRPC server for consensus communication
    ConsensusCommImpl service;
    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    log_info(stderr, "RPC server listening on %s", server_address.c_str());

    // Create block formation thread
    pthread_t block_form_tid;
    pthread_create(&block_form_tid, NULL, block_formation_thread, NULL);
    pthread_detach(block_form_tid);

    // Wait for gRPC server to finish (blocking call)
    server->Wait();
}

/**
 * Main function - parses command line arguments and starts appropriate node role.
 */
int main(int argc, char *argv[]) {
    int opt;
    string configfile = "config/consensus.config";
    string server_addr;
    
    // Parse command line options
    while ((opt = getopt(argc, argv, "hlfa:c:")) != -1) {
        switch (opt) {
            case 'h':  // Help
                fprintf(stderr, "compute server usage:\n");
                fprintf(stderr, "\t-h: print this help message\n");
                fprintf(stderr, "\t-l: set role to be the leader\n");
                fprintf(stderr, "\t-f: set role to be a follower\n");
                fprintf(stderr, "\t-a <server_ip:server_port>: the listening addr of grpc server\n");
                fprintf(stderr, "\t-c <path_to_config_file>: path to the configuration file\n");
                exit(0);
            case 'c':  // Config file
                configfile = string(optarg);
                break;
            case 'l':  // Leader role
                role = LEADER;
                break;
            case 'f':  // Follower role
                role = FOLLOWER;
                break;
            case 'a':  // Server address
                server_addr = std::string(optarg);
                break;
            default:
                fprintf(stderr, "Invalid option -%c\n", opt);
                exit(1);
                break;
        }
    }
    
    // Ensure server address is provided
    assert(!server_addr.empty());
    
    // Initialize logger
    pthread_mutex_init(&logger_lock, NULL);

    // Run as leader or follower based on role
    if (role == LEADER) {
        // Parse config file to get follower endpoints
        fstream fs;
        fs.open(configfile, fstream::in);
        for (string line; getline(fs, line);) {
            vector<string> tmp = split(line, "=");
            assert(tmp.size() == 2);
            if (tmp[0] == "follower") {
                follower_grpc_endpoints.push_back(tmp[1]);
            }
        }

        // Uncomment to run benchmarking client
        // pthread_t client_id;
        // pthread_create(&client_id, NULL, run_client, NULL);

        // Run leader node
        run_leader(server_addr, configfile);
    } else if (role == FOLLOWER) {
        // Run follower node
        run_follower(server_addr);
    }

    return 0;
}