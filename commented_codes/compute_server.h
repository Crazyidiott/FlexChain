/**
 * compute_server.h
 * 
 * Header file for the compute server component of FlexChain, a disaggregated blockchain system.
 * This file defines the key classes and data structures for the compute server, including:
 * - Cache structures for storing data and metadata
 * - Client classes for communicating with storage and other compute servers
 * - Queue structures for managing blocks and validation requests
 * - Thread context and synchronization primitives
 */

 #ifndef COMPUTE_SERVER_H
 #define COMPUTE_SERVER_H
 
 #include <grpc/grpc.h>
 #include <grpcpp/channel.h>
 #include <grpcpp/client_context.h>
 #include <grpcpp/create_channel.h>
 #include <grpcpp/security/credentials.h>
 #include <grpcpp/server.h>
 #include <grpcpp/server_builder.h>
 #include <grpcpp/server_context.h>
 #include <pthread.h>
 #include <semaphore.h>
 #include <stdio.h>
 #include <unistd.h>
 
 #include <atomic>
 #include <filesystem>
 #include <fstream>
 #include <iostream>
 #include <list>
 #include <memory>
 #include <queue>
 #include <set>
 #include <sstream>
 #include <string>
 #include <unordered_map>
 #include <vector>
 
 #include "blockchain.grpc.pb.h"  // Protobuf definitions for blockchain operations
 #include "storage.grpc.pb.h"     // Protobuf definitions for storage operations
 
 using namespace std;
 using grpc::Channel;
 using grpc::ClientAsyncWriter;
 using grpc::ClientContext;
 using grpc::CompletionQueue;
 using grpc::Server;
 using grpc::ServerBuilder;
 using grpc::ServerContext;
 using grpc::ServerReader;
 using grpc::Status;
 
 /**
  * Context structure for worker threads.
  * 
  * Contains information about the thread's RDMA connections and index.
  */
 struct ThreadContext {
     int thread_index;         // Thread identifier
     struct ibv_qp* m_qp;      // InfiniBand queue pair for RDMA operations
     struct ibv_cq* m_cq;      // InfiniBand completion queue for RDMA operations
 };
 
 /**
  * Cache for data retrieved from remote memory.
  * 
  * This class manages a local cache of data buffers, with an LRU (Least Recently Used)
  * eviction policy to manage limited cache space.
  */
 class DataCache {
    public:
     /**
      * Structure representing a cached data frame.
      */
     struct Frame {
         uint64_t l_addr;      // Local memory address of the cached data
         uint64_t r_addr;      // Remote memory address that this cache entry corresponds to
     };
     
     // Maps remote addresses to iterators in the LRU list for quick lookup
     unordered_map<uint64_t, list<struct Frame>::iterator> addr_hashtable;
     pthread_mutex_t hashtable_lock;   // Mutex for thread-safe access to the hash table
 
     list<struct Frame> lru_list;      // LRU list of cached frames
 
     queue<uint64_t> free_addrs;       // Queue of available local memory addresses
     // Since we use write-through, no need to use a background thread for flushing pages
 
     /**
      * Constructor: initializes the mutex lock.
      */
     DataCache() {
         pthread_mutex_init(&hashtable_lock, NULL);
     }
     
     /**
      * Destructor: destroys the mutex lock.
      */
     ~DataCache() {
         pthread_mutex_destroy(&hashtable_lock);
     }
 };
 
 /**
  * Cache for metadata about keys in remote memory.
  * 
  * This class maintains information about where each key's data is stored in remote memory,
  * allowing the compute server to quickly locate data without querying the memory server.
  */
 class MetaDataCache {
    public:
     /**
      * Structure representing metadata for a key-value pair.
      */
     struct EntryHeader {
         uint64_t ptr_next;    // Remote memory address of the key-value pair
         uint64_t bk_addr;     // Bookkeeping address (unused in this implementation)
         string key;           // The key this metadata is for
     };
     
     // Maps keys to iterators in the LRU list for quick lookup
     unordered_map<string, list<struct EntryHeader>::iterator> key_hashtable;
     pthread_mutex_t hashtable_lock;   // Mutex for thread-safe access to the hash table
 
     list<struct EntryHeader> lru_list; // LRU list of metadata entries
 
     // Assume no space limit for metadata cache for now
 
     /**
      * Constructor: initializes the mutex lock.
      */
     MetaDataCache() {
         pthread_mutex_init(&hashtable_lock, NULL);
     }
     
     /**
      * Destructor: destroys the mutex lock.
      */
     ~MetaDataCache() {
         pthread_mutex_destroy(&hashtable_lock);
     }
 };
 
 /**
  * Client for interacting with the storage server.
  * 
  * This class provides methods to read from and write to SSTables on the storage server,
  * as well as to write validated blocks to persistent storage.
  */
 class KVStableClient {
    public:
     /**
      * Constructor: creates a new storage client with the provided channel.
      * 
      * @param channel gRPC channel to the storage server
      */
     KVStableClient(std::shared_ptr<Channel> channel)
         : stub_(KVStable::NewStub(channel)) {}
 
     /**
      * Reads a key-value pair from the SSTable on the storage server.
      * 
      * @param key The key to read
      * @param value Output parameter to store the retrieved value
      * @return 0 on success, 1 if key not found, -1 on error
      */
     int read_sstables(const string& key, string& value);
     
     /**
      * Writes a key-value pair to the SSTable on the storage server.
      * 
      * @param key The key to write
      * @param value The value to write
      * @return 0 on success, -1 on error
      */
     int write_sstables(const string& key, const string& value);
     
     /**
      * Writes a serialized block to the block store on the storage server.
      * 
      * @param block Serialized block data
      * @return 0 on success
      */
     int write_blocks(const string& block);
 
    private:
     unique_ptr<KVStable::Stub> stub_;  // gRPC stub for the storage server
 };
 
 /**
  * Client for interacting with other compute servers.
  * 
  * This class provides methods to send cache invalidation requests to other compute
  * servers and to signal the start of benchmarking.
  */
 class ComputeCommClient {
    public:
     /**
      * Constructor: creates a new compute client with the provided channel.
      * 
      * @param channel gRPC channel to the other compute server
      */
     ComputeCommClient(std::shared_ptr<Channel> channel)
         : stub_(ComputeComm::NewStub(channel)) {}
 
     /**
      * Sends a request to invalidate a cached key on another compute server.
      * 
      * @param key The key to invalidate
      * @return 0 on success
      */
     int invalidate_cn(const string& key);
     
     /**
      * Sends a notification to start benchmarking to another compute server.
      */
     void start_benchmarking();
 
    private:
     unique_ptr<ComputeComm::Stub> stub_;  // gRPC stub for the other compute server
 };
 
 /**
  * Queue for blocks waiting to be validated.
  * 
  * This class manages a thread-safe queue of blocks received from the ordering service
  * that need to be validated by the compute server.
  */
 class BlockQueue {
    public:
     queue<Block> bq_queue;         // Queue of blocks
     pthread_mutex_t mutex;         // Mutex for thread-safe access to the queue
     sem_t full;                    // Semaphore to signal when the queue has blocks
 
     /**
      * Constructor: initializes the mutex and semaphore.
      */
     BlockQueue() {
         pthread_mutex_init(&mutex, NULL);
         sem_init(&full, 0, 0);
     }
 
     /**
      * Destructor: destroys the mutex and semaphore.
      */
     ~BlockQueue() {
         pthread_mutex_destroy(&mutex);
         sem_destroy(&full);
     }
 };
 
 /**
  * Queue for transactions waiting to be validated in parallel.
  * 
  * This class manages a thread-safe queue of transactions that are ready to be
  * validated by parallel validation workers.
  */
 class ValidationQueue {
    public:
     queue<uint64_t> id_queue;      // Queue of transaction IDs
     queue<Endorsement> trans_queue; // Queue of transaction endorsements
     uint64_t curr_block_id;        // ID of the current block being validated
     pthread_mutex_t mutex;         // Mutex for thread-safe access to the queues
     sem_t full;                    // Semaphore to signal when the queue has transactions
 
     /**
      * Constructor: initializes the mutex and semaphore.
      */
     ValidationQueue() {
         pthread_mutex_init(&mutex, NULL);
         sem_init(&full, 0, 0);
     }
 
     /**
      * Destructor: destroys the mutex and semaphore.
      */
     ~ValidationQueue() {
         pthread_mutex_destroy(&mutex);
         sem_destroy(&full);
     }
 };
 
 /**
  * Set of completed transactions during parallel validation.
  * 
  * This class tracks which transactions have been successfully validated
  * during parallel validation, allowing the validation manager to determine
  * when dependencies are satisfied.
  */
 class CompletionSet {
    private:
     set<uint64_t> C;               // Set of completed transaction IDs
     pthread_mutex_t mutex;         // Mutex for thread-safe access to the set
 
    public:
     /**
      * Constructor: initializes the mutex.
      */
     CompletionSet() {
         pthread_mutex_init(&mutex, NULL);
     }
 
     /**
      * Destructor: destroys the mutex.
      */
     ~CompletionSet() {
         pthread_mutex_destroy(&mutex);
     }
 
     /**
      * Adds a transaction ID to the completion set.
      * 
      * @param trans_id The transaction ID to add
      */
     void add(uint64_t trans_id) {
         pthread_mutex_lock(&mutex);
         C.insert(trans_id);
         pthread_mutex_unlock(&mutex);
     }
 
     /**
      * Clears all transaction IDs from the completion set.
      */
     void clear() {
         pthread_mutex_lock(&mutex);
         C.clear();
         pthread_mutex_unlock(&mutex);
     }
 
     /**
      * Checks if a transaction ID is in the completion set.
      * 
      * @param trans_id The transaction ID to check
      * @return true if the transaction ID is in the set, false otherwise
      */
     bool find(uint64_t trans_id) {
         bool find = false;
         pthread_mutex_lock(&mutex);
         if (C.find(trans_id) != C.end()) {
             find = true;
         }
         pthread_mutex_unlock(&mutex);
 
         return find;
     }
 
     /**
      * Returns the number of transaction IDs in the completion set.
      * 
      * @return The size of the completion set
      */
     size_t size() {
         size_t size;
         // pthread_mutex_lock(&mutex);  // Commented out for performance in original code
         size = C.size();
         // pthread_mutex_unlock(&mutex);
         return size;
     }
 };
 
 #endif  // COMPUTE_SERVER_H