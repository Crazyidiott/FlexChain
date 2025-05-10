#include "memory_server.h"

#include "config.h"
#include "log.h"
#include "setup_ib.h"
#include "utils.h"

struct MConfigInfo m_config_info;
struct MemoryIBInfo m_ib_info;
RequestQueue space_manager_rq;
RequestQueue address_manager_rq;
RequestQueue bookkeeping_agent_rq;
SpaceAllocator space_allocator;
GarbageCollector gc;
unordered_map<string, struct EntryHeader> key_to_addr;
shared_ptr<grpc::Channel> channel_ptr;
pthread_mutex_t logger_lock;

uint32_t EVICT_THR = 100; // 默认值
pthread_mutex_t evict_thr_mutex;
std::unique_ptr<Server> config_server;

// 实现MemoryConfigServiceImpl类的方法
Status MemoryConfigServiceImpl::SetEvictThreshold(ServerContext* context, const EvictThresholdRequest* request, EvictThresholdResponse* response) {
    pthread_mutex_lock(&evict_thr_mutex);
    EVICT_THR = request->threshold();
    response->set_threshold(EVICT_THR);
    response->set_success(true);
    log_info(stderr, "Setting EVICT_THR to %u", EVICT_THR);
    pthread_mutex_unlock(&evict_thr_mutex);
    return Status::OK;
}

Status MemoryConfigServiceImpl::GetEvictThreshold(ServerContext* context, const GetEvictThresholdRequest* request, EvictThresholdResponse* response) {
    pthread_mutex_lock(&evict_thr_mutex);
    response->set_threshold(EVICT_THR);
    response->set_success(true);
    pthread_mutex_unlock(&evict_thr_mutex);
    return Status::OK;
}

// 配置服务线程函数
void* config_service_thread(void* arg) {
    string server_address = *(string*)arg;
    MemoryConfigServiceImpl service;

    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    config_server = builder.BuildAndStart();
    log_info(stderr, "Memory config service listening on %s", server_address.c_str());

    config_server->Wait();
    return NULL;
}

void *space_manager(void *arg) {
    while (true) {
        sem_wait(&space_manager_rq.full);
        pthread_mutex_lock(&space_manager_rq.mutex);
        struct ibv_wc wc;
        wc = space_manager_rq.wc_queue.front();
        space_manager_rq.wc_queue.pop();
        pthread_mutex_unlock(&space_manager_rq.mutex);

        sem_wait(&space_allocator.full);
        pthread_mutex_lock(&space_allocator.lock);
        uint64_t free_addr = (uintptr_t)space_allocator.free_addrs.front();
        space_allocator.free_addrs.pop();
        /* trigger buffer eviction in remote memory pool */
        pthread_mutex_lock(&evict_thr_mutex);
        size_t current_evict_thr = EVICT_THR;  // 读取当前值
        pthread_mutex_unlock(&evict_thr_mutex);

        if (space_allocator.free_addrs.size() < current_evict_thr) {
            pthread_cond_signal(&space_allocator.cv_below);
        }
        pthread_mutex_unlock(&space_allocator.lock);

        char *msg_ptr = (char *)wc.wr_id;
        bzero(msg_ptr, m_config_info.ctrl_msg_size);
        memcpy(msg_ptr, &free_addr, sizeof(uint64_t));  // assume all servers have the same endianness, otherwise use htonll()
        msg_ptr += sizeof(uint64_t);
        uint32_t index = ((char *)free_addr - m_ib_info.ib_data_buf) / m_config_info.data_msg_size;
        memcpy(msg_ptr, &index, sizeof(uint32_t));
        msg_ptr = (char *)wc.wr_id;

        int qp_idx = m_ib_info.qp_num_to_idx[wc.qp_num];
        post_send(m_config_info.ctrl_msg_size, m_ib_info.mr_control->lkey, wc.wr_id, 0,
                  m_ib_info.qp[qp_idx], msg_ptr, __func__, to_string(__LINE__));
        log_debug(stderr, "space manager: buffer addr 0x%lx is allocated to compute nodes.", free_addr);
    }
}

void *eviction_manager(void *arg) {
    /* set up grpc client */
    KVStableClient client(channel_ptr);

    while (true) {
        pthread_mutex_lock(&space_allocator.lock);
        pthread_mutex_lock(&evict_thr_mutex);
        size_t current_evict_thr = EVICT_THR;  // 读取当前值
        pthread_mutex_unlock(&evict_thr_mutex);

        while (space_allocator.free_addrs.size() >= current_evict_thr) {
            pthread_cond_wait(&space_allocator.cv_below, &space_allocator.lock);
            pthread_mutex_lock(&evict_thr_mutex);
            current_evict_thr = EVICT_THR;
            pthread_mutex_unlock(&evict_thr_mutex);
        }
        pthread_mutex_unlock(&space_allocator.lock);

        /* run the buffer eviction procedure */
        pthread_mutex_lock(&m_ib_info.bg_buf_lock);
        /* notify all CNs to collect LRU keys */
        log_debug(stderr, "eviction manager: buffer eviction procedure is initiated. notifying CNs...");
        for (int i = 0; i < m_config_info.num_compute_servers; i++) {
            char *bg_buf = m_ib_info.ib_bg_buf + i * m_config_info.bg_msg_size;
            bzero(bg_buf, m_config_info.bg_msg_size);
            memcpy(bg_buf, EVICT_MSG, CTL_MSG_TYPE_SIZE);
            post_send(CTL_MSG_TYPE_SIZE, m_ib_info.mr_bg->lkey, (uintptr_t)bg_buf, 0, m_ib_info.bg_qp[i], bg_buf,
                      __func__, to_string(__LINE__));
            post_recv(m_config_info.bg_msg_size, m_ib_info.mr_bg->lkey, (uintptr_t)bg_buf, m_ib_info.bg_qp[i], bg_buf,
                      __func__, to_string(__LINE__));
        }

        wait_completion(m_ib_info.comp_channel, m_ib_info.bg_cq, IBV_WC_RECV, m_config_info.num_compute_servers, __LINE__);
        log_debug(stderr, "eviction manager: received lru keys from CNs. locking the corresponding entries in control plane...");

        set<string> lru_keys;
        for (int i = 0; i < m_config_info.num_compute_servers; i++) {
            char *read_buf = m_ib_info.ib_bg_buf + i * m_config_info.bg_msg_size;

            uint32_t num;
            memcpy(&num, read_buf, sizeof(uint32_t));
            read_buf += sizeof(uint32_t);

            for (int j = 0; j < num; j++) {
                uint32_t key_len;
                memcpy(&key_len, read_buf, sizeof(uint32_t));
                read_buf += sizeof(uint32_t);
                string key(read_buf, key_len);
                lru_keys.insert(key);
                read_buf += key_len;
            }
        }

        /* lock the control plane mapping and set invalid flags */
        for (auto it = lru_keys.begin(); it != lru_keys.end(); it++) {
            pthread_rwlock_wrlock(&key_to_addr[*it].rwlock);
            char *ptr = (char *)key_to_addr[*it].ptr_next;
            ptr += sizeof(uint64_t) * 2;
            uint8_t invalid = 1;
            memcpy(ptr, &invalid, sizeof(uint8_t));
        }

        /* write the latest version in chain to SSTables */
        log_debug(stderr, "eviction manager: begin writing to SSTables on remote storage.");
        client.write_sstables(lru_keys);
        log_debug(stderr, "eviction manager: finished writing to SSTables on remote storage.");

        /* delete the evicted keys in control plane */
        for (auto it = lru_keys.begin(); it != lru_keys.end(); it++) {
            key_to_addr[*it].in_memory = false;
        }

        /* invalidate cached cursors and data buffers */
        log_debug(stderr, "eviction manager: begin invalidation.");
        char *write_buf = m_ib_info.ib_bg_buf;
        memcpy(write_buf, INVAL_MSG, CTL_MSG_TYPE_SIZE);
        write_buf += CTL_MSG_TYPE_SIZE;
        uint32_t num, key_len;
        uint64_t r_addr;
        num = lru_keys.size();
        memcpy(write_buf, &num, sizeof(uint32_t));
        write_buf += sizeof(uint32_t);
        for (auto it = lru_keys.begin(); it != lru_keys.end(); it++) {
            key_len = it->length();
            memcpy(write_buf, &key_len, sizeof(uint32_t));
            write_buf += sizeof(uint32_t);
            memcpy(write_buf, it->c_str(), key_len);
            write_buf += key_len;
            r_addr = key_to_addr[*it].ptr_next;
            memcpy(write_buf, &r_addr, sizeof(uint64_t));
            write_buf += sizeof(uint64_t);
        }
        post_send(m_config_info.bg_msg_size, m_ib_info.mr_bg->lkey, (uintptr_t)m_ib_info.ib_bg_buf, 0,
                  m_ib_info.bg_qp[0], m_ib_info.ib_bg_buf, __func__, to_string(__LINE__));
        post_recv(m_config_info.bg_msg_size, m_ib_info.mr_bg->lkey, (uintptr_t)m_ib_info.ib_bg_buf,
                  m_ib_info.bg_qp[0], m_ib_info.ib_bg_buf, __func__, to_string(__LINE__));
        for (int i = 1; i < m_config_info.num_compute_servers; i++) {
            write_buf = m_ib_info.ib_bg_buf + i * m_config_info.bg_msg_size;
            memcpy(write_buf, m_ib_info.ib_bg_buf, m_config_info.bg_msg_size);
            post_send(m_config_info.bg_msg_size, m_ib_info.mr_bg->lkey, (uintptr_t)write_buf, 0,
                      m_ib_info.bg_qp[i], write_buf, __func__, to_string(__LINE__));
            post_recv(m_config_info.bg_msg_size, m_ib_info.mr_bg->lkey, (uintptr_t)write_buf,
                      m_ib_info.bg_qp[i], write_buf, __func__, to_string(__LINE__));
        }

        wait_completion(m_ib_info.comp_channel, m_ib_info.bg_cq, IBV_WC_RECV, m_config_info.num_compute_servers, __LINE__);

        /* mark the evicted buffers as free */
        for (int i = 0; i < m_config_info.num_compute_servers; i++) {
            char *read_buf = m_ib_info.ib_bg_buf + i * m_config_info.bg_msg_size;
            if (strncmp(read_buf, INVAL_COMP_MSG, CTL_MSG_TYPE_SIZE) != 0) {
                log_err("Failed to receive invalidation ack.");
            }
        }
        pthread_mutex_unlock(&m_ib_info.bg_buf_lock);

        log_debug(stderr, "eviction manager: successfully received invalidation acks.");

        for (auto it = lru_keys.begin(); it != lru_keys.end(); it++) {
            pthread_rwlock_unlock(&key_to_addr[*it].rwlock);
            pthread_mutex_lock(&space_allocator.lock);
            char *free_addr = (char *)key_to_addr[*it].ptr_next;
            space_allocator.free_addrs.push(free_addr);
            pthread_mutex_unlock(&space_allocator.lock);
            sem_post(&space_allocator.full);
        }
        log_debug(stderr, "eviction manager: buffer eviction procedure is completed.");
    }
}

void *gc_manager(void *arg) {
    while (true) {
        pthread_mutex_lock(&gc.lock);
        while (gc.to_gc_queue.size() < GC_THR) {
            pthread_cond_wait(&gc.cv_above, &gc.lock);
        }

        set<string> gc_keys;
        set<char *> gc_addrs;
        for (int i = 0; i < GC_THR; i++) {
            gc_keys.insert(gc.to_gc_queue.front().key);
            gc_addrs.insert(gc.to_gc_queue.front().addr);
            gc.to_gc_queue.pop();
        }

        pthread_mutex_unlock(&gc.lock);

        log_debug(stderr, "gc manager: garbage collection procedure is initiated.");

        /* invalidate keys that corresponds to the recycled buffer */
        pthread_mutex_lock(&m_ib_info.bg_buf_lock);
        char *write_buf = m_ib_info.ib_bg_buf;
        memcpy(write_buf, GC_MSG, CTL_MSG_TYPE_SIZE);
        write_buf += CTL_MSG_TYPE_SIZE;
        uint32_t num, key_len;
        num = gc_keys.size();
        memcpy(write_buf, &num, sizeof(uint32_t));
        write_buf += sizeof(uint32_t);
        for (auto it = gc_keys.begin(); it != gc_keys.end(); it++) {
            key_len = it->length();
            memcpy(write_buf, &key_len, sizeof(uint32_t));
            write_buf += sizeof(uint32_t);
            memcpy(write_buf, it->c_str(), key_len);
            write_buf += key_len;
        }
        post_send(m_config_info.bg_msg_size, m_ib_info.mr_bg->lkey, (uintptr_t)m_ib_info.ib_bg_buf, 0,
                  m_ib_info.bg_qp[0], m_ib_info.ib_bg_buf, __func__, to_string(__LINE__));
        post_recv(m_config_info.bg_msg_size, m_ib_info.mr_bg->lkey, (uintptr_t)m_ib_info.ib_bg_buf,
                  m_ib_info.bg_qp[0], m_ib_info.ib_bg_buf, __func__, to_string(__LINE__));
        for (int i = 1; i < m_config_info.num_compute_servers; i++) {
            write_buf = m_ib_info.ib_bg_buf + i * m_config_info.bg_msg_size;
            memcpy(write_buf, m_ib_info.ib_bg_buf, m_config_info.bg_msg_size);
            post_send(m_config_info.bg_msg_size, m_ib_info.mr_bg->lkey, (uintptr_t)write_buf, 0,
                      m_ib_info.bg_qp[i], write_buf, __func__, to_string(__LINE__));
            post_recv(m_config_info.bg_msg_size, m_ib_info.mr_bg->lkey, (uintptr_t)write_buf,
                      m_ib_info.bg_qp[i], write_buf, __func__, to_string(__LINE__));
        }

        wait_completion(m_ib_info.comp_channel, m_ib_info.bg_cq, IBV_WC_RECV, m_config_info.num_compute_servers, __LINE__);

        /* mark the buffers as free */
        for (int i = 0; i < m_config_info.num_compute_servers; i++) {
            char *read_buf = m_ib_info.ib_bg_buf + i * m_config_info.bg_msg_size;
            if (strncmp(read_buf, GC_COMP_MSG, CTL_MSG_TYPE_SIZE) != 0) {
                log_err("Failed to receive garbage collection ack.");
            }
        }
        pthread_mutex_unlock(&m_ib_info.bg_buf_lock);

        log_debug(stderr, "gc manager: successfully received invalidation acks.");

        for (auto it = gc_addrs.begin(); it != gc_addrs.end(); it++) {
            pthread_mutex_lock(&space_allocator.lock);
            space_allocator.free_addrs.push(*it);
            pthread_mutex_unlock(&space_allocator.lock);
            sem_post(&space_allocator.full);
        }
        log_debug(stderr, "gc manager: garbage collection procedure is completed.");
    }
}

void *address_manager(void *arg) {
    while (true) {
        sem_wait(&address_manager_rq.full);
        pthread_mutex_lock(&address_manager_rq.mutex);
        struct ibv_wc wc;
        wc = address_manager_rq.wc_queue.front();
        address_manager_rq.wc_queue.pop();
        pthread_mutex_unlock(&address_manager_rq.mutex);

        char *msg_ptr = (char *)wc.wr_id;
        msg_ptr += CTL_MSG_TYPE_SIZE;
        uint32_t key_len;
        memcpy(&key_len, msg_ptr, sizeof(uint32_t));
        msg_ptr += sizeof(uint32_t);
        string key(msg_ptr, key_len);

        msg_ptr = (char *)wc.wr_id;
        bzero(msg_ptr, m_config_info.ctrl_msg_size);
        if (pthread_rwlock_tryrdlock(&key_to_addr[key].rwlock) == 0) {
            if (key_to_addr[key].in_memory) {
                memcpy(msg_ptr, FOUND_MSG, CTL_MSG_TYPE_SIZE);
                msg_ptr += CTL_MSG_TYPE_SIZE;
                memcpy(msg_ptr, &key_to_addr[key].ptr_next, sizeof(uint64_t));
                msg_ptr += sizeof(uint64_t);
                memcpy(msg_ptr, &key_to_addr[key].bk_addr, sizeof(uint64_t));
                // TODO: add bookkeeping info for this header, or use the GC version approach
            } else {
                memcpy(msg_ptr, NOT_FOUND_MSG, CTL_MSG_TYPE_SIZE);
            }
            pthread_rwlock_unlock(&key_to_addr[key].rwlock);
        } else {
            /* the key is under eviction now */
            memcpy(msg_ptr, BACKOFF_MSG, CTL_MSG_TYPE_SIZE);
        }

        msg_ptr = (char *)wc.wr_id;
        int qp_idx = m_ib_info.qp_num_to_idx[wc.qp_num];
        post_send(m_config_info.ctrl_msg_size, m_ib_info.mr_control->lkey, wc.wr_id, 0,
                  m_ib_info.qp[qp_idx], msg_ptr, __func__, to_string(__LINE__));
    }
}

void *bookkeeping_agent(void *arg) {
    while (true) {
        sem_wait(&bookkeeping_agent_rq.full);
        pthread_mutex_lock(&bookkeeping_agent_rq.mutex);
        struct ibv_wc wc;
        wc = bookkeeping_agent_rq.wc_queue.front();
        bookkeeping_agent_rq.wc_queue.pop();
        pthread_mutex_unlock(&bookkeeping_agent_rq.mutex);

        uint32_t imm_data = ntohl(wc.imm_data);
        uint64_t offset = imm_data * m_config_info.data_msg_size;
        uint32_t key_len;
        char *data_ptr = m_ib_info.ib_data_buf + offset + sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint8_t);
        memcpy(&key_len, data_ptr, sizeof(uint32_t));
        data_ptr += sizeof(uint32_t);
        string key(data_ptr, key_len);

        char *msg_ptr;
        auto it = key_to_addr.find(key);
        if (it == key_to_addr.end()) {
            /* pre-populate the key value store, no contention */
            key_to_addr[key].ptr_next = (uintptr_t)(m_ib_info.ib_data_buf + offset);
            key_to_addr[key].bk_addr = 0;
            key_to_addr[key].in_memory = true;
            pthread_rwlock_init(&key_to_addr[key].rwlock, NULL);

            msg_ptr = (char *)wc.wr_id;
            bzero(msg_ptr, m_config_info.ctrl_msg_size);
            memcpy(msg_ptr, COMMITTED_MSG, CTL_MSG_TYPE_SIZE);  // return msg to the compute server
        } else {
            pthread_rwlock_wrlock(&it->second.rwlock);
            if (it->second.in_memory) {
                char *old_addr = (char *)it->second.ptr_next;
                uint64_t new_addr = (uintptr_t)(m_ib_info.ib_data_buf + offset);
                char *flag_addr = old_addr + sizeof(uint64_t) + sizeof(uint64_t);
                (*flag_addr) = 1;                                 // set the invalid bit
                memcpy(old_addr, &new_addr, sizeof(uint64_t));  // link the latest buffer to the chain
                (*flag_addr) = 0;
                it->second.ptr_next = new_addr;

                /* retire old buffers in the background */
                GarbageCollector::GCItem item;
                item.addr = old_addr;
                item.key = key;
                pthread_mutex_lock(&gc.lock);
                gc.to_gc_queue.push(item);
                if (gc.to_gc_queue.size() >= GC_THR) {
                    pthread_cond_signal(&gc.cv_above);
                }

                pthread_mutex_unlock(&gc.lock);
            } else {
                it->second.in_memory = true;
                uint64_t new_addr = (uintptr_t)(m_ib_info.ib_data_buf + offset);
                it->second.ptr_next = new_addr;
            }

            pthread_rwlock_unlock(&it->second.rwlock);

            msg_ptr = (char *)wc.wr_id;
            bzero(msg_ptr, m_config_info.ctrl_msg_size);
            memcpy(msg_ptr, COMMITTED_MSG, CTL_MSG_TYPE_SIZE);
            // return msg to the compute server: referencing CNs of second latest version when multiple compute servers
        }
        int qp_idx = m_ib_info.qp_num_to_idx[wc.qp_num];
        post_send(m_config_info.ctrl_msg_size, m_ib_info.mr_control->lkey, wc.wr_id, 0,
                  m_ib_info.qp[qp_idx], msg_ptr, __func__, to_string(__LINE__));
    }
}

int run_server() {
    /* init space allocator */
    size_t offset;
    for (offset = 0; offset < m_config_info.data_slab_size; offset += m_config_info.data_msg_size) {
        space_allocator.free_addrs.push(&m_ib_info.ib_data_buf[offset]);
    }
    sem_init(&space_allocator.full, 0, space_allocator.free_addrs.size());
    log_info(stderr, "main thread: remote memory pool contains %ld free_addrs.", space_allocator.free_addrs.size());
    
    /* spawn agent threads: each agent is single threaded for now. */
    pthread_t tid_bg;
    pthread_create(&tid_bg, NULL, eviction_manager, NULL);
    pthread_detach(tid_bg);
    pthread_t tid_gc;
    pthread_create(&tid_gc, NULL, gc_manager, NULL);
    pthread_detach(tid_gc);

    pthread_t tid[3];
    pthread_create(&tid[0], NULL, space_manager, NULL);
    pthread_detach(tid[0]);
    pthread_create(&tid[1], NULL, address_manager, NULL);
    pthread_detach(tid[1]);
    pthread_create(&tid[2], NULL, bookkeeping_agent, NULL);
    pthread_detach(tid[2]);

    /* main thread polls cq to detect incoming message */
    int num_wc = 20;
    struct ibv_wc *wc;
    wc = (struct ibv_wc *)calloc(num_wc, sizeof(struct ibv_wc));
    while (true) {
        int n = ibv_poll_cq(m_ib_info.cq, num_wc, wc);
        if (n < 0) {
            log_err("main thread: Failed to poll cq.");
        }

        for (int i = 0; i < n; i++) {
            if (wc[i].status != IBV_WC_SUCCESS) {
                if (wc[i].opcode == IBV_WC_SEND) {
                    log_err("main thread: send failed with status %s", ibv_wc_status_str(wc[i].status));
                } else {
                    log_err("main thread: recv failed with status %s", ibv_wc_status_str(wc[i].status));
                }
                continue;
            }

            if (wc[i].opcode == IBV_WC_SEND) {
                char *msg_ptr = (char *)wc[i].wr_id;
                bzero(msg_ptr, m_config_info.ctrl_msg_size);
                post_srq_recv(m_config_info.ctrl_msg_size, m_ib_info.mr_control->lkey, wc[i].wr_id,
                              m_ib_info.srq, msg_ptr, __func__, to_string(__LINE__));
            } else {
                RequestQueue *rq;
                if (wc[i].opcode == IBV_WC_RECV) {
                    /* received a message via send */
                    char *msg_ptr = (char *)wc[i].wr_id;
                    if (strncmp(msg_ptr, ALLOC_MSG, CTL_MSG_TYPE_SIZE) == 0) {
                        rq = &space_manager_rq;
                    } else if (strncmp(msg_ptr, ADDR_MSG, CTL_MSG_TYPE_SIZE) == 0) {
                        rq = &address_manager_rq;
                    }
                } else if (wc[i].opcode == IBV_WC_RECV_RDMA_WITH_IMM) {
                    /* received a message via RDMA write with immediate (sg_list is unused) */
                    rq = &bookkeeping_agent_rq;
                }

                pthread_mutex_lock(&rq->mutex);
                rq->wc_queue.push(wc[i]);
                pthread_mutex_unlock(&rq->mutex);
                sem_post(&rq->full);
            }
        }
    }
}

void KVStableClient::write_sstables(const set<string> &keys) {
    // 这个方法负责将一组键对应的数据从内存写入到持久化存储(SSTable)
    // 接收一个键的集合作为参数，这些键代表要被驱逐到存储层的数据
    
    ClientContext context;
    // 创建一个gRPC客户端上下文对象，用于远程过程调用
    
    EvictedBuffers ev;
    // 创建一个EvictedBuffers消息对象，这是在storage.proto中定义的protobuf消息类型
    // 用于存储要被驱逐的键值对
    
    EvictionResponse rsp;
    // 创建一个EvictionResponse消息对象，用于接收服务器的响应
    
    for (auto it = keys.begin(); it != keys.end(); it++) {
        // 遍历所有需要驱逐的键
        
        string key(*it);
        // 获取当前键的副本
        
        char *ptr = (char *)key_to_addr[*it].ptr_next;
        // 从key_to_addr哈希表中获取该键对应的内存地址
        // ptr_next字段存储了该键的最新版本的数据在内存中的位置
        
        string value(ptr, m_config_info.data_msg_size);
        // 创建一个字符串，包含从ptr指向的地址开始，长度为data_msg_size的数据
        // 这里直接使用了固定的消息大小，而不是根据实际数据大小来定
        
        (*ev.mutable_eviction())[key] = value;
        // 将键值对添加到EvictedBuffers消息的eviction映射字段中
        // mutable_eviction()返回一个可修改的映射引用
    }
    
    Status status = stub_->write_sstables(&context, ev, &rsp);
    // 调用远程存储服务的write_sstables方法，发送驱逐的键值对
    // stub_是KVStable::Stub的实例，通过gRPC通道连接到存储服务器
    
    if (!status.ok()) {
        log_err("gRPC failed with error message: %s.", status.error_message().c_str());
        // 如果gRPC调用失败，记录错误信息
    }
}

int main(int argc, char *argv[]) {
    /* set config info */
    m_config_info.num_compute_servers = 1;
    m_config_info.num_qps_per_server = 64;
    m_config_info.data_msg_size = 4 * 1024;                 // 4KB each data entry
    m_config_info.data_slab_size = 1 * 1024 * 1024 * 1024;  // 1FGB per slab
    m_config_info.ctrl_msg_size = 1 * 1024;                 // 1KB, maximum size of control message
    m_config_info.bg_msg_size = 4 * 1024;                   // 4KB, maximum size of background message
    m_config_info.sock_port = 4711;                         // socket port used by memory server to init RDMA connection
    m_config_info.grpc_endpoint = "localhost:50051";        // address:port of the grpc server
    m_config_info.memory_config_grpc_endpoint = "localhost:50054";   // address:port for the memory config service
    
    int opt;
    string configfile = "config/memory.config";
    while ((opt = getopt(argc, argv, "hc:")) != -1) {
        switch (opt) {
            case 'h':
                fprintf(stderr, "memory server usage:\n");
                fprintf(stderr, "\t-h: print this help message\n");
                fprintf(stderr, "\t-c <path_to_config_file>: path to the configuration file\n");
                exit(0);
            case 'c':
                configfile = string(optarg);
                break;
            default:
                fprintf(stderr, "Invalid option -%c\n", opt);
                exit(1);
                break;
        }
    }

    fstream fs;
    fs.open(configfile, fstream::in);
    for (string line; getline(fs, line);) {
        vector<string> tmp = split(line, "=");
        assert(tmp.size() == 2);
        std::stringstream sstream(tmp[1]);
        if (tmp[0] == "num_compute_servers") {
            sstream >> m_config_info.num_compute_servers;
        } else if (tmp[0] == "num_qps_per_server") {
            sstream >> m_config_info.num_qps_per_server;
        } else if (tmp[0] == "data_msg_size") {
            sstream >> m_config_info.data_msg_size;
        } else if (tmp[0] == "data_slab_size") {
            sstream >> m_config_info.data_slab_size;
        } else if (tmp[0] == "ctrl_msg_size") {
            sstream >> m_config_info.ctrl_msg_size;
        } else if (tmp[0] == "bg_msg_size") {
            sstream >> m_config_info.bg_msg_size;
        } else if (tmp[0] == "sock_port") {
            sstream >> m_config_info.sock_port;
        } else if (tmp[0] == "grpc_endpoint") {
            m_config_info.grpc_endpoint = tmp[1];
        } else if (tmp[0] == "memory_config_grpc_endpoint") {
            m_config_info.memory_config_grpc_endpoint = tmp[1];
        } else if (tmp[0] == "evict_threshold") {
            sstream >> EVICT_THR;
        }  else {
            fprintf(stderr, "Invalid config parameter `%s`.\n", tmp[0].c_str());
            exit(1);
        }
    }
    m_config_info.ctrl_buffer_size = m_config_info.ctrl_msg_size * m_config_info.num_qps_per_server * m_config_info.num_compute_servers;
    m_config_info.bg_buffer_size = m_config_info.bg_msg_size * m_config_info.num_compute_servers;

    /* init logger */
    pthread_mutex_init(&logger_lock, NULL);
    pthread_mutex_init(&evict_thr_mutex, NULL);

    /* set up grpc channel */
    channel_ptr = grpc::CreateChannel(m_config_info.grpc_endpoint, grpc::InsecureChannelCredentials());

    /* start config service thread */
    pthread_t config_tid;
    pthread_create(&config_tid, NULL, config_service_thread, &memory_config_grpc_endpoint);
    pthread_detach(config_tid);

    /* set up RDMA connection with compute servers */
    memory_setup_ib(m_config_info, m_ib_info);

    run_server();
}