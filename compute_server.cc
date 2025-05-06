#include "compute_server.h"

#include <assert.h>
#include <signal.h>

#include "benchmark.h"
#include "config.h"
#include "leveldb/db.h"
#include "log.h"
#include "setup_ib.h"
#include "utils.h"
#include "statistics.h"

struct CConfigInfo c_config_info;
struct ComputeIBInfo c_ib_info;
CompletionSet C;
ValidationQueue vq;
BlockQueue bq;
RequestQueue rq;
DataCache datacache;
MetaDataCache metadatacache;
shared_ptr<grpc::Channel> storage_channel_ptr;
shared_ptr<grpc::Channel> orderer_channel_ptr;
vector<shared_ptr<grpc::Channel>> compute_channel_ptrs;
pthread_mutex_t logger_lock;
FILE *logger_fp;
volatile int end_flag = 0;
volatile int start_flag = 0;
atomic<long> total_ops = 0;

atomic<long> cache_hit = 0;
atomic<long> sst_count = 0;
atomic<long> cache_total = 0;
atomic<long> abort_count = 0;
atomic_bool prepopulation_completed(false);
atomic_bool warmup_completed(false);

leveldb::DB *db;
leveldb::Options options;

/* read from the disaggregated key value store */
string kv_get(struct ThreadContext &ctx, KVStableClient &client, const string &key) {
    cache_total++;
    /* find cursor of this key */
    pthread_mutex_lock(&metadatacache.hashtable_lock);
    uint64_t ptr_next = 0;
    auto it = metadatacache.key_hashtable.find(key);
    if (it != metadatacache.key_hashtable.end()) {
        ptr_next = it->second->ptr_next;
        MetaDataCache::EntryHeader h = *(it->second);
        h.key = it->second->key;
        metadatacache.lru_list.erase(it->second);
        auto it_new = metadatacache.lru_list.insert(metadatacache.lru_list.begin(), h);  // update lru list and pointer in hashtable
        it->second = it_new;
        log_debug(stderr, "kv_get[thread_index = %d, key = %s]: remote address 0x%lx is found in local metadata cache.",
                  ctx.thread_index, key.c_str(), ptr_next);
    }
    pthread_mutex_unlock(&metadatacache.hashtable_lock);

    if (ptr_next == 0) {
        /* ask the control plane */
        char *ctrl_buf = c_ib_info.ib_control_buf + ctx.thread_index * c_config_info.ctrl_msg_size;
        bzero(ctrl_buf, c_config_info.ctrl_msg_size);
        char *write_buf = ctrl_buf;
        memcpy(write_buf, ADDR_MSG, CTL_MSG_TYPE_SIZE);
        write_buf += CTL_MSG_TYPE_SIZE;
        uint32_t key_len = key.length();
        assert(key_len < c_config_info.ctrl_msg_size);
        memcpy(write_buf, &key_len, sizeof(uint32_t));
        write_buf += sizeof(uint32_t);
        memcpy(write_buf, key.c_str(), key_len);

        post_send(c_config_info.ctrl_msg_size, c_ib_info.mr_control->lkey, (uintptr_t)ctrl_buf, 0,
                  ctx.m_qp, ctrl_buf, to_string(ctx.thread_index), to_string(__LINE__));
        post_recv(c_config_info.ctrl_msg_size, c_ib_info.mr_control->lkey, (uintptr_t)ctrl_buf,
                  ctx.m_qp, ctrl_buf, to_string(ctx.thread_index), to_string(__LINE__));

        int ret = poll_completion(ctx.thread_index, ctx.m_cq, IBV_WC_RECV, __LINE__);

        char *read_buf = ctrl_buf;
        if (strncmp(read_buf, BACKOFF_MSG, CTL_MSG_TYPE_SIZE) == 0) {
            /* in evection now */
            log_debug(stderr, "kv_get[thread_index = %d, key = %s]: remote buffer of this key is in eviction, retry.",
                      ctx.thread_index, key.c_str());
            usleep(2000);
            return kv_get(ctx, client, key);
        } else if (strncmp(read_buf, FOUND_MSG, CTL_MSG_TYPE_SIZE) == 0) {
            read_buf += CTL_MSG_TYPE_SIZE;
            memcpy(&ptr_next, read_buf, sizeof(uint64_t));
            /* update local metadata cache */
            struct MetaDataCache::EntryHeader h;
            h.ptr_next = ptr_next;
            h.key = key;
            pthread_mutex_lock(&metadatacache.hashtable_lock);
            auto it_new = metadatacache.lru_list.insert(metadatacache.lru_list.begin(), h);
            metadatacache.key_hashtable[key] = it_new;
            pthread_mutex_unlock(&metadatacache.hashtable_lock);

            log_debug(stderr, "kv_get[thread_index = %d, key = %s]: remote address 0x%lx is found by contacting the control plane.",
                      ctx.thread_index, key.c_str(), ptr_next);
        }
    }

    if (ptr_next != 0) {
        /* have found cursor of this key */
        pthread_mutex_lock(&datacache.hashtable_lock);
        uint64_t local_ptr = 0;
        auto it = datacache.addr_hashtable.find(ptr_next);
        if (it != datacache.addr_hashtable.end()) {
            local_ptr = it->second->l_addr;
            struct DataCache::Frame f = *(it->second);
            datacache.lru_list.erase(it->second);  // update lru list and pointer in hashtable
            auto it_new = datacache.lru_list.insert(datacache.lru_list.begin(), f);
            it->second = it_new;
        }
        pthread_mutex_unlock(&datacache.hashtable_lock);

        if (local_ptr != 0) {
            cache_hit++;
            /* the buffer is cached locally */
            log_debug(stderr, "kv_get[thread_index = %d, key = %s]: found in local data cache (raddr = 0x%lx, laddr = 0x%lx).",
                      ctx.thread_index, key.c_str(), ptr_next, local_ptr);
            unsigned long offset = sizeof(uint64_t) * 2 + sizeof(uint8_t) + sizeof(uint32_t) + key.length();
            char *val_ptr = (char *)local_ptr;
            val_ptr += offset;
            return string(val_ptr, c_config_info.data_msg_size - offset);
        } else {
            /* allocate a buffer in local datacache */
            pthread_mutex_lock(&datacache.hashtable_lock);
            if (datacache.free_addrs.size()) {
                local_ptr = datacache.free_addrs.front();
                datacache.free_addrs.pop();
            } else {
                local_ptr = datacache.lru_list.back().l_addr;
                uint64_t remote_ptr = datacache.lru_list.back().r_addr;
                datacache.lru_list.pop_back();
                auto it = datacache.addr_hashtable.find(remote_ptr);
                if (it != datacache.addr_hashtable.end() && it->second->l_addr == local_ptr) {
                    datacache.addr_hashtable.erase(it);
                }
            }
            pthread_mutex_unlock(&datacache.hashtable_lock);

            /* find the latest version */
            bzero((char *)local_ptr, c_config_info.data_msg_size);
            post_read(c_config_info.data_msg_size, c_ib_info.mr_data->lkey, local_ptr, ctx.m_qp,
                      (char *)local_ptr, ptr_next, c_ib_info.remote_mr_data_rkey, to_string(ctx.thread_index), to_string(__LINE__));
            // chain walk is not needed for M-C topology
            int ret = poll_completion(ctx.thread_index, ctx.m_cq, IBV_WC_RDMA_READ, __LINE__);
            log_debug(stderr, "kv_get[thread_index = %d, key = %s]: finished RDMA read from remote memory pool (raddr = 0x%lx).",
                      ctx.thread_index, key.c_str(), ptr_next);

            uint8_t invalid = 0;
            char *val_ptr = (char *)local_ptr;
            val_ptr += sizeof(uint64_t) * 2;
            memcpy(&invalid, val_ptr, sizeof(uint8_t));

            if (invalid) {
                /* in eviction now */
                log_debug(stderr, "kv_get[thread_index = %d, key = %s]: invalid buffer (raddr = 0x%lx) due to eviction, discard and retry.",
                          ctx.thread_index, key.c_str(), ptr_next);
                datacache.free_addrs.push(local_ptr);
                usleep(500000);
                return kv_get(ctx, client, key);
            } else {
                // uint64_t ptr;
                // memcpy(&ptr, (char *)local_ptr, sizeof(uint64_t));
                // assert(ptr == 0); // not true under contention

                /* update local cache */
                struct DataCache::Frame f = {
                    .l_addr = local_ptr,
                    .r_addr = ptr_next};
                pthread_mutex_lock(&datacache.hashtable_lock);
                auto it_new = datacache.lru_list.insert(datacache.lru_list.begin(), f);
                datacache.addr_hashtable[ptr_next] = it_new;
                pthread_mutex_unlock(&datacache.hashtable_lock);

                unsigned long offset = sizeof(uint64_t) * 2 + sizeof(uint8_t) + sizeof(uint32_t) + key.length();
                val_ptr = (char *)local_ptr;
                val_ptr += offset;
                return string(val_ptr, c_config_info.data_msg_size - offset);
            }
        }
    } else {
        sst_count++;
        /* search in SSTables on the storage server */
        log_debug(stderr, "kv_get[thread_index = %d, key = %s]: read from sstables.", ctx.thread_index, key.c_str());
        string value;
        int ret = client.read_sstables(key, value);

        unsigned long offset = sizeof(uint64_t) * 2 + sizeof(uint8_t) + sizeof(uint32_t) + key.length();
        value = value.substr(offset);
        return value;
    }
}

/* put to the disaggregated key value store */
int kv_put(struct ThreadContext &ctx, vector<ComputeCommClient> &compute_clients, const string &key, const string &value) {
    /* allocate a remote buffer */
    char *ctrl_buf = c_ib_info.ib_control_buf + ctx.thread_index * c_config_info.ctrl_msg_size;
    bzero(ctrl_buf, c_config_info.ctrl_msg_size);
    memcpy(ctrl_buf, ALLOC_MSG, CTL_MSG_TYPE_SIZE);

    post_send(c_config_info.ctrl_msg_size, c_ib_info.mr_control->lkey, (uintptr_t)ctrl_buf, 0,
              ctx.m_qp, ctrl_buf, to_string(ctx.thread_index), to_string(__LINE__));
    post_recv(c_config_info.ctrl_msg_size, c_ib_info.mr_control->lkey, (uintptr_t)ctrl_buf,
              ctx.m_qp, ctrl_buf, to_string(ctx.thread_index), to_string(__LINE__));

    /* allocate a local buffer, write to local buffer */
    pthread_mutex_lock(&datacache.hashtable_lock);
    uint64_t local_ptr;
    if (datacache.free_addrs.size()) {
        local_ptr = datacache.free_addrs.front();
        datacache.free_addrs.pop();
    } else {
        local_ptr = datacache.lru_list.back().l_addr;
        uint64_t remote_ptr = datacache.lru_list.back().r_addr;
        datacache.lru_list.pop_back();
        auto it = datacache.addr_hashtable.find(remote_ptr);
        if (it != datacache.addr_hashtable.end() && it->second->l_addr == local_ptr) {
            datacache.addr_hashtable.erase(it);
        }
    }
    pthread_mutex_unlock(&datacache.hashtable_lock);

    char *write_ptr = (char *)local_ptr;
    bzero(write_ptr, c_config_info.data_msg_size);
    write_ptr += sizeof(uint64_t) * 2 + sizeof(uint8_t);
    uint32_t key_len = key.length();
    memcpy(write_ptr, &key_len, sizeof(uint32_t));
    write_ptr += sizeof(uint32_t);
    memcpy(write_ptr, key.c_str(), key_len);
    write_ptr += key_len;
    memcpy(write_ptr, value.c_str(), value.length());

    /* write to remote buffer */
    int ret = poll_completion(ctx.thread_index, ctx.m_cq, IBV_WC_RECV, __LINE__);
    uint64_t remote_ptr;
    char *read_ptr = ctrl_buf;
    memcpy(&remote_ptr, read_ptr, sizeof(uint64_t));
    read_ptr += sizeof(uint64_t);
    uint32_t index;
    memcpy(&index, read_ptr, sizeof(uint32_t));
    log_debug(stderr, "kv_put[thread_index = %d, key = %s]: remote addr 0x%lx is allocated.",
              ctx.thread_index, key.c_str(), remote_ptr);

    post_write_with_imm(c_config_info.data_msg_size, c_ib_info.mr_data->lkey, local_ptr, index, ctx.m_qp,
                        (char *)local_ptr, remote_ptr, c_ib_info.remote_mr_data_rkey, to_string(ctx.thread_index), to_string(__LINE__));
    post_recv(c_config_info.ctrl_msg_size, c_ib_info.mr_control->lkey, (uintptr_t)ctrl_buf,
              ctx.m_qp, ctrl_buf, to_string(ctx.thread_index), to_string(__LINE__));

    /* poll RDMA write completion: write-through cache */
    ret = poll_completion(ctx.thread_index, ctx.m_cq, IBV_WC_RECV, __LINE__);
    if (strncmp(ctrl_buf, COMMITTED_MSG, CTL_MSG_TYPE_SIZE) != 0) {
        log_err("thread[%d]: failed to receive message committed ack.", ctx.thread_index);
        return -1;
    }

    log_debug(stderr, "kv_put[thread_index = %d, key = %s]: finished RDMA write to remote addr 0x%lx, committed.",
              ctx.thread_index, key.c_str(), remote_ptr);

    /* update local caches */
    struct DataCache::Frame f = {
        .l_addr = local_ptr,
        .r_addr = remote_ptr};
    pthread_mutex_lock(&datacache.hashtable_lock);
    auto it_new = datacache.lru_list.insert(datacache.lru_list.begin(), f);
    datacache.addr_hashtable[remote_ptr] = it_new;
    pthread_mutex_unlock(&datacache.hashtable_lock);

    pthread_mutex_lock(&metadatacache.hashtable_lock);
    auto it = metadatacache.key_hashtable.find(key);
    if (it != metadatacache.key_hashtable.end()) {
        metadatacache.lru_list.erase(it->second);
    }
    MetaDataCache::EntryHeader h;
    h.ptr_next = remote_ptr;
    h.key = key;  // the bk_addr field is dummy for now
    auto it_meta = metadatacache.lru_list.insert(metadatacache.lru_list.begin(), h);
    metadatacache.key_hashtable[key] = it_meta;
    pthread_mutex_unlock(&metadatacache.hashtable_lock);
    log_debug(stderr, "kv_put[thread_index = %d, key = %s]: local caches are updated.", ctx.thread_index, key.c_str());

    /* invalidate all CNs' caches including itself */
    // for (int i = 0; i < compute_clients.size(); i++) {
    //     compute_clients[i].invalidate_cn(key);
    // }

    return 0;
}

void *background_handler(void *arg) {
    while (true) {
        /* block wait for incoming message */
        log_debug(stderr, "background handler: listening for background requests.");
        wait_completion(c_ib_info.comp_channel, c_ib_info.bg_cq, IBV_WC_RECV, 1, __LINE__);

        if (strncmp(c_ib_info.ib_bg_buf, EVICT_MSG, CTL_MSG_TYPE_SIZE) == 0) {
            log_debug(stderr, "background handler[eviction]: received buffer eviction request, sending my lru keys.");
            /* send my local LRU keys to the memory server */
            char *write_buf = c_ib_info.ib_bg_buf;
            pthread_mutex_lock(&metadatacache.hashtable_lock);

            uint32_t num;
            num = (metadatacache.lru_list.size() >= LRU_KEY_NUM) ? LRU_KEY_NUM : metadatacache.lru_list.size();
            memcpy(write_buf, &num, sizeof(uint32_t));
            write_buf += sizeof(uint32_t);

            for (int i = 0; i < num; i++) {
                string key = metadatacache.lru_list.back().key;
                metadatacache.lru_list.pop_back();
                metadatacache.key_hashtable.erase(key);
                uint32_t key_len = key.length();
                memcpy(write_buf, &key_len, sizeof(uint32_t));
                write_buf += sizeof(uint32_t);
                memcpy(write_buf, key.c_str(), key_len);
                write_buf += key_len;
            }
            pthread_mutex_unlock(&metadatacache.hashtable_lock);

            post_send(c_config_info.bg_msg_size, c_ib_info.mr_bg->lkey, (uintptr_t)c_ib_info.ib_bg_buf, 0,
                      c_ib_info.bg_qp, c_ib_info.ib_bg_buf, __func__, to_string(__LINE__));
            post_recv(c_config_info.bg_msg_size, c_ib_info.mr_bg->lkey, (uintptr_t)c_ib_info.ib_bg_buf,
                      c_ib_info.bg_qp, c_ib_info.ib_bg_buf, __func__, to_string(__LINE__));

            wait_completion(c_ib_info.comp_channel, c_ib_info.bg_cq, IBV_WC_RECV, 1, __LINE__);

            /* invalidation */
            log_debug(stderr, "background handler[eviction]: received invalidation request, begin invalidation.");
            set<string> keys_to_inval;
            set<uint64_t> r_addrs_to_inval;
            char *read_buf = c_ib_info.ib_bg_buf;
            if (strncmp(read_buf, INVAL_MSG, CTL_MSG_TYPE_SIZE) != 0) {
                log_err("Failed to receive invalidation request.");
            }
            read_buf += CTL_MSG_TYPE_SIZE;
            memcpy(&num, read_buf, sizeof(uint32_t));
            read_buf += sizeof(uint32_t);
            for (int i = 0; i < num; i++) {
                uint32_t key_len;
                memcpy(&key_len, read_buf, sizeof(uint32_t));
                read_buf += sizeof(uint32_t);
                string key_to_inval(read_buf, key_len);
                read_buf += key_len;
                uint64_t r_addr_to_inval;
                memcpy(&r_addr_to_inval, read_buf, sizeof(uint64_t));
                read_buf += sizeof(uint64_t);

                keys_to_inval.insert(key_to_inval);
                r_addrs_to_inval.insert(r_addr_to_inval);
            }

            pthread_mutex_lock(&metadatacache.hashtable_lock);
            for (auto it = keys_to_inval.begin(); it != keys_to_inval.end(); it++) {
                auto it_meta = metadatacache.key_hashtable.find(*it);
                if (it_meta != metadatacache.key_hashtable.end()) {
                    auto it_lru = it_meta->second;
                    metadatacache.lru_list.erase(it_lru);
                    metadatacache.key_hashtable.erase(it_meta);
                }
            }
            pthread_mutex_unlock(&metadatacache.hashtable_lock);

            pthread_mutex_lock(&datacache.hashtable_lock);
            for (auto it = r_addrs_to_inval.begin(); it != r_addrs_to_inval.end(); it++) {
                auto it_data = datacache.addr_hashtable.find(*it);
                if (it_data != datacache.addr_hashtable.end()) {
                    auto it_lru = it_data->second;
                    uint64_t l_addr = it_lru->l_addr;
                    datacache.free_addrs.push(l_addr);
                    datacache.lru_list.erase(it_lru);
                    datacache.addr_hashtable.erase(it_data);
                }
            }
            pthread_mutex_unlock(&datacache.hashtable_lock);

            log_debug(stderr, "background handler[eviction]: finished invalidating local caches, sending ack.");

            write_buf = c_ib_info.ib_bg_buf;
            bzero(write_buf, c_config_info.bg_msg_size);
            memcpy(write_buf, INVAL_COMP_MSG, CTL_MSG_TYPE_SIZE);
            post_send(CTL_MSG_TYPE_SIZE, c_ib_info.mr_bg->lkey, (uintptr_t)c_ib_info.ib_bg_buf, 0,
                      c_ib_info.bg_qp, c_ib_info.ib_bg_buf, __func__, to_string(__LINE__));
            post_recv(c_config_info.bg_msg_size, c_ib_info.mr_bg->lkey, (uintptr_t)c_ib_info.ib_bg_buf,
                      c_ib_info.bg_qp, c_ib_info.ib_bg_buf, __func__, to_string(__LINE__));
        } else if (strncmp(c_ib_info.ib_bg_buf, GC_MSG, CTL_MSG_TYPE_SIZE) == 0) {
            log_debug(stderr, "background handler[gc]: received gc request, begin invalidation.");
            /* GC: invalidate the GCed keys */
            set<string> keys_to_inval;
            char *read_buf = c_ib_info.ib_bg_buf;
            if (strncmp(read_buf, GC_MSG, CTL_MSG_TYPE_SIZE) != 0) {
                log_err("Failed to receive gc request.");
            }
            read_buf += CTL_MSG_TYPE_SIZE;
            uint32_t num;
            memcpy(&num, read_buf, sizeof(uint32_t));
            read_buf += sizeof(uint32_t);
            for (int i = 0; i < num; i++) {
                uint32_t key_len;
                memcpy(&key_len, read_buf, sizeof(uint32_t));
                read_buf += sizeof(uint32_t);
                string key_to_inval(read_buf, key_len);
                read_buf += key_len;

                keys_to_inval.insert(key_to_inval);
            }

            pthread_mutex_lock(&metadatacache.hashtable_lock);
            for (auto it = keys_to_inval.begin(); it != keys_to_inval.end(); it++) {
                auto it_meta = metadatacache.key_hashtable.find(*it);
                if (it_meta != metadatacache.key_hashtable.end()) {
                    auto it_lru = it_meta->second;
                    metadatacache.lru_list.erase(it_lru);
                    metadatacache.key_hashtable.erase(it_meta);
                }
            }
            pthread_mutex_unlock(&metadatacache.hashtable_lock);

            log_debug(stderr, "background handler[gc]: finished invalidating local caches, sending ack.");

            char *write_buf = c_ib_info.ib_bg_buf;
            bzero(write_buf, c_config_info.bg_msg_size);
            memcpy(write_buf, GC_COMP_MSG, CTL_MSG_TYPE_SIZE);
            post_send(CTL_MSG_TYPE_SIZE, c_ib_info.mr_bg->lkey, (uintptr_t)c_ib_info.ib_bg_buf, 0,
                      c_ib_info.bg_qp, c_ib_info.ib_bg_buf, __func__, to_string(__LINE__));
            post_recv(c_config_info.bg_msg_size, c_ib_info.mr_bg->lkey, (uintptr_t)c_ib_info.ib_bg_buf,
                      c_ib_info.bg_qp, c_ib_info.ib_bg_buf, __func__, to_string(__LINE__));
        }
    }
}

/* wrappers for the kv interface, to be used within the smart contracts */
string s_kv_get(struct ThreadContext &ctx, KVStableClient &client, const string &key, Endorsement &endorsement) {
    string value;
    value = kv_get(ctx, client, key);
    // leveldb::Status s = db->Get(leveldb::ReadOptions(), key, &value);

    uint64_t read_version_blockid = 0;
    uint64_t read_version_transid = 0;
    memcpy(&read_version_blockid, value.c_str(), sizeof(uint64_t));
    memcpy(&read_version_transid, value.c_str() + sizeof(uint64_t), sizeof(uint64_t));

    ReadItem *read_item = endorsement.add_read_set();
    read_item->set_read_key(key);
    read_item->set_block_seq_num(read_version_blockid);
    read_item->set_trans_seq_num(read_version_transid);

    /* the value returned to smart contracts should not contain version numbers */
    value.erase(0, 16);
    uint64_t data;
    memcpy(&data, value.c_str(), sizeof(uint64_t));

    log_debug(logger_fp,
              "s_kv_get[thread_index = %d, key = %s]:\n"
              "version [block_id = %ld, trans_id = %ld] is stored in read set.\n"
              "value [%ld] is returned to smart contract.\n",
              ctx.thread_index, key.c_str(), read_item->block_seq_num(), read_item->trans_seq_num(), data);

    return value;
}

int s_kv_put(const string &key, const string &value, Endorsement &endorsement) {
    WriteItem *write_item = endorsement.add_write_set();
    write_item->set_write_key(key);
    write_item->set_write_value(value);

    return 0;
}

/* simulate smart contract (X stage) */
void *simulation_handler(void *arg) {
    struct ThreadContext *ctx = (struct ThreadContext *)arg;
    assert(ctx->m_cq != NULL);
    assert(ctx->m_qp != NULL);
    /* set up grpc client for storage server */
    KVStableClient storage_client(storage_channel_ptr);
    /* set up grpc client for ordering service */
    unique_ptr<ConsensusComm::Stub> orderer_stub(ConsensusComm::NewStub(orderer_channel_ptr));
    /* set up grpc client for other compute servers (dummy clients here) */
    vector<ComputeCommClient> compute_clients;

    ClientContext context;
    google::protobuf::Empty rsp;
    CompletionQueue cq;
    unique_ptr<ClientAsyncWriter<Endorsement>> orderer_stream(orderer_stub->Asyncsend_to_leader_stream(&context, &rsp, &cq, (void *)1));
    bool ok;
    void *got_tag;
    cq.Next(&got_tag, &ok);

    default_random_engine generator;
    uniform_int_distribution<int> distribution(0, BANK_KEY_NUM - 1);

    char *buf = (char *)malloc(c_config_info.data_msg_size);
    long local_ops = 0;

    pthread_t my_tid = pthread_self();
    printf("My thread ID is %lu\n", (unsigned long)my_tid);

    while (!ctx->end_flag) {
        sem_wait(&rq.full);
        pthread_mutex_lock(&rq.mutex);
        struct Request proposal;
        proposal = rq.rq_queue.front();
        rq.rq_queue.pop();
        pthread_mutex_unlock(&rq.mutex);

        // ClientContext context;
        // google::protobuf::Empty rsp;
        Endorsement endorsement;

        size_t meta_data_size = sizeof(uint64_t) * 2 + sizeof(uint8_t) + sizeof(uint32_t) + proposal.key.length() + 2 * sizeof(uint64_t);
        // size_t meta_data_size = 2 * sizeof(uint64_t);

        /* the smart contract */
        if (proposal.type == Request::Type::GET) {
            // string value;
            // value = kv_get(ctx, storage_client, proposal.key);
            // storage_client.read_sstables(proposal.key, value);
            // leveldb::Status s = db->Get(leveldb::ReadOptions(), proposal.key, &value);
            // log_debug(logger_fp, "thread_index = #%d\trequest_type = GET\nget_key = %s\nget_value = %s\nget_size = %ld\n",
            //           ctx.thread_index, proposal.key.c_str(), value.c_str(), value.size());
            // local_ops++;

            s_kv_get(*ctx, storage_client, proposal.key, endorsement);
        } else if (proposal.type == Request::Type::PUT) {
            // int ret = kv_put(ctx, proposal.key, proposal.value);
            // int ret = storage_client.write_sstables(proposal.key, proposal.value);
            // bzero(buf, 1024 * 10);
            // strcpy(buf, proposal.value.c_str());
            // string value(buf, 1024 * 10);

            // leveldb::Status s = db->Put(leveldb::WriteOptions(), proposal.key, value);
            // int ret = 0;
            // if (!s.ok()) {
            //     ret = 1;
            // }
            // if (!ret) {
            //     log_debug(logger_fp, "thread_index = #%d\trequest_type = PUT\nput_key = %s\nput_value = %s\n",
            //               ctx.thread_index, proposal.key.c_str(), proposal.value.c_str());
            //     if (!proposal.is_prep) {
            //         local_ops++;
            //     }
            // } else {
            //     log_debug(logger_fp, "thread_index = #%d\trequest_type = PUT\nput_key = %s\noperation failed...\n",
            //               ctx.thread_index, proposal.key.c_str());
            // }

            uint64_t put_value;
            memcpy(&put_value, proposal.value.c_str(), sizeof(uint64_t));
            log_debug(logger_fp, "thread_index = #%d\trequest_type = PUT\nput_key = %s\nput_value = %ld\n",
                      ctx->thread_index, proposal.key.c_str(), put_value);
            bzero(buf, c_config_info.data_msg_size);
            strcpy(buf, proposal.value.c_str());
            proposal.value.assign(buf, c_config_info.data_msg_size - meta_data_size);
            /* if it is prepopulation */
            if (proposal.is_prep) {
                uint64_t curr_version_blockid = 0;
                uint64_t curr_version_transid = 0;
                char *ver = (char *)malloc(2 * sizeof(uint64_t));
                bzero(ver, 2 * sizeof(uint64_t));
                memcpy(ver, &curr_version_blockid, sizeof(uint64_t));
                memcpy(ver + sizeof(uint64_t), &curr_version_transid, sizeof(uint64_t));
                string value(ver, 2 * sizeof(uint64_t));
                value += proposal.value;

                int ret = kv_put(*ctx, compute_clients, proposal.key, value);
                // leveldb::Status s = db->Put(leveldb::WriteOptions(), proposal.key, value);
                // int ret = 0;
                // if (!s.ok()) {
                //     ret = 1;
                // }
                if (!ret) {
                    // uint64_t put_value;
                    // memcpy(&put_value, proposal.value.c_str(), sizeof(uint64_t));
                    // log_debug(logger_fp, "thread_index = #%d\trequest_type = PUT\nput_key = %s\nput_value = %ld\n",
                    //           ctx.thread_index, proposal.key.c_str(), put_value);
                } else {
                    log_debug(logger_fp, "thread_index = #%d\trequest_type = PUT\nput_key = %s\noperation failed...\n",
                              ctx->thread_index, proposal.key.c_str());
                }
                continue;
            } else {
                s_kv_put(proposal.key, proposal.value, endorsement);
            }
        } else if (proposal.type == Request::Type::KMEANS) {
            default_random_engine generator;
            uniform_int_distribution<int> distribution(0, KMEANS_KEY_NUM - 1);

            vector<int> A;
            int num_keys_per_trans = 60;
            for (int i = 0; i < num_keys_per_trans; i++) {
                int random_number = distribution(generator);
                string key = "key_k_" + to_string(random_number);
                string value = s_kv_get(*ctx, storage_client, key, endorsement);
                uint64_t data;
                memcpy(&data, value.c_str(), sizeof(uint64_t));
                A.push_back(data);
            }
            kmeans(A, 20);
        } else if (proposal.type == Request::Type::TransactSavings) {
            int user_id = distribution(generator);
            string key = "saving_" + to_string(user_id);
            string value = s_kv_get(*ctx, storage_client, key, endorsement);
            uint64_t balance;
            memcpy(&balance, value.c_str(), sizeof(uint64_t));
            balance += 1000;

            s_kv_put(key, get_balance_str(balance, c_config_info.data_msg_size - meta_data_size), endorsement);
            // log_info(stderr, "[W]TransactSavings: user_id = %d.", user_id);
        } else if (proposal.type == Request::Type::DepositChecking) {
            int user_id = distribution(generator);
            string key = "checking_" + to_string(user_id);
            string value = s_kv_get(*ctx, storage_client, key, endorsement);
            uint64_t balance;
            memcpy(&balance, value.c_str(), sizeof(uint64_t));
            balance += 1000;

            s_kv_put(key, get_balance_str(balance, c_config_info.data_msg_size - meta_data_size), endorsement);
            // log_info(stderr, "[W]DepositChecking: user_id = %d.", user_id);
        } else if (proposal.type == Request::Type::SendPayment) {
            int sender_id = distribution(generator);
            int receiver_id = distribution(generator);
            string sender_key = "checking_" + to_string(sender_id);
            string receiver_key = "checking_" + to_string(receiver_id);

            string sender_value = s_kv_get(*ctx, storage_client, sender_key, endorsement);
            string receiver_value = s_kv_get(*ctx, storage_client, receiver_key, endorsement);
            uint64_t sender_balance, receiver_balance;
            memcpy(&sender_balance, sender_value.c_str(), sizeof(uint64_t));
            memcpy(&receiver_balance, receiver_value.c_str(), sizeof(uint64_t));
            if (sender_balance >= 5) {
                sender_balance -= 5;
                receiver_balance += 5;

                s_kv_put(sender_key, get_balance_str(sender_balance, c_config_info.data_msg_size - meta_data_size), endorsement);
                s_kv_put(receiver_key, get_balance_str(receiver_balance, c_config_info.data_msg_size - meta_data_size), endorsement);
                // log_info(stderr, "[W]SendPayment: sender_id = %d, receiver_id = %d.", sender_id, receiver_id);
            }
        } else if (proposal.type == Request::Type::WriteCheck) {
            int user_id = distribution(generator);
            string key = "checking_" + to_string(user_id);
            string value = s_kv_get(*ctx, storage_client, key, endorsement);
            uint64_t balance;
            memcpy(&balance, value.c_str(), sizeof(uint64_t));
            if (balance >= 100) {
                balance -= 100;
                s_kv_put(key, get_balance_str(balance, c_config_info.data_msg_size - meta_data_size), endorsement);
                // log_info(stderr, "[W]WriteCheck: user_id = %d.", user_id);
            }
        } else if (proposal.type == Request::Type::Amalgamate) {
            int user_id = distribution(generator);
            string checking_key = "checking_" + to_string(user_id);
            string saving_key = "saving_" + to_string(user_id);

            string checking_value = s_kv_get(*ctx, storage_client, checking_key, endorsement);
            string saving_value = s_kv_get(*ctx, storage_client, saving_key, endorsement);
            uint64_t checking_balance, saving_balance;
            memcpy(&checking_balance, checking_value.c_str(), sizeof(uint64_t));
            memcpy(&saving_balance, saving_value.c_str(), sizeof(uint64_t));
            checking_balance = checking_balance + saving_balance;
            saving_balance = 0;

            s_kv_put(checking_key, get_balance_str(checking_balance, c_config_info.data_msg_size - meta_data_size), endorsement);
            s_kv_put(saving_key, get_balance_str(saving_balance, c_config_info.data_msg_size - meta_data_size), endorsement);
            // log_info(stderr, "[W]Amalgamate: user_id = %d.", user_id);
        } else if (proposal.type == Request::Type::Query) {
            int user_id = distribution(generator);
            string checking_key = "checking_" + to_string(user_id);
            string saving_key = "saving_" + to_string(user_id);

            string checking_value = s_kv_get(*ctx, storage_client, checking_key, endorsement);
            string saving_value = s_kv_get(*ctx, storage_client, saving_key, endorsement);
            // log_info(stderr, "[R]Query: user_id = %d.", user_id);
        }

        /* send the generated endorsement to the client/orderer */
        orderer_stream->Write(endorsement, (void *)1);
        bool ok;
        void *got_tag;
        // cq.AsyncNext(&got_tag, &ok, gpr_time_0(GPR_CLOCK_REALTIME));
        cq.Next(&got_tag, &ok);

        // orderer_stub->async()->send_to_leader(&context, &endorsement, &rsp, [](Status s) {});
        // Status status = orderer_stub->send_to_leader(&context, endorsement, &rsp);
        // if (!status.ok()) {
        //     log_err("gRPC failed with error message: %s.", status.error_message().c_str());
        // }

        // total_ops++;
        if (ctx->end_flag) {
            log_info(stderr, "thread_index = %d: end_flag is %d", ctx->thread_index,ctx->end_flag);   
        }
    }
    free(buf);
    return NULL;
}

/* validate and commit transactions (V stage) */
bool validate_transaction(struct ThreadContext &ctx, KVStableClient &storage_client, vector<ComputeCommClient> &compute_clients,
                          uint64_t block_id, uint64_t trans_id, const Endorsement &transaction) {
    // log_debug(logger_fp, "******validating transaction[block_id = %ld, trans_id = %ld, thread_id = %d]******",
    //           block_id, trans_id, ctx.thread_index);
    bool is_valid = true;

    for (int read_id = 0; read_id < transaction.read_set_size(); read_id++) {
        uint64_t curr_version_blockid = 0;
        uint64_t curr_version_transid = 0;

        string value;
        value = kv_get(ctx, storage_client, transaction.read_set(read_id).read_key());
        // leveldb::Status s = db->Get(leveldb::ReadOptions(), transaction.read_set(read_id).read_key(), &value);
        memcpy(&curr_version_blockid, value.c_str(), sizeof(uint64_t));
        memcpy(&curr_version_transid, value.c_str() + sizeof(uint64_t), sizeof(uint64_t));

        // log_debug(logger_fp,
        //           "read_key = %s\nstored_read_version = [block_id = %ld, trans_id = %ld]\n"
        //           "current_key_version = [block_id = %ld, trans_id = %ld]",
        //           transaction.read_set(read_id).read_key().c_str(),
        //           transaction.read_set(read_id).block_seq_num(),
        //           transaction.read_set(read_id).trans_seq_num(),
        //           curr_version_blockid, curr_version_transid);

        if (curr_version_blockid != transaction.read_set(read_id).block_seq_num() ||
            curr_version_transid != transaction.read_set(read_id).trans_seq_num()) {
            is_valid = false;
            break;
        }
    }

    if (is_valid) {
        uint64_t curr_version_blockid = block_id;
        char *ver = (char *)malloc(2 * sizeof(uint64_t));
        for (int write_id = 0; write_id < transaction.write_set_size(); write_id++) {
            uint64_t curr_version_transid = trans_id;

            bzero(ver, 2 * sizeof(uint64_t));
            memcpy(ver, &curr_version_blockid, sizeof(uint64_t));
            memcpy(ver + sizeof(uint64_t), &curr_version_transid, sizeof(uint64_t));
            string value(ver, 2 * sizeof(uint64_t));
            value += transaction.write_set(write_id).write_value();

            int ret = kv_put(ctx, compute_clients, transaction.write_set(write_id).write_key(), value);
            // leveldb::Status s = db->Put(leveldb::WriteOptions(), transaction.write_set(write_id).write_key(), value);

            // log_debug(logger_fp, "write_key = %s\nwrite_value = %s",
            //           transaction.write_set(write_id).write_key().c_str(),
            //           transaction.write_set(write_id).write_value().c_str());
        }
        free(ver);
        // log_debug(logger_fp, "transaction is committed.\n");

        total_ops++;

    } else {
        abort_count++;

        // log_debug(logger_fp, "transaction is aborted: found stale read version.\n");
    }
    return is_valid;
}

void *validation_handler(void *arg) {
    struct ThreadContext *ctx = (struct ThreadContext *)arg;
    /* set up grpc client for storage server */
    KVStableClient storage_client(storage_channel_ptr);
    string serialised_block;
    /* set up grpc client for other compute servers */
    vector<ComputeCommClient> compute_clients;
    for (int i = 0; i < compute_channel_ptrs.size(); i++) {
        compute_clients.emplace_back(compute_channel_ptrs[i]);
    }

    while (!ctx->end_flag) {
        sem_wait(&bq.full);
        pthread_mutex_lock(&bq.mutex);
        Block block = bq.bq_queue.front();
        // log_info(stderr,"validator extracts block %d", block.block_id());
        bq.bq_queue.pop();
        pthread_mutex_unlock(&bq.mutex);

        for (int trans_id = 0; trans_id < block.transactions_size(); trans_id++) {
            validate_transaction(*ctx, storage_client, compute_clients, block.block_id(), trans_id, block.transactions(trans_id));
        }

        /* store the block with its bit mask in remote block store */
        if (!block.SerializeToString(&serialised_block)) {
            log_err("validator: failed to serialize block.");
        }
        storage_client.write_blocks(serialised_block);
    }

    return NULL;
}

void *parallel_validation_worker(void *arg) {
    struct ThreadContext ctx = *(struct ThreadContext *)arg;
    /* set up grpc client for storage server */
    KVStableClient storage_client(storage_channel_ptr);
    /* set up grpc client for other compute servers */
    vector<ComputeCommClient> compute_clients;
    for (int i = 0; i < compute_channel_ptrs.size(); i++) {
        compute_clients.emplace_back(compute_channel_ptrs[i]);
    }

    while (!end_flag) {
        sem_wait(&vq.full);

        pthread_mutex_lock(&vq.mutex);
        uint64_t trans_id = vq.id_queue.front();
        Endorsement transaction = vq.trans_queue.front();
        uint64_t curr_block_id = vq.curr_block_id;
        vq.id_queue.pop();
        vq.trans_queue.pop();
        pthread_mutex_unlock(&vq.mutex);

        validate_transaction(ctx, storage_client, compute_clients, curr_block_id, trans_id, transaction);

        // add this transaction to C
        C.add(trans_id);
    }

    return NULL;
}

void *parallel_validation_manager(void *arg) {
    /* set up grpc client for storage server */
    KVStableClient storage_client(storage_channel_ptr);
    string serialised_block;
    set<uint64_t> W;

    while (!end_flag) {
        sem_wait(&bq.full);
        pthread_mutex_lock(&bq.mutex);
        Block block = bq.bq_queue.front();
        bq.bq_queue.pop();
        pthread_mutex_unlock(&bq.mutex);

        W.clear();
        C.clear();
        for (int trans_id = 0; trans_id < block.transactions_size(); trans_id++) {
            W.insert(trans_id);
        }

        while (!W.empty()) {
            for (auto it = W.begin(); it != W.end();) {
                uint64_t trans_id = *it;
                bool all_pred_in_c = true;

                // if (block.transactions(trans_id).adjacency_list_size() > 0) {
                //     log_info(stderr, "[block_id = %ld, trans_id = %ld] size of adjacency list = %d.",
                //             block.block_id(), trans_id, block.transactions(trans_id).adjacency_list_size());
                // }

                for (int i = 0; i < block.transactions(trans_id).adjacency_list_size(); i++) {
                    uint64_t pred_id = block.transactions(trans_id).adjacency_list(i);
                    if (!C.find(pred_id)) {
                        all_pred_in_c = false;
                        break;
                    }
                }
 
                if (all_pred_in_c) {
                    it = W.erase(it);

                    /* trigger parallel validation worker for this transaction */
                    pthread_mutex_lock(&vq.mutex);
                    vq.curr_block_id = block.block_id();
                    vq.id_queue.push(trans_id);
                    vq.trans_queue.emplace(block.transactions(trans_id));
                    pthread_mutex_unlock(&vq.mutex);
                    sem_post(&vq.full);
                } else {
                    it++;
                }
            }
        }

        while (C.size() != block.transactions_size())
            ;

        /* store the block with its bit mask in remote block store */
        if (!block.SerializeToString(&serialised_block)) {
            log_err("validator: failed to serialize block.");
        }
        storage_client.write_blocks(serialised_block);
    }

    return NULL;
}

class ComputeCommImpl final : public ComputeComm::Service {
   public:
    Status send_to_validator(ServerContext *context, const Block *request, google::protobuf::Empty *response) override {
        pthread_mutex_lock(&bq.mutex);
        bq.bq_queue.push(*request);
        pthread_mutex_unlock(&bq.mutex);
        sem_post(&bq.full);
        // total_ops += request->transactions_size();

        return Status::OK;
    }

    Status send_to_validator_stream(ServerContext *context, ServerReader<Block> *reader, google::protobuf::Empty *response) override {
        Block block;

        while (reader->Read(&block)) {
            pthread_mutex_lock(&bq.mutex);
            // log_info(stderr,"received block %d in validator stream", block.block_id());
            bq.bq_queue.push(block);
            pthread_mutex_unlock(&bq.mutex);
            sem_post(&bq.full);
            // total_ops += block.transactions_size();
        }

        return Status::OK;
    }

    Status invalidate_cn(ServerContext *context, const InvalidationRequest *request, google::protobuf::Empty *response) override {
        string key = request->key_to_inval();
        pthread_mutex_lock(&metadatacache.hashtable_lock);
        uint64_t r_addr = 0;
        auto meta_it = metadatacache.key_hashtable.find(key);
        if (meta_it != metadatacache.key_hashtable.end()) {
            r_addr = meta_it->second->ptr_next;
            metadatacache.lru_list.erase(meta_it->second);
            metadatacache.key_hashtable.erase(meta_it);
        }
        pthread_mutex_unlock(&metadatacache.hashtable_lock);

        if (r_addr != 0) {
            pthread_mutex_lock(&datacache.hashtable_lock);
            auto it = datacache.addr_hashtable.find(r_addr);
            if (it != datacache.addr_hashtable.end()) {
                uint64_t l_addr = it->second->l_addr;
                datacache.lru_list.erase(it->second);
                datacache.addr_hashtable.erase(it);
                datacache.free_addrs.push(l_addr);
            }
            pthread_mutex_unlock(&datacache.hashtable_lock);
        }

        return Status::OK;
    }

    Status start_benchmarking(ServerContext *context, const Notification *request, google::protobuf::Empty *response) override {
        prepopulation_completed = true;

        return Status::OK;
    }
};

class CoreManager {
    private:
        // Track active cores
        std::vector<int> active_cores;
        
        // Map core_id to its simulation and validation threads
        std::unordered_map<int, std::vector<pthread_t>> sim_threads_by_core;
        std::unordered_map<int, std::vector<pthread_t>> val_threads_by_core;
        
        // Communication components pool
        std::vector<ThreadContext> thread_contexts;
        std::vector<bool> context_in_use;
        std::unordered_map<pthread_t, int> thread_to_context_index;
        
        // Configuration
        int sim_threads_per_core;
        int val_threads_per_core;
        int max_available_threads;
        
        // Thread synchronization
        std::mutex core_mutex;
        
        // 创建并启动线程
        pthread_t create_thread(int core_id, bool is_simulation, int thread_index) {
            pthread_t tid;
            
            // 查找可用上下文
            int context_index = -1;
            for (size_t i = 0; i < context_in_use.size(); i++) {
                if (!context_in_use[i]) {
                    context_index = i;
                    context_in_use[i] = true;
                    break;
                }
            }
            
            if (context_index == -1) {
                std::cerr << "No available thread context!" << std::endl;
                return 0;
            }
            
            // 初始化线程上下文
            ThreadContext* ctx = &thread_contexts[context_index];
            ctx->thread_index = thread_index;
            ctx->end_flag = 0;  // 初始化为不终止
            
            if (is_simulation) {
                pthread_create(&tid, NULL, simulation_handler, ctx);
            } else {
                pthread_create(&tid, NULL, validation_handler, ctx);
            }
            
            // 设置CPU亲和性
            cpu_set_t cpuset;
            CPU_ZERO(&cpuset);
            CPU_SET(core_id, &cpuset);
            int ret = pthread_setaffinity_np(tid, sizeof(cpu_set_t), &cpuset);
            if (ret) {
                std::cerr << "pthread_setaffinity_np failed with: " 
                        << strerror(ret) << std::endl;
            }
            
            // 记录线程ID到上下文索引的映射
            thread_to_context_index[tid] = context_index;
            
            return tid;
        }
    
        // 优雅地停止线程
        void stop_thread(pthread_t tid) {
            // 查找线程的上下文
            auto it = thread_to_context_index.find(tid);
            if (it == thread_to_context_index.end()) {
                std::cerr << "Thread ID not found in mapping!" << std::endl;
                return;
            }
            
            int context_index = it->second;
            
            // 标记线程应当终止
            thread_contexts[context_index].end_flag = 1;
            
            // 等待线程完成
            log_info(stderr, "Waiting for thread %lu to finish..., context index is %d", tid, thread_contexts[context_index].thread_index);
            pthread_join(tid, NULL);
            log_info(stderr, "Thread %lu has finished...", tid);

            
            // 释放上下文和映射
            context_in_use[context_index] = false;
            thread_to_context_index.erase(tid);
        }
    
    public:
        CoreManager(int sim_per_core, int val_per_core, int max_threads) : 
            sim_threads_per_core(sim_per_core), 
            val_threads_per_core(val_per_core),
            max_available_threads(max_threads) {
            
            // Initialize thread contexts pool
            thread_contexts.resize(max_threads);
            context_in_use.resize(max_threads, false);
            
            // Set up communication components for each context
            for (int i = 0; i < max_threads; i++) {
                // Initialize QP and CQ for each context
                // This would be implementation-specific based on your RDMA setup
                thread_contexts[i].thread_index = i;
                thread_contexts[i].m_qp = c_ib_info.qp[i];
                thread_contexts[i].m_cq = c_ib_info.cq[i];
                // log_info(stderr, "thread_context[%d]: m_qp = %p, m_cq = %p", i, thread_contexts[i].m_qp, thread_contexts[i].m_cq);
                assert(thread_contexts[i].m_qp != NULL);
                assert(thread_contexts[i].m_cq != NULL);
            }
        }
        
        ~CoreManager() {
            // Clean up all active cores
            std::vector<int> cores_to_remove = active_cores;
            for (int core_id : cores_to_remove) {
                remove_core(core_id);
            }
        }
    
        // 初始化特定数量的核心
        void initialize(int num_cores, const std::vector<int>& core_ids = {}) {
            // 如果没有提供特定核心ID，则使用0到num_cores-1
            std::vector<int> cores_to_add;
            if (core_ids.empty()) {
                for (int i = 0; i < num_cores; i++) {
                    cores_to_add.push_back(i);
                }
            } else {
                // 使用提供的核心IDs
                cores_to_add = core_ids;
                if (cores_to_add.size() != num_cores) {
                    std::cerr << "Warning: core_ids size doesn't match num_cores" << std::endl;
                }
            }
            
            // 添加每个核心
            for (int core_id : cores_to_add) {
                if (add_core(core_id) != 0) {
                    std::cerr << "Failed to add core " << core_id << std::endl;
                }
            }
        }
        
        // 初始化具有特定线程配置的核心
        void initialize_with_config(int num_cores, 
                                   int sim_threads, 
                                   int val_threads, 
                                   const std::vector<int>& core_ids = {}) {
            // 临时保存原配置
            int original_sim = sim_threads_per_core;
            int original_val = val_threads_per_core;
            
            // 设置新配置
            sim_threads_per_core = sim_threads;
            val_threads_per_core = val_threads;
            
            // 初始化核心
            initialize(num_cores, core_ids);
            
            // 恢复原配置（如果有必要的话）
            sim_threads_per_core = original_sim;
            val_threads_per_core = original_val;
        }
    
        // Get current number of cores
        int get_core_count() {
            std::lock_guard<std::mutex> lock(core_mutex);
            return active_cores.size();
        }
        
        // Get current threads per core
        std::pair<int, int> get_threads_per_core() {
            std::lock_guard<std::mutex> lock(core_mutex);
            return {sim_threads_per_core, val_threads_per_core};
        }
        
        // Get maximum available threads
        int get_max_threads() {
            return max_available_threads;
        }

        // 添加单个验证线程到指定核心
        int add_validation_thread(int core_id) {
            std::lock_guard<std::mutex> lock(core_mutex);
            
            // 检查核心是否已经激活
            auto it = std::find(active_cores.begin(), active_cores.end(), core_id);
            if (it == active_cores.end()) {
                // 如果核心不在活动列表中，我们需要先添加它
                std::cerr << "Core " << core_id << " is not active, activating it first." << std::endl;
                active_cores.push_back(core_id);
                sim_threads_by_core[core_id] = std::vector<pthread_t>();
                val_threads_by_core[core_id] = std::vector<pthread_t>();
            }
            
            // 检查是否有足够的线程上下文可用
            int current_total_threads = 0;
            for (int core : active_cores) {
                current_total_threads += sim_threads_by_core[core].size() + val_threads_by_core[core].size();
            }
            
            if (current_total_threads + 1 > max_available_threads) {
                std::cerr << "Not enough thread contexts available!" << std::endl;
                return -1;
            }
            
            // 创建新的验证线程
            int thread_index = current_total_threads;
            pthread_t tid = create_thread(core_id, false, thread_index);
            if (tid == 0) {
                std::cerr << "Failed to create validation thread!" << std::endl;
                return -2;
            }
            
            // 添加到该核心的验证线程列表
            val_threads_by_core[core_id].push_back(tid);
            
            log_info(stderr, "Added validation thread %lu to core %d", tid, core_id);
            return 0; // 成功
        }
        
        // Add a core with the current thread distribution
        int add_core(int core_id) {
            std::lock_guard<std::mutex> lock(core_mutex);

            
            // Check if the core is already active
            if (std::find(active_cores.begin(), active_cores.end(), core_id) != active_cores.end()) {
                std::cerr << "Core " << core_id << " is already active!" << std::endl;
                return -1;
            }
            


            // Check if we have enough threads available
            int total_threads_needed = sim_threads_per_core + val_threads_per_core;
            int current_total_threads = 0;
            for (int core : active_cores) {
                current_total_threads += sim_threads_by_core[core].size() + val_threads_by_core[core].size();
            }
            
            if (current_total_threads + total_threads_needed > max_available_threads) {
                std::cerr << "Not enough thread contexts available!" << std::endl;
                return -2;
            }


            
            // Add the core to active cores
            active_cores.push_back(core_id);
            log_info(stderr, "Adding core %d with %d simulation threads and %d validation threads.\n", 
                     core_id, sim_threads_per_core, val_threads_per_core);
            // Create simulation threads
            std::vector<pthread_t> sim_tids;
            for (int i = 0; i < sim_threads_per_core; i++) {
                pthread_t tid = create_thread(core_id, true, current_total_threads + i);
                sim_tids.push_back(tid);
            }
            sim_threads_by_core[core_id] = sim_tids;


            
            // Create validation threads
            std::vector<pthread_t> val_tids;
            for (int i = 0; i < val_threads_per_core; i++) {
                pthread_t tid = create_thread(core_id, false, 
                                              current_total_threads + sim_threads_per_core + i);
                val_tids.push_back(tid);
            }
            val_threads_by_core[core_id] = val_tids;
            
            return 0; // Success
        }
        
        // Remove a core (defaults to last core if none specified)
        int remove_core(int core_id = -1) {
            std::lock_guard<std::mutex> lock(core_mutex);
            
            if (active_cores.empty()) {
                std::cerr << "No active cores to remove!" << std::endl;
                return -1;
            }
            
            // If no core_id specified, remove the last core
            if (core_id == -1) {
                core_id = active_cores.back();
            }
            
            // Check if core exists
            auto it = std::find(active_cores.begin(), active_cores.end(), core_id);
            if (it == active_cores.end()) {
                std::cerr << "Core " << core_id << " is not active!" << std::endl;
                return -2;
            }

            log_info(stderr, "Removing core %d\n", core_id);
            
            // Stop all simulation threads for this core
            log_info(stderr, "size of sim_threads_by_core: %d", sim_threads_by_core[core_id].size());
            for (pthread_t tid : sim_threads_by_core[core_id]) {
                log_info(stderr, "Stopping simulation thread %lu\n", tid);
                int ctx_index = thread_to_context_index[tid];
                stop_thread(tid);
            }

            
            // Stop all validation threads for this core
            for (pthread_t tid : val_threads_by_core[core_id]) {
                int ctx_index = thread_to_context_index[tid];
                stop_thread(tid);
                log_info(stderr, "Stopping validation thread %lu\n", tid);
            }

            
            // Remove the core from active cores
            active_cores.erase(it);
            sim_threads_by_core.erase(core_id);
            val_threads_by_core.erase(core_id);
            
            return 0; // Success
        }
        
        // Adjust thread counts per core
        int adjust_thread(int d_sim, int d_val) {
            std::lock_guard<std::mutex> lock(core_mutex);
            
            int new_sim_count = sim_threads_per_core + d_sim;
            int new_val_count = val_threads_per_core + d_val;

            log_info(stderr, "test place 1");

            
            // Ensure at least one thread of each type
            if (new_sim_count < 1 || new_val_count < 1) {
                std::cerr << "Must have at least one thread of each type!" << std::endl;
                return -1;
            }
            
            // Check if we have enough threads available
            int total_new_threads_per_core = new_sim_count + new_val_count;
            int new_total_threads = total_new_threads_per_core * active_cores.size();
            
            if (new_total_threads > max_available_threads) {
                std::cerr << "Not enough thread contexts available for adjustment!" << std::endl;
                return -2;
            }

            log_info(stderr, "test place 2");

            
            // Process each core
            for (int core_id : active_cores) {
                // Handle simulation threads
                if (d_sim > 0) {
                    // Add simulation threads
                    int current_sim_count = sim_threads_by_core[core_id].size();
                    int sim_to_add = new_sim_count - current_sim_count;
                    
                    for (int i = 0; i < sim_to_add; i++) {
                        log_info(stderr, "test place 3");

                        pthread_t tid = create_thread(core_id, true, 
                                                     current_sim_count + i);
                        log_info(stderr, "test place 4");
                        sim_threads_by_core[core_id].push_back(tid);
                    }
                } else if (d_sim < 0) {
                    // Remove simulation threads
                    int current_sim_count = sim_threads_by_core[core_id].size();
                    int sim_to_remove = current_sim_count - new_sim_count;
                    
                    for (int i = 0; i < sim_to_remove; i++) {
                        pthread_t tid = sim_threads_by_core[core_id].back();
                        sim_threads_by_core[core_id].pop_back();
                        
                        int ctx_index = thread_to_context_index[tid];
                        stop_thread(tid);
                    }
                }
                
                // Handle validation threads
                if (d_val > 0) {
                    // Add validation threads
                    int current_val_count = val_threads_by_core[core_id].size();
                    int val_to_add = new_val_count - current_val_count;
                    
                    for (int i = 0; i < val_to_add; i++) {
                        pthread_t tid = create_thread(core_id, false, 
                                                     current_val_count + i);
                        val_threads_by_core[core_id].push_back(tid);
                    }
                } else if (d_val < 0) {
                    // Remove validation threads
                    int current_val_count = val_threads_by_core[core_id].size();
                    int val_to_remove = current_val_count - new_val_count;
                    
                    for (int i = 0; i < val_to_remove; i++) {
                        pthread_t tid = val_threads_by_core[core_id].back();
                        val_threads_by_core[core_id].pop_back();
                        
                        int ctx_index = thread_to_context_index[tid];
                        stop_thread(tid);
                    }
                }
            }
            
            log_info(stderr, "test place 5");
            // Update thread counts
            sim_threads_per_core = new_sim_count;
            val_threads_per_core = new_val_count;
            
            return 0; // Success
        }
    
    };

void run_server(const string &server_address, bool is_validator) {
    //TODO 这里tg是根目录吗？
    std::filesystem::remove_all("../mydata/testdb");

    options.create_if_missing = true;
    options.error_if_exists = true;
    options.write_buffer_size = 8192000000;
    leveldb::Status s = leveldb::DB::Open(options, "../mydata/testdb", &db);
    assert(s.ok());

    /* start the grpc server for ComputeComm service */
    ComputeCommImpl service;
    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    log_info(stderr, "RPC server listening on %s", server_address.c_str());

    /* set up grpc client for other compute servers */
    vector<ComputeCommClient> compute_clients;
    for (int i = 0; i < compute_channel_ptrs.size(); i++) {
        compute_clients.emplace_back(compute_channel_ptrs[i]);
    }

    /* init local caches and spawn the thread pool */
    total_ops = 0;
    for (int offset = 0; offset < c_config_info.data_cache_size; offset += c_config_info.data_msg_size) {
        char *addr = c_ib_info.ib_data_buf + offset;
        datacache.free_addrs.push((uintptr_t)addr);
    }

    pthread_t bg_tid;
    pthread_create(&bg_tid, NULL, background_handler, NULL);
    // if (is_validator) {
    //     pthread_t validation_manager_tid;
    //     pthread_create(&validation_manager_tid, NULL, parallel_validation_manager, NULL);
    //     cpu_set_t cpuset;
    //     CPU_ZERO(&cpuset);
    //     CPU_SET(0, &cpuset);
    //     int ret = pthread_setaffinity_np(validation_manager_tid, sizeof(cpu_set_t), &cpuset);
    //     if (ret) {
    //         log_err("pthread_setaffinity_np failed with '%s'.", strerror(ret));
    //     }
    // }

    //=========================start worker threads=============================
    int num_threads = c_config_info.num_qps_per_server;
    int num_sim_threads = c_config_info.num_sim_threads;
    //TODO: HARD CODED
    CoreManager core_manager(2, 0, num_threads);
    // std::vector<int> specific_cores = {0}; 
    core_manager.initialize(15);
    core_manager.add_validation_thread(15);
    // #region original initialization code
    // pthread_t tid[num_threads];
    // struct ThreadContext *ctxs = (struct ThreadContext *)calloc(num_threads, sizeof(struct ThreadContext));
    // for (int i = 0; i < num_threads; i++) {
    //     assert(c_ib_info.cq[i] != NULL);
    //     assert(c_ib_info.qp[i] != NULL);
    //     ctxs[i].thread_index = i;
    //     ctxs[i].m_qp = c_ib_info.qp[i];
    //     ctxs[i].m_cq = c_ib_info.cq[i];
    //     if (i < num_sim_threads) {
    //         pthread_create(&tid[i], NULL, simulation_handler, &ctxs[i]);
    //     } else {
    //         // pthread_create(&tid[i], NULL, parallel_validation_worker, &ctxs[i]);
    //         pthread_create(&tid[i], NULL, validation_handler, &ctxs[i]);
    //         log_info(stderr, "validation thread created.");
    //     }
    //     /* stick thread to a core for better performance */
    //     int num_cores = sysconf(_SC_NPROCESSORS_ONLN);
    //     int core_id = i % num_cores;
    //     cpu_set_t cpuset;
    //     CPU_ZERO(&cpuset);
    //     CPU_SET(core_id, &cpuset);
    //     int ret = pthread_setaffinity_np(tid[i], sizeof(cpu_set_t), &cpuset);
    //     if (ret) {
    //         log_err("pthread_setaffinity_np failed with '%s'.", strerror(ret));
    //     }
    // }
    //#endregion

    //========================================================================


    //===================initialize data and start client=====================
    /* microbenchmark logics */
    // test_get_only();
    log_info(stderr, "prepopulate data...");
    prepopulate();
    if (is_validator) {
        for (int i = 0; i < compute_clients.size(); i++) {
            compute_clients[i].start_benchmarking();
        }
        prepopulation_completed = true;
    }
    while (!prepopulation_completed)
        ;

    //separate the client initialization
    start_client();
    //=======================================================================


    //=========================statistics thread=============================
    pthread_t stat_tid;
    pthread_create(&stat_tid, NULL, statistics_thread, NULL);
    //bind the statistic thread to cpu 0
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(0, &cpuset);
    int ret = pthread_setaffinity_np(stat_tid, sizeof(cpu_set_t), &cpuset);
    if (ret) {
        log_err("pthread_setaffinity_np failed with '%s'.", strerror(ret));
    }
    pthread_detach(stat_tid);
    //=======================================================================

    //=================threads number adjustment=============================
    // sleep(10);
    // core_manager.adjust_thread(-1, 0);
    // sleep(10);
    // core_manager.adjust_thread(1, 0);

    // core_manager.remove_core(0);
    //=======================================================================
    

    /* output stats */
    void *status;
    // for (int i = 0; i < num_threads; i++) {
    //     pthread_join(tid[i], &status);
    // }

    // pthread_join(validation_manager_tid, &status);
    pthread_join(bg_tid, &status);

    // free(ctxs);
}



/* return 0 on success, 1 on not found, -1 on error */
int KVStableClient::read_sstables(const string &key, string &value) {
    ClientContext context;
    GetRequest get_req;
    GetResponse get_rsp;

    get_req.set_key(key);
    Status status = stub_->read_sstables(&context, get_req, &get_rsp);
    if (!status.ok()) {
        log_err("gRPC failed with error message: %s.", status.error_message().c_str());
        return -1;
    } else {
        if (get_rsp.status() == GetResponse_Status_FOUND) {
            value = get_rsp.value();
        } else if (get_rsp.status() == GetResponse_Status_NOTFOUND) {
            log_err("Failed to find key (%s) on SSTables.", key.c_str());
            return 1;
        } else if (get_rsp.status() == GetResponse_Status_ERROR) {
            log_err("Error in reading sstables.");
            return -1;
        }
    }
    return 0;
}

int KVStableClient::write_sstables(const string &key, const string &value) {
    ClientContext context;
    EvictedBuffers ev;
    EvictionResponse rsp;

    char *buf = (char *)malloc(c_config_info.data_msg_size);
    bzero(buf, c_config_info.data_msg_size);
    memcpy(buf, value.c_str(), value.size());
    string actual_value(buf, c_config_info.data_msg_size);

    (*ev.mutable_eviction())[key] = actual_value;

    Status status = stub_->write_sstables(&context, ev, &rsp);
    if (!status.ok()) {
        log_err("gRPC failed with error message: %s.", status.error_message().c_str());
        free(buf);
        return -1;
    }
    free(buf);
    return 0;
}

int KVStableClient::write_blocks(const string &block) {
    ClientContext context;
    SerialisedBlock serialised_block;
    google::protobuf::Empty rsp;

    serialised_block.set_block(block);

    Status status = stub_->write_blocks(&context, serialised_block, &rsp);

    return 0;
}

int ComputeCommClient::invalidate_cn(const string &key) {
    ClientContext context;
    InvalidationRequest inval;
    google::protobuf::Empty rsp;

    inval.set_key_to_inval(key);

    // stub_->async()->invalidate_cn(&context, &inval, &rsp, [](Status s) {});
    stub_->invalidate_cn(&context, inval, &rsp);

    return 0;
}

void ComputeCommClient::start_benchmarking() {
    ClientContext context;
    Notification notifiction;
    google::protobuf::Empty rsp;

    Status status = stub_->start_benchmarking(&context, notifiction, &rsp);
}

int main(int argc, char *argv[]) {
    /* set config info */
    c_config_info.num_qps_per_server = 64;
    c_config_info.data_msg_size = 4 * 1024;            // 4KB each data entry
    c_config_info.data_cache_size = 50 * 1024 * 1024;  // 50MB data cache
    c_config_info.metadata_cache_size = 10000;         // 10000 cursors in metadata cache
    c_config_info.ctrl_msg_size = 1 * 1024;            // 1KB, maximum size of control message
    c_config_info.bg_msg_size = 4 * 1024;              // 4KB, maximum size of background message
    c_config_info.sock_port = 4711;                    // socket port used by memory server to init RDMA connection
    c_config_info.sock_addr = "127.0.0.1";             // ip address of memory server

    bool is_validator = false;
    int opt;
    string configfile = "config/compute.config";
    string server_address;
    while ((opt = getopt(argc, argv, "hva:c:")) != -1) {
        switch (opt) {
            case 'h':
                fprintf(stderr, "compute server usage:\n");
                fprintf(stderr, "\t-h: print this help message\n");
                fprintf(stderr, "\t-v: only if specified, act as the validator node\n");
                fprintf(stderr, "\t-a <server_ip:server_port>: the listening addr of grpc server\n");
                fprintf(stderr, "\t-c <path_to_config_file>: path to the configuration file\n");
                exit(0);
            case 'v':
                is_validator = true;
                break;
            case 'a':
                server_address = std::string(optarg);
                break;
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
        if (tmp[0] == "num_qps_per_server") {
            sstream >> c_config_info.num_qps_per_server;
            log_info(stderr,"set qps per server:%d", c_config_info.num_qps_per_server);
        } else if (tmp[0] == "num_sim_threads") {
            sstream >> c_config_info.num_sim_threads;
            log_info(stderr,"set sim thread per server:%d", c_config_info.num_sim_threads);
        } else if (tmp[0] == "data_msg_size") {
            sstream >> c_config_info.data_msg_size;
        } else if (tmp[0] == "data_cache_size") {
            sstream >> c_config_info.data_cache_size;
        } else if (tmp[0] == "metadata_cache_size") {
            sstream >> c_config_info.metadata_cache_size;
        } else if (tmp[0] == "ctrl_msg_size") {
            sstream >> c_config_info.ctrl_msg_size;
        } else if (tmp[0] == "bg_msg_size") {
            sstream >> c_config_info.bg_msg_size;
        } else if (tmp[0] == "sock_port") {
            sstream >> c_config_info.sock_port;
        } else if (tmp[0] == "sock_addr") {
            c_config_info.sock_addr = tmp[1];
        } else if (tmp[0] == "storage_grpc_endpoint") {
            c_config_info.storage_grpc_endpoint = tmp[1];
        } else if (tmp[0] == "orderer_grpc_endpoint") {
            c_config_info.orderer_grpc_endpoint = tmp[1];
        } else if (tmp[0] == "compute_grpc_endpoint") {
            if (is_validator) {
                compute_channel_ptrs.push_back(grpc::CreateChannel(tmp[1], grpc::InsecureChannelCredentials()));
            }
        } else {
            fprintf(stderr, "Invalid config parameter `%s`.\n", tmp[0].c_str());
            exit(1);
        }
    }
    c_config_info.ctrl_buffer_size = c_config_info.ctrl_msg_size * c_config_info.num_qps_per_server;
    assert(c_config_info.data_cache_size % c_config_info.data_msg_size == 0);

    /* init logger */
    pthread_mutex_init(&logger_lock, NULL);
    logger_fp = fopen("compute_server.log", "w+");

    /* set up grpc channels */
    storage_channel_ptr = grpc::CreateChannel(c_config_info.storage_grpc_endpoint, grpc::InsecureChannelCredentials());
    orderer_channel_ptr = grpc::CreateChannel(c_config_info.orderer_grpc_endpoint, grpc::InsecureChannelCredentials());

    /* set up RDMA connection with the memory server */
    compute_setup_ib(c_config_info, c_ib_info);

    run_server(server_address, is_validator);
}