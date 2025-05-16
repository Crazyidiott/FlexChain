#include <map>
#include <set>
#include <thread>
#include <atomic>
#include <unistd.h>
#include <condition_variable>


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
        pthread_t create_thread(int core_id, bool is_simulation) {
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
            ctx->thread_index = context_index;
            ctx->end_flag = 0;  // 初始化为不终止
            
            if (is_simulation) {
                pthread_create(&tid, NULL, simulation_handler, ctx);
            } else {
                pthread_create(&tid, NULL, validation_handler, ctx);
            }

            // log_info(stderr, "tid is: %lu, context_id is %d", tid, context_index);

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
                log_err("在映射中找不到线程 ID %lu！", (unsigned long)tid);
                return;
            }
            
            int context_index = it->second;
            
            // 记录实际的映射值和线程索引
            log_debug(stderr, "stopping thread %lu, map to context_index %d, thread index %d", 
                     (unsigned long)tid, context_index, thread_contexts[context_index].thread_index);
            
            // 标记线程应当终止
            thread_contexts[context_index].end_flag = 1;
            
            // 等待线程完成
            pthread_join(tid, NULL);
            log_info(stderr, "thread %lu stopped", (unsigned long)tid);
            
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
            pthread_t tid = create_thread(core_id, false);
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
                pthread_t tid = create_thread(core_id, true);
                sim_tids.push_back(tid);
            }
            sim_threads_by_core[core_id] = sim_tids;


            
            // Create validation threads
            std::vector<pthread_t> val_tids;
            for (int i = 0; i < val_threads_per_core; i++) {
                pthread_t tid = create_thread(core_id, false);
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

            // Ensure at least one thread of each type
            if (new_sim_count < 1) {
                std::cerr << "Must have at least one simulation thread!" << std::endl;
                return -1;
            }
            
            // Check if we have enough threads available
            int total_new_threads_per_core = new_sim_count + new_val_count;
            int new_total_threads = total_new_threads_per_core * active_cores.size();
            
            if (new_total_threads > max_available_threads) {
                std::cerr << "Not enough thread contexts available for adjustment!" << std::endl;
                return -2;
            }

            // Process each core
            for (int core_id : active_cores) {
                // Handle simulation threads
                if (d_sim > 0) {
                    // Add simulation threads
                    int current_sim_count = sim_threads_by_core[core_id].size();
                    int sim_to_add = new_sim_count - current_sim_count;
                    
                    for (int i = 0; i < sim_to_add; i++) {
                        pthread_t tid = create_thread(core_id, true);
                        sim_threads_by_core[core_id].push_back(tid);
                    }
                } else if (d_sim < 0) {
                    // Remove simulation threads
                    int current_sim_count = sim_threads_by_core[core_id].size();
                    int sim_to_remove = current_sim_count - new_sim_count;
                    
                    for (int i = 0; i < sim_to_remove; i++) {
                        pthread_t tid = sim_threads_by_core[core_id].back();
                        sim_threads_by_core[core_id].pop_back();
                        auto it = thread_to_context_index.find(tid);

                        if (it != thread_to_context_index.end()) {
                            // log_info(stderr, "thread %lu map to context_index %d", (unsigned long)tid, it->second);
                        } else {
                            log_err("找不到线程 %lu 的映射", (unsigned long)tid);
                        }
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
                        pthread_t tid = create_thread(core_id, false);
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
            
            // Update thread counts
            sim_threads_per_core = new_sim_count;
            val_threads_per_core = new_val_count;
            
            return 0; // Success
        }
    
    };
