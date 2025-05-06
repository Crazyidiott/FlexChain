#include <map>
#include <set>
#include <thread>
#include <atomic>
#include <unistd.h>
#include <condition_variable>

struct WorkerInfo {
    pthread_t tid;
    bool is_simulation;
    ThreadContext ctx;
};

class CoreManager {
private:
    std::map<int, std::vector<WorkerInfo>> core_workers; // 核ID -> 线程信息
    std::set<int> active_cores;
    pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
    std::condition_variable_any cv;
    std::atomic<bool> running{true};

    int current_thread_index = 0;

public:
    int sim_threads_per_core = 1;
    int val_threads_per_core = 1;

    void start();
    void stop();

    void add_core(int core_id);
    void remove_core(int core_id);

    void *manager_loop();
};

CoreManager core_manager;

/////////////////////////////////////
void *core_manager_thread(void *arg) {
    return core_manager.manager_loop();
}

void CoreManager::start() {
    pthread_t tid;
    pthread_create(&tid, nullptr, core_manager_thread, nullptr);
    pthread_detach(tid);
}

void CoreManager::stop() {
    running = false;
    cv.notify_all();
}

void *CoreManager::manager_loop() {
    while (running) {
        // 阻塞式等待（可后续用于响应外部gRPC事件）
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    return nullptr;
}
//////////////////////////////////////////

void CoreManager::add_core(int core_id) {
    pthread_mutex_lock(&lock);

    if (active_cores.count(core_id)) {
        pthread_mutex_unlock(&lock);
        return;
    }

    active_cores.insert(core_id);

    for (int i = 0; i < sim_threads_per_core; ++i) {
        ThreadContext ctx;
        ctx.thread_index = current_thread_index++;
        ctx.m_qp = c_ib_info.qp[ctx.thread_index];
        ctx.m_cq = c_ib_info.cq[ctx.thread_index];

        WorkerInfo info;
        info.ctx = ctx;
        info.is_simulation = true;

        pthread_create(&info.tid, nullptr, simulation_handler, &info.ctx);

        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(core_id, &cpuset);
        pthread_setaffinity_np(info.tid, sizeof(cpu_set_t), &cpuset);

        core_workers[core_id].push_back(info);
    }

    for (int i = 0; i < val_threads_per_core; ++i) {
        ThreadContext ctx;
        ctx.thread_index = current_thread_index++;
        ctx.m_qp = c_ib_info.qp[ctx.thread_index];
        ctx.m_cq = c_ib_info.cq[ctx.thread_index];

        WorkerInfo info;
        info.ctx = ctx;
        info.is_simulation = false;

        pthread_create(&info.tid, nullptr, validation_handler, &info.ctx);

        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(core_id, &cpuset);
        pthread_setaffinity_np(info.tid, sizeof(cpu_set_t), &cpuset);

        core_workers[core_id].push_back(info);
    }

    pthread_mutex_unlock(&lock);
}

//////////////////////////////////////////////////////

void CoreManager::remove_core(int core_id) {
    pthread_mutex_lock(&lock);

    if (!active_cores.count(core_id)) {
        pthread_mutex_unlock(&lock);
        return;
    }

    for (auto &worker : core_workers[core_id]) {
        pthread_cancel(worker.tid);
        pthread_join(worker.tid, nullptr);
    }

    core_workers.erase(core_id);
    active_cores.erase(core_id);

    pthread_mutex_unlock(&lock);
}

// 在run_server初始化后添加：
core_manager.sim_threads_per_core = 2;
core_manager.val_threads_per_core = 1;
core_manager.start();

// 添加核
core_manager.add_core(2); // 在第2号核心上添加
core_manager.add_core(3);

// 删除核
core_manager.remove_core(2);

