#include <iostream>
#include <fstream>
#include <queue>
#include <string>
#include <vector>
#include <thread>
#include <mutex>
#include <atomic>
#include <chrono>
#include <random>
#include <cstring>
#include <filesystem>

// 交易队列结构
struct TransactionQueue {
    std::queue<std::string> trans_queue;
    std::mutex mutex;
};

// 全局变量
TransactionQueue tq;
std::atomic<bool> ready_flag(false);
std::atomic<bool> end_flag(false);
std::atomic<unsigned long> last_log_index(0);
std::atomic<unsigned long> commit_index(0);
const int MAX_BATCH_SIZE = 100;

// 写入日志文件的线程函数
void write_log_thread() {
    std::cout << "Write log thread is running." << std::endl;
    
    // 确保目录存在
    std::filesystem::remove_all("./log_data");
    std::filesystem::create_directory("./log_data");
    
    // 打开日志文件
    std::ofstream logfile("./log_data/transactions.log", std::ios::out | std::ios::binary);
    if (!logfile.is_open()) {
        std::cerr << "Error: Could not open log file for writing." << std::endl;
        return;
    }
    
    const int LOG_ENTRY_BATCH = 10; // 每批写入的最大交易数量
    
    ready_flag = true; // 表示写入线程已准备好
    
    while (!end_flag) {
        std::lock_guard<std::mutex> lock(tq.mutex);
        int i = 0;
        for (; (!tq.trans_queue.empty()) && i < LOG_ENTRY_BATCH; i++) {
            // 写入交易大小
            uint32_t size = tq.trans_queue.front().size();
            std::cout << "Before read: tellP=" << logfile.tellp() << std::endl;  
            
            logfile.write(reinterpret_cast<char*>(&size), sizeof(uint32_t));
            std::cout << "After read: tellp=" << logfile.tellp() << ",size=" << size << ",good=" << logfile.good() << ",fail=" << logfile.fail() << std::endl;
            
            // 写入交易内容
            logfile.write(tq.trans_queue.front().c_str(), tq.trans_queue.front().size());
            tq.trans_queue.pop();
        }
        
        // 刷新文件确保数据写入磁盘
        logfile.flush();
        
        // 更新日志索引
        last_log_index += i;
    }
    
    // 关闭文件
    logfile.close();
    std::cout << "Write log thread has stopped." << std::endl;
}

// 读取日志并进行批处理的线程函数
void batch_processing_thread() {
    std::cout << "Batch processing thread is running." << std::endl;
    
    // 确保日志文件存在
    while (!ready_flag) {
        // std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    
    // 等待一些日志数据生成
    // std::this_thread::sleep_for(std::chrono::seconds(1));
    
    // 打开日志文件用于读取
    std::ifstream logfile("./log_data/transactions.log", std::ios::in | std::ios::binary);
    if (!logfile.is_open()) {
        std::cerr << "Error: Could not open log file for reading." << std::endl;
        return;
    }
    
    std::vector<std::string> batch;
    unsigned long last_applied = 0;
    int batch_count = 0;
    
    while (!end_flag) {
        // 只处理已提交的日志条目
        if (commit_index > last_applied) {
            last_applied++;
            
            // 读取交易大小
            uint32_t size;
            // std::cout << "Before read: tellg=" << logfile.tellg() << std::endl;
            // logfile.read(reinterpret_cast<char*>(&size), sizeof(uint32_t));
            logfile.read((char *)&size, sizeof(uint32_t));

            // std::cout << "After read: tellg=" << logfile.tellg() << ",size=" << size << ",good=" << logfile.good() << ",fail=" << logfile.fail() << std::endl;
            
            if (logfile.eof() || !logfile.good()) {
                // 文件结束或读取错误，稍后重试
                std::cout << "End of file reached or read error. Retrying...................................................................................................................." << std::endl;
                // std::this_thread::sleep_for(std::chrono::milliseconds(100));
                continue;
            }
            
            // 读取交易数据
            // char* entry_ptr = new char[size + 1];
            char *entry_ptr = (char *)malloc(size);

            logfile.read(entry_ptr, size);
            // entry_ptr[size] = '\0'; // 添加字符串结束符
            
            // 添加到批处理中
            std::string transaction(entry_ptr, size);
            batch.push_back(transaction);
            free(entry_ptr);
            // delete[] entry_ptr;
            
            // 当批处理达到最大大小时处理
            if (batch.size() >= MAX_BATCH_SIZE) {
                batch_count++;
                
                // 打印批处理内容
                std::cout << "\n----- Batch #" << batch_count << " -----" << std::endl;
                std::cout << "Processing " << batch.size() << " transactions:" << std::endl;
                for (int i = 0; i < batch.size(); i++) {
                    std::cout << "Transaction " << i << ": " << batch[i] << std::endl;
                }
                std::cout << "----- End of Batch #" << batch_count << " -----\n" << std::endl;
                
                // 清空批处理以处理下一批
                batch.clear();
            }
        } else {
            // 没有新的已提交日志条目，休眠一会儿
            // std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        
        // 更新commit_index（在实际系统中这可能是基于一致性协议的）
        // 在这个简化的版本中，我们简单地追随last_log_index
        if (last_log_index > commit_index) {
            commit_index++;
        }
    }
    
    // 处理剩余的交易
    if (!batch.empty()) {
        batch_count++;
        std::cout << "\n----- Final Batch #" << batch_count << " -----" << std::endl;
        std::cout << "Processing " << batch.size() << " transactions:" << std::endl;
        for (int i = 0; i < batch.size(); i++) {
            std::cout << "Transaction " << i << ": " << batch[i] << std::endl;
        }
        std::cout << "----- End of Final Batch #" << batch_count << " -----\n" << std::endl;
    }
    
    // 关闭文件
    logfile.close();
    std::cout << "Batch processing thread has stopped." << std::endl;
}

// 客户端线程函数，生成交易
void client_thread() {
    std::cout << "Client thread is running." << std::endl;
    
    // 等待写入线程准备就绪
    while (!ready_flag) {
        // std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    
    // 配置参数
    const int trans_per_interval = 50;  // 每次间隔生成的交易数
    const int interval_ms = 500;        // 间隔时间（毫秒）
    
    // 随机数生成器，用于生成不同的交易值
    std::default_random_engine generator;
    std::uniform_int_distribution<int> distribution(1, 1000);
    
    // 循环生成交易，直到收到结束信号
    while (!end_flag) {
        std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms));

        // 在每个间隔内生成交易
        for (int i = 0; i < trans_per_interval; i++) {
            // 生成随机交易
            int number = distribution(generator);
            std::string transaction = "Transaction_" + std::to_string(number) + "_Value_" + std::to_string(number * 100);
            
            // 将交易添加到队列
            std::lock_guard<std::mutex> lock(tq.mutex);
            tq.trans_queue.push(transaction);
        }
        
        // 等待下一个间隔
    }
    
    std::cout << "Client thread has stopped." << std::endl;
}

// 主函数
int main() {
    std::cout << "Starting simplified transaction system..." << std::endl;
    
    // 启动写入线程
    std::thread writer(write_log_thread);
    
    // 启动批处理线程
    std::thread processor(batch_processing_thread);
    
    // 启动客户端线程
    std::thread client(client_thread);
    
    // 运行一段时间后结束
    std::cout << "System will run for 10 seconds..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(10));
    
    // 设置结束标志
    end_flag = true;
    std::cout << "Shutting down..." << std::endl;
    
    // 等待所有线程完成
    client.join();
    writer.join();
    processor.join();
    
    std::cout << "All threads have stopped. System shutdown complete." << std::endl;
    
    return 0;
}