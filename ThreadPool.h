#ifndef THREADPOOL_H
#define THREADPOOL_H
#include <vector>
#include <queue>
#include <thread>
#include <functional>
#include <mutex>
#include <atomic>
#include <condition_variable>

class ThreadPool {
public:
    ThreadPool(size_t threads);
    ~ThreadPool();
    
    void shutdown();             // 关闭线程池并等待所有任务完成
    void enqueue(std::function<void()> task);

private:

    std::vector<std::thread> workers;
    std::queue<std::function<void()>> tasks;
    std::mutex queue_mutex;
    std::condition_variable condition;
    std::atomic<bool> stop;

    void workerFunction();
};

#endif