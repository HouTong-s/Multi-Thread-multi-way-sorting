#include "ThreadPool.h"

ThreadPool::ThreadPool(size_t threads) : stop(false) {
    for(size_t i = 0; i < threads; ++i) {
        workers.emplace_back([this] { this->workerFunction(); });
    }
}

ThreadPool::~ThreadPool() {

}

void ThreadPool::shutdown() {
    stop = true;
    condition.notify_all();
    for(std::thread &worker: workers) {
        worker.join();
    }
}

void ThreadPool::enqueue(std::function<void()> task) {
    {
        std::unique_lock<std::mutex> lock(queue_mutex);
        tasks.push(task);
    }
    condition.notify_one();
}

void ThreadPool::workerFunction() {
    while(true) {
        std::function<void()> task;
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            condition.wait(lock, [this]{ return this->stop || !this->tasks.empty(); });
            if(this->stop && this->tasks.empty()) {
                return;
            }
            task = std::move(this->tasks.front());
            this->tasks.pop();
        }
        task();
    }
}
