#include "ThreadPool.h"
#include<iostream>

ThreadPool::ThreadPool(size_t threads) : stop(false) {

    for(size_t i = 0; i < threads; ++i) {
        workers.emplace_back([this, i] {  // capture i
            this->workerFunction();
        });
    }
}

ThreadPool::~ThreadPool() {
    // shutdown();
}

void ThreadPool::shutdown() {
    {
        std::unique_lock<std::mutex> lock(queue_mutex);
        stop = true;
    }
    condition.notify_all();

    for(size_t i = 0; i < workers.size(); ++i) {
        if (workers[i].joinable()) {
            workers[i].join();
        }
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
                std::cout<<"thread exited!"<<std::endl;
                return;
            }
            task = std::move(this->tasks.front());
            this->tasks.pop();
        }
        task();
    }
}
