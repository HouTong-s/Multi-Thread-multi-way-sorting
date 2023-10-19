#include <iostream>
#include <filesystem>
#include <fstream>
#include <chrono>
#include <atomic>

#include "ThreadPool.h"
#include "FileHandler.h"
#include "Sorter.h"
#include "Merger.h"


int main(int argc, char* argv[]) {

    if (argc < 3) {
        std::cout << "Usage: " << argv[0] << " <input directory> <output file>" << std::endl;
        return 1;
    }

    std::string inputDirectory = argv[1];
    std::string outputFile = argv[2];

    auto startTime = std::chrono::high_resolution_clock::now();

    std::cout<< "thread nums:" <<std::thread::hardware_concurrency() << std::endl;
    
    // 初始化线程池
    ThreadPool threadPool(std::thread::hardware_concurrency()*2); // 使用逻辑核心的数量作为线程数

    // 获取文件列表
    FileHandler fileHandler(inputDirectory);
    std::vector<std::string> fileList = fileHandler.getFileList();
    Sorter sorter;

    std::vector<std::string> tempFiles; // 存储所有临时排序文件的路径

    std::mutex tempFilesMutex;

    constexpr size_t maxTaskDataSize = 200 * 1024 * 1024 / sizeof(int64_t); // 200MB in terms of int64_t numbers

    // 对文件列表中的每个文件进行排序
    for (const auto& file : fileList) {
        size_t totalSize = std::filesystem::file_size(file) / sizeof(int64_t); // Total numbers in the file
        size_t partitionSize = fileHandler.getPartitionSize(file);
        size_t tasksForCurrentFile = (totalSize + maxTaskDataSize - 1) / maxTaskDataSize; // Ceiling division

        for (size_t task = 0; task < tasksForCurrentFile; ++task) {
            threadPool.enqueue([&, file, task]() {
                try{
                size_t startOffset = task * maxTaskDataSize;
                size_t endOffset = std::min((task+1) * maxTaskDataSize, totalSize);
                size_t currentOffset = startOffset;

                while (currentOffset < endOffset) {
                    size_t currentTaskSize = std::min(partitionSize, endOffset - currentOffset);

                    std::vector<int64_t> data = fileHandler.readPartition(file, currentOffset, currentTaskSize);
                    if (data.empty()) {
                        break; // No more data to read
                    }

                    std::cout << file << ": Task " << task << " Thread id: " << std::this_thread::get_id() << " DataNums:" << data.size() << std::endl;
                    
                    std::vector<int64_t> sortedData = sorter.sortData(data);
                    std::string tempFilename = sorter.saveSortedData(sortedData, inputDirectory);

                    // This part should be synchronized since tempFiles is shared among threads
                    {
                        std::unique_lock<std::mutex> lock(tempFilesMutex);
                        tempFiles.push_back(tempFilename);
                    }

                    currentOffset += currentTaskSize;
                }
                }
                catch (const std::system_error& e) {
                std::cerr << "Code part: " << 1 << '\n';
                std::cerr << "Error: " << e.what() << '\n';
                std::cerr << "Code: " << e.code() << '\n';
            }
            });
        }
    }


    // 确保所有排序任务完成
    threadPool.shutdown();

    ThreadPool mergerThreadPool(std::thread::hardware_concurrency()*2);

    std::atomic<size_t> mergeTaskCounter(0);
    std::condition_variable mergeCompleted;
    std::mutex fileMutex;
    std::mutex dummyMutex;

    Merger merger;
    while (tempFiles.size() > 1) {
        size_t taskCount = (tempFiles.size() + 1) / 2; // 两个文件一组
        mergeTaskCounter = taskCount;
        std::vector<std::string> mergedFiles;

        for (size_t i = 0; i < tempFiles.size(); i += 2) {
            mergerThreadPool.enqueue([&, i]() {
                if(i+1==tempFiles.size())
                {
                    std::cout << "empty!" << std::endl;
                }
                std::string mergedFileName = merger.twoWayMergeThreadSafe(tempFiles[i], i + 1 < tempFiles.size() ? tempFiles[i + 1] : "", inputDirectory);
                {
                    std::unique_lock<std::mutex> lock(fileMutex);
                    mergedFiles.push_back(mergedFileName);
                }
                if (--mergeTaskCounter == 0) {
                    mergeCompleted.notify_one();
                }
            });
        }

        // 等待所有归并任务完成
        {
            std::unique_lock<std::mutex> dummyLock(dummyMutex);
            mergeCompleted.wait(dummyLock, [&]() { return mergeTaskCounter == 0; });
        }

        tempFiles = mergedFiles; // 为下一轮归并更新文件列表
    }


    mergerThreadPool.shutdown();
    

    if (!tempFiles.empty()) {
        std::filesystem::rename(tempFiles[0], outputFile); // 将最后一个临时文件重命名为最终输出文件
    }

    std::cout << "Sorting completed. Sorted data is saved to: " << outputFile << std::endl;

    auto endTime = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime).count();
    std::cout << "\nTotal execution time: " << duration << " milliseconds.\n";

    

    // 打开输出文件并显示前100个数字
    std::ifstream output(outputFile, std::ios::binary | std::ios::in);
    if (output) {
        std::cout << "\nFirst 100 numbers in sorted order:\n";
        for (int i = 0; i < 100; i++) {
            int64_t value;
            if (output.read(reinterpret_cast<char*>(&value), sizeof(int64_t))) {
                std::cout << value << " ";
            }
        }
        std::cout << std::endl;
        output.close();
    }
    // std::cout<<"exit"<<std::endl;
    return 0;

}
