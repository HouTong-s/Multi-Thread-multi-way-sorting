#include <iostream>
#include <filesystem>
#include <fstream>
#include <chrono>

#include "ThreadPool.h"
#include "FileHandler.h"
#include "Sorter.h"
#include "Merger.h"


int main(int argc, char* argv[]) {
    try {
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
                    std::string tempFilename = sorter.saveSortedData(sortedData, inputDirectory+"/temp");

                    // This part should be synchronized since tempFiles is shared among threads
                    {
                        std::unique_lock<std::mutex> lock(tempFilesMutex);
                        tempFiles.push_back(tempFilename);
                    }

                    currentOffset += currentTaskSize;
                }
            });
        }
    }


    // 确保所有排序任务完成
    threadPool.shutdown();

    ThreadPool mergerThreadPool(std::thread::hardware_concurrency()*2);

    Merger merger;

    for(int i=0;i<std::thread::hardware_concurrency()*2;i++)
    {
        mergerThreadPool.enqueue([&](){
            merger.twoWayMergeThreadSafe(tempFiles, inputDirectory+"/temp");
        });
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
    }
    catch (const std::system_error& e) {
        std::cerr << "Error: " << e.what() << '\n';
        std::cerr << "Code: " << e.code() << '\n';
    }



    return 0;

}
