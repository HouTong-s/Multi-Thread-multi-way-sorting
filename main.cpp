#include <iostream>
#include <filesystem>
#include <fstream>
#include <chrono>

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

    // 对文件列表中的每个文件进行排序
    for (const auto& file : fileList) {
        threadPool.enqueue([&, file]() {
            size_t partitionSize = fileHandler.getPartitionSize(file);
            size_t offset = 0;
            std::vector<int64_t> data;
            int i = 0;
            
            while ((data = fileHandler.readPartition(file, offset, partitionSize)).size() > 0) {
                std::cout << file << ":" << i <<" DataNums:" << data.size() << std::endl;
                std::vector<int64_t> sortedData = sorter.sortData(data);
                std::string tempFilename = sorter.saveSortedData(sortedData, inputDirectory); // 临时文件保存在输入目录中
                tempFiles.push_back(tempFilename);

                i++;

                offset += partitionSize;
            }
        });
    }

    // 确保所有排序任务完成
    threadPool.~ThreadPool();

    // 归并所有临时文件
    Merger merger;
    while (tempFiles.size() > 1) { // 保持归并，直到只剩下一个文件
        tempFiles = merger.twoWayMerge(tempFiles, inputDirectory);
    }
    

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

    

    return 0;

}
