#include "Merger.h"
#include <iostream>
#include <filesystem>
#include <fstream>
#include <sstream>

Merger::Merger() : mergeFileCount(0) {}

std::string Merger::twoWayMergeThreadSafe(const std::string& file1Name, const std::string& file2Name, const std::string& tempDir) {
    std::string mergedFileName = getNextMergeFileName(tempDir);

    std::ifstream file1(file1Name, std::ios::binary);
    std::ifstream file2;
    if (!file2Name.empty()) {
        file2.open(file2Name, std::ios::binary);
    }
    else {
        std::cout << "file2Name empty" << std::endl;
    }
    std::ofstream mergedFile(mergedFileName, std::ios::binary);

    int64_t value1, value2;
    bool hasValue1 = bool(file1.read(reinterpret_cast<char*>(&value1), sizeof(int64_t)));
    bool hasValue2 = file2.is_open() && bool(file2.read(reinterpret_cast<char*>(&value2), sizeof(int64_t)));

    while (hasValue1 || hasValue2) {
        if (!hasValue2 || (hasValue1 && value1 <= value2)) {
            mergedFile.write(reinterpret_cast<char*>(&value1), sizeof(int64_t));
            hasValue1 = bool(file1.read(reinterpret_cast<char*>(&value1), sizeof(int64_t)));
        } else {
            mergedFile.write(reinterpret_cast<char*>(&value2), sizeof(int64_t));
            hasValue2 = bool(file2.read(reinterpret_cast<char*>(&value2), sizeof(int64_t)));
        }
    }

    file1.close();
    if (file2.is_open()) file2.close();
    mergedFile.close();

    // 删除归并之前的两个文件
    std::filesystem::remove(file1Name);
    if (!file2Name.empty()) {
        std::filesystem::remove(file2Name);
    }

    return mergedFileName;
}

std::string Merger::multiWayMergeThreadSafe(const std::vector<std::string>& fileNames, const std::string& tempDir) {
    if (fileNames.empty()) {
        return "";  // 没有要合并的文件
    }

    // 创建一个用于合并数据的输出文件
    std::string mergedFileName = getNextMergeFileName(tempDir);
    std::ofstream mergedFile(mergedFileName, std::ios::binary);

    // 创建缓存区用于每个文件
    std::vector<std::ifstream> inputFiles;
    std::vector<std::vector<int64_t>> fileBuffers;
    for (const std::string& fileName : fileNames) {
        inputFiles.emplace_back(fileName, std::ios::binary);
        fileBuffers.emplace_back(10 * 1024 * 1024 / sizeof(int64_t)); // 10MB缓存区
    }

    // 从每个文件读取初始数据到缓存区
    for (size_t i = 0; i < fileNames.size(); ++i) {
        int64_t* bufferData = fileBuffers[i].data();
        inputFiles[i].read(reinterpret_cast<char*>(bufferData), fileBuffers[i].size() * sizeof(int64_t));
    }

    // 创建优先队列，用于保存来自每个文件缓存区的当前最小元素
    std::priority_queue<MergeElement> minHeap;

    for (size_t i = 0; i < fileNames.size(); ++i) {
        if (!fileBuffers[i].empty()) {
            minHeap.push({fileBuffers[i][0], i});
        }
    }

    while (!minHeap.empty()) {
        // 从优先队列中获取最小的元素
        MergeElement minElement = minHeap.top();
        minHeap.pop();

        // 将最小的元素写入合并文件
        mergedFile.write(reinterpret_cast<char*>(&minElement.value), sizeof(int64_t));

        // 从提供最小元素的文件缓存区中读取下一个元素
        size_t fileIndex = minElement.fileIndex;
        std::vector<int64_t>& buffer = fileBuffers[fileIndex];

        if (!buffer.empty()) {
            // 移除已使用的元素
            buffer.erase(buffer.begin());

            // 如果缓存区不为空，将下一个元素添加到优先队列
            if (!buffer.empty()) {
                minHeap.push(MergeElement{buffer[0], fileIndex});
            } else {
                // 缓存区已空，从文件读取下一个数据块到缓存区
                int64_t* bufferData = buffer.data();
                inputFiles[fileIndex].read(reinterpret_cast<char*>(bufferData), buffer.size() * sizeof(int64_t));
            }
        }
    }

    // 删除输入文件
    for (const std::string& fileName : fileNames) {
        std::filesystem::remove(fileName);
    }

    mergedFile.close();

    return mergedFileName;
}


std::string Merger::getNextMergeFileName(const std::string& tempDir) {
    std::ostringstream oss;
    size_t currentCount = mergeFileCount.fetch_add(1);  // Atomically increase the count
    oss << tempDir << "/merged_" << currentCount << ".dat";
    return oss.str();
}

