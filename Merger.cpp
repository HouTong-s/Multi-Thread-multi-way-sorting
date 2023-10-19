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



std::vector<std::string> Merger::twoWayMerge(const std::vector<std::string>& tempFiles, const std::string& tempDir) {
    std::vector<std::string> mergedFiles;

    std::mutex mutex; // 用于同步对mergedFiles的访问
    
    size_t fileCount = tempFiles.size();

    for (size_t i = 0; i < fileCount; i += 2) {
        std::string mergedFileName = getNextMergeFileName(tempDir);
        mergedFiles.push_back(mergedFileName);

        std::ifstream file1(tempFiles[i], std::ios::binary);
        std::ifstream file2((i + 1) < fileCount ? tempFiles[i + 1] : "", std::ios::binary);
        std::ofstream mergedFile(mergedFileName, std::ios::binary);

        int64_t value1, value2;
        bool hasValue1 = bool(file1.read(reinterpret_cast<char*>(&value1), sizeof(int64_t)));
        bool hasValue2 = bool(file2.read(reinterpret_cast<char*>(&value2), sizeof(int64_t)));

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
        file2.close();
        mergedFile.close();


        std::filesystem::remove(tempFiles[i]);

        if ((i + 1) < fileCount) {
            std::filesystem::remove(tempFiles[i + 1]);
        }
    }

    return mergedFiles;
}


void Merger::mergeToFinalOutput(const std::vector<std::string>& tempFiles, const std::string& outputFilename) {
    std::ofstream outputFile(outputFilename, std::ios::binary);
    std::priority_queue<MergeElement> pq;
    std::vector<std::ifstream> files(tempFiles.size());

    for (size_t i = 0; i < tempFiles.size(); ++i) {
        files[i].open(tempFiles[i], std::ios::binary);
        MergeElement elem;
        if (files[i].read(reinterpret_cast<char*>(&elem.value), sizeof(int64_t))) {
            elem.fileIndex = i;
            pq.push(elem);
        }
    }

    while (!pq.empty()) {
        MergeElement elem = pq.top();
        pq.pop();
        outputFile.write(reinterpret_cast<char*>(&elem.value), sizeof(int64_t));

        if (files[elem.fileIndex].read(reinterpret_cast<char*>(&elem.value), sizeof(int64_t))) {
            pq.push(elem);
        }
    }

    for (auto& file : files) {
        file.close();
    }
    outputFile.close();
}


std::string Merger::getNextMergeFileName(const std::string& tempDir) {
    std::ostringstream oss;
    size_t currentCount = mergeFileCount.fetch_add(1);  // Atomically increase the count
    oss << tempDir << "/merged_" << currentCount << ".dat";
    return oss.str();
}

