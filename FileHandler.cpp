#include "FileHandler.h"
#include <fstream>
#include <filesystem> // C++17 feature
#include <algorithm>

FileHandler::FileHandler(const std::string& dir) : directory(dir) {}

std::vector<std::string> FileHandler::getFileList() {
    std::vector<std::string> fileList;
    for (const auto& entry : std::filesystem::directory_iterator(directory)) {
        fileList.push_back(entry.path().string());
    }
    return fileList;
}

size_t FileHandler::getPartitionSize(const std::string& filename) {
    // 这里假设我们的分片大小为10MB
    constexpr size_t defaultPartitionSize = 10 * 1024 * 1024 / sizeof(int64_t); // 10MB in terms of int64_t numbers
    size_t fileSize = std::filesystem::file_size(filename);
    size_t totalNumbers = fileSize / sizeof(int64_t);
    return std::min(totalNumbers, defaultPartitionSize);
}

std::vector<int64_t> FileHandler::readPartition(const std::string& filename, size_t offset, size_t partitionSize) {
    std::ifstream file(filename, std::ios::binary | std::ios::in);
    
    // 先初始化一个空的vector
    std::vector<int64_t> data;

    if (file) {
        file.seekg(offset * sizeof(int64_t));

        // 创建一个临时缓冲区来读取数据
        std::vector<int64_t> buffer(partitionSize);
        file.read(reinterpret_cast<char*>(buffer.data()), partitionSize * sizeof(int64_t));

        // 使用gcount()获取实际读取的字符数，并根据此值调整vector的大小
        buffer.resize(file.gcount() / sizeof(int64_t));
        
        // 为"data"分配缓冲区中读取的实际数据
        data = buffer;

        file.close();
    }
    
    return data;
}


void FileHandler::saveTempData(const std::vector<int64_t>& data, const std::string& tempFilename) {
    std::ofstream file(tempFilename, std::ios::binary | std::ios::out);
    
    if (file) {
        file.write(reinterpret_cast<const char*>(data.data()), data.size() * sizeof(int64_t));
        file.close();
    }
}

void FileHandler::mergeToFinalOutput(const std::vector<std::string>& tempFiles, const std::string& outputFilename) {
    // ... 
    // 此处应该包含归并多个临时文件的逻辑，但它可能会很长并且与Merger的工作重叠。
    // 如果您确实需要这部分的代码，可以单独询问。
}

