#include "Sorter.h"
#include <mutex>
#include <fstream>
#include <algorithm>
#include <sstream>

Sorter::Sorter() : tempFileCount(0) {}

std::vector<int64_t> Sorter::sortData(const std::vector<int64_t>& data) {
    std::vector<int64_t> sortedData = data;  // 复制数据
    std::sort(sortedData.begin(), sortedData.end());
    return sortedData;
}

std::string Sorter::saveSortedData(const std::vector<int64_t>& data, const std::string& tempDir) {
    std::ostringstream oss;
    size_t currentFileCount;
    
    {
        std::unique_lock<std::mutex> lock(fileCountMutex);
        currentFileCount = tempFileCount++;
    }

    oss << tempDir << "/temp_sorted_" << currentFileCount << ".dat";
    std::string tempFilename = oss.str();
    
    std::ofstream file(tempFilename, std::ios::binary | std::ios::out);
    if (file) {
        file.write(reinterpret_cast<const char*>(data.data()), data.size() * sizeof(int64_t));
        file.close();
    }
    
    return tempFilename;
}
