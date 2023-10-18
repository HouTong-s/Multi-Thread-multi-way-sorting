#include <iostream>
#include <fstream>
#include <random>
#include <string>
#include <filesystem>

void generateData(const std::string& directory, size_t totalFiles, uint64_t totalSize) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int64_t> dis(INT64_MIN, INT64_MAX);
    std::uniform_int_distribution<uint64_t> sizeDis(1024 * 10, 1024 * 1024 * 1024); // 10KB to 1GB

    uint64_t totalSizeGenerated = 0;
    size_t count = 0;

    std::filesystem::create_directory(directory);

    while (count < totalFiles && totalSizeGenerated < totalSize) {
        uint64_t fileSize = sizeDis(gen);
        if (totalSizeGenerated + fileSize > totalSize) {
            fileSize = totalSize - totalSizeGenerated;
        }
        
        std::ofstream file(directory + "/file" + std::to_string(count) + ".dat", std::ios::binary);

        for (uint64_t i = 0; i < fileSize; i += sizeof(int64_t)) {
            int64_t randomValue = dis(gen);
            file.write(reinterpret_cast<char*>(&randomValue), sizeof(int64_t));
        }

        file.close();
        totalSizeGenerated += fileSize;
        ++count;
    }
}

int main(int argc, char* argv[]) {
    
    if (argc < 2) {
        std::cout << "Usage: " << argv[0] << " <data size in GB>" << std::endl;
        return 1;
    }

    uint64_t dataSizeGB = std::stoull(argv[1]);
    const uint64_t totalSize = dataSizeGB * 1024 * 1024 * 1024;

    const std::string dirPath = "data_directory"; 
    const size_t totalFiles = 100000; // 10万个文件

    generateData(dirPath, totalFiles, totalSize);

    std::cout << "Data generation completed!" << std::endl;
    return 0;
}

