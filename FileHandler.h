#ifndef FILEHANDLER_H
#define FILEHANDLER_H

#include <string>
#include <vector>

class FileHandler {
public:
    // 构造函数
    FileHandler(const std::string& directory);

    // 从目录获取所有文件列表
    std::vector<std::string> getFileList();

    // 根据文件大小分片, 返回每片的大小
    size_t getPartitionSize(const std::string& filename);

    // 读取文件的一个片段
    std::vector<int64_t> readPartition(const std::string& filename, size_t offset, size_t partitionSize);

    // 保存临时数据到文件
    void saveTempData(const std::vector<int64_t>& data, const std::string& tempFilename);

    // 合并所有临时文件到最终的输出文件
    void mergeToFinalOutput(const std::vector<std::string>& tempFiles, const std::string& outputFilename);

private:
    std::string directory;
};

#endif