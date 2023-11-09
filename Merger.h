#ifndef MERGER_H
#define MERGER_H

#include <string>
#include <vector>
#include <queue>
#include <mutex>
#include <atomic>

struct MergeElement {
    int64_t value;
    size_t fileIndex;
    bool operator<(const MergeElement& other) const {
        return value > other.value;  // 注意：为了使优先队列为最小堆，我们需要反转比较
    }
};

class Merger {
public:
    // 构造函数
    Merger();

    // 多路归并函数，合并多个文件块到一个有序文件
    std::string multiWayMergeThreadSafe(const std::vector<std::string>& fileNames, const std::string& tempDir);

    std::string twoWayMergeThreadSafe(const std::string& file1Name, const std::string& file2Name, const std::string& tempDir);

private:

    std::atomic<size_t> mergeFileCount; // 用于生成临时归并文件名
    std::mutex fileMutex;

    struct MergeElement {
        int64_t value;
        size_t fileIndex;
        bool operator<(const MergeElement& other) const {
            return value > other.value;  // 注意：为了使优先队列为最小堆，我们需要反转比较
        }
    };

    std::string getNextMergeFileName(const std::string& tempDir);
};

#endif 
