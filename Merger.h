#include <string>
#include <vector>
#include <queue>

class Merger {
public:
    // 构造函数
    Merger();

    // 执行两两归并操作，返回归并后的临时文件列表
    std::vector<std::string> twoWayMerge(const std::vector<std::string>& tempFiles, const std::string& tempDir);

    // 合并所有临时文件到最终的输出文件
    void mergeToFinalOutput(const std::vector<std::string>& tempFiles, const std::string& outputFilename);

private:
    int mergeFileCount;  // 用于生成临时归并文件名

    struct MergeElement {
        int64_t value;
        size_t fileIndex;
        bool operator<(const MergeElement& other) const {
            return value > other.value;  // 注意：为了使优先队列为最小堆，我们需要反转比较
        }
    };

    std::string getNextMergeFileName(const std::string& tempDir);
};
