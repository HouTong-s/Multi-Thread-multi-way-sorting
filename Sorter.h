#include <vector>
#include <string>

class Sorter {
public:
    // 构造函数
    Sorter();

    // 对给定的数据片段进行排序
    std::vector<int64_t> sortData(const std::vector<int64_t>& data);

    // 保存排序后的数据到临时文件，并返回文件名
    std::string saveSortedData(const std::vector<int64_t>& data, const std::string& tempDir);

private:
    int tempFileCount;  // 用于生成临时文件名
};
