////
// @file timestamp.h
// @brief
// 时戳
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#ifndef __DB_TIMESTAMP_H__
#define __DB_TIMESTAMP_H__

#include <chrono>
#include <utility>

namespace db {

// 时戳
// TimeStamp不能用于表示1970年之前的时间
struct TimeStamp
{
    std::chrono::system_clock::time_point stamp_;
    void now() { stamp_ = std::chrono::system_clock::now(); }
    bool toString(char *buffer, size_t size) const;
    void fromString(const char *time);
    void store(long long *buf);   // 存储到long long中
    void retrieve(long long buf); // 从long long获取
};

bool operator<(const TimeStamp &lhs, const TimeStamp &rhs);
bool operator>(const TimeStamp &lhs, const TimeStamp &rhs);
bool operator<=(const TimeStamp &lhs, const TimeStamp &rhs);
bool operator>=(const TimeStamp &lhs, const TimeStamp &rhs);
bool operator==(const TimeStamp &lhs, const TimeStamp &rhs);
bool operator!=(const TimeStamp &lhs, const TimeStamp &rhs);

} // namespace db

#endif // __DB_TIMESTAMP_H__
