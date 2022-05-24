////
// @file timestamp.cc
// @brief
// 实现时辍
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include <stdio.h>
#include <time.h>
#include <string>
#include <db/timestamp.h>
#include <db/endian.h>

namespace db {

bool TimeStamp::toString(char *buffer, size_t size) const
{
    struct tm tm;
    time_t tmt;
    int ms = (int)
            (std::chrono::duration_cast<std::chrono::microseconds>(stamp_.time_since_epoch()).count() % 1000000);
    tmt = std::chrono::system_clock::to_time_t(stamp_);
    localtime_s(&tm, &tmt);
    int ret = snprintf(
        buffer,
        size,
        "%4d_%02d_%02d-%02d:%02d:%02d.%06d", // 26?
        tm.tm_year + 1900,
        tm.tm_mon + 1, // 0 - 11
        tm.tm_mday,
        tm.tm_hour,
        tm.tm_min,
        tm.tm_sec,
        ms);
    return ret > 0;
}

void TimeStamp::fromString(const char *time)
{
    struct tm tm;
    memset(&tm, 0, sizeof(tm));
    tm.tm_year = std::stoi(time) - 1900;
    tm.tm_mon = std::stoi(time + 5) - 1;
    tm.tm_mday = std::stoi(time + 8);
    tm.tm_hour = std::stoi(time + 11);
    tm.tm_min = std::stoi(time + 14);
    tm.tm_sec = std::stoi(time + 17);
    std::chrono::microseconds micro(std::stoi(time + 20));

    time_t tmt;
    tmt = mktime(&tm);
    stamp_ = std::chrono::system_clock::from_time_t(tmt);
    stamp_ += micro;
}

void TimeStamp::store(long long *buf)
{
    static_assert(
        sizeof(this) == sizeof(long long), "sizeof TimeStamp is not long long");
    *buf = htobe64(*((long long *) this));
}
void TimeStamp::retrieve(long long buf)
{
    (*((long long *) this)) = be64toh(buf);
}

bool operator<(const TimeStamp &lhs, const TimeStamp &rhs)
{
    char buf_lhs[512];
    char buf_rhs[512];
    lhs.toString(buf_lhs, 512);
    rhs.toString(buf_rhs, 512);
    int i = 0;
    while (buf_lhs[i]) {
        if (buf_lhs[i] > buf_rhs[i]) {
            return false;
        } else if (buf_lhs[i] < buf_rhs[i]) {
            return true;
        } else {
            i++;
        }
    }
    return false;
}

bool operator>(const TimeStamp &lhs, const TimeStamp &rhs)
{
    char buf_lhs[512];
    char buf_rhs[512];
    lhs.toString(buf_lhs, 512);
    rhs.toString(buf_rhs, 512);
    int i = 0;
    while (buf_lhs[i]) {
        if (buf_lhs[i] < buf_rhs[i]) {
            return false;
        } else if (buf_lhs[i] > buf_rhs[i]) {
            return true;
        } else {
            i++;
        }
    }
    return false;
}

bool operator==(const TimeStamp &lhs, const TimeStamp &rhs)
{
    char buf_lhs[512];
    char buf_rhs[512];
    lhs.toString(buf_lhs, 512);
    rhs.toString(buf_rhs, 512);
    int i = 0;
    while (buf_lhs[i]) {
        if (buf_lhs[i] == buf_rhs[i]) {
            i++;
        } else {
            return false;
        }
    }
    return true;
}

bool operator<=(const TimeStamp &lhs, const TimeStamp &rhs)
{
    return !operator>(lhs, rhs);
}
bool operator>=(const TimeStamp &lhs, const TimeStamp &rhs)
{
    return !operator<(lhs, rhs);
}
bool operator!=(const TimeStamp &lhs, const TimeStamp &rhs)
{
    return !operator==(lhs, rhs);
}

} // namespace db
