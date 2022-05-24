////
// @file integer.h
// @brief
// 压缩整数
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#ifndef __DB_INTEGER_H__
#define __DB_INTEGER_H__

#include <string.h>
#include "./endian.h"

namespace db {

// 按网络字节序编解码
class Integer
{
  private:
    const unsigned long long OneByteLimit = 0x3F;
    const unsigned long long TwoByteLimit = 0x3FFF;
    const unsigned long long FourByteLimit = 0x3FFFFFFF;
    const unsigned long long EightByteLimit = 0x3FFFFFFFFFFFFFFF;

  public:
    unsigned long long value_; // 整数

  public:
    Integer()
        : value_(0)
    {}

    // 设置
    inline unsigned long long get() const { return value_; };
    inline void set(unsigned long long v) { value_ = v; }
    // 返回字节数
    inline int size() const
    {
        if (value_ <= OneByteLimit)
            return 1;
        else if (value_ <= TwoByteLimit)
            return 2;
        else if (value_ <= FourByteLimit)
            return 4;
        else if (value_ <= EightByteLimit)
            return 8;
        else
            return -1;
    }

    // 编码
    bool encode(char *buf, size_t len) const;
    // 解码
    bool decode(char *buf, size_t len);
};

} // namespace db

#endif // __DB_INTEGER_H__
