////
// @file integer.cc
// @brief
// 实现压缩整数表示
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include <db/integer.h>

namespace db {
// 编码
bool Integer::encode(char *buf, size_t len) const
{
    if (value_ <= OneByteLimit && len >= 1) {
        *buf = static_cast<char>(value_);
        return true;
    } else if (value_ <= TwoByteLimit && len >= 2) {
        unsigned short reduced =
            htobe16(static_cast<unsigned short>(value_) | 0x4000);
        memcpy(buf, &reduced, 2);
        return true;
    } else if (value_ <= FourByteLimit && len >= 4) {
        unsigned int reduced =
            htobe32(static_cast<unsigned int>(value_) | 0x80000000);
        memcpy(buf, &reduced, 4);
        return true;
    } else if (value_ <= EightByteLimit && len >= 8) {
        unsigned long long reduced = htobe64(
            static_cast<unsigned long long>(value_) | 0xC000000000000000);
        memcpy(buf, &reduced, 8);
        return true;
    }
    return false;
}

// 解码
bool Integer::decode(char *buf, size_t len)
{
    if (buf == NULL || len == 0) return false;
    unsigned char first = *buf;
    unsigned char type = (first >> 6) & 0x03;
    first &= 0x3F;

    switch (type) {
    case 0:
        value_ = static_cast<unsigned long long>(first);
        return true;
    case 1:
        if (len <= 1) return false;
        {
            unsigned short value;
            *((char *) &value) = first;
            memcpy((char *) &value + 1, buf + 1, 1);
            value_ = be16toh(value);
        }
        return true;
    case 2:
        if (len <= 3) return false;
        {
            unsigned int value;
            *((char *) &value) = first;
            memcpy((char *) &value + 1, buf + 1, 3);
            value_ = be32toh(value);
        }
        return true;
    case 3:
        if (len <= 7) return false;
        {
            unsigned long long value;
            *((char *) &value) = first;
            memcpy((char *) &value + 1, buf + 1, 7);
            value_ = be64toh(value);
        }
        return true;
    default:
        return false;
    }
}
} // namespace db
