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
    if (value_ <= OneByteLimit && len >= 1) 
    {
        *buf = static_cast<char>(value_);
        return true;
    } //1B，第一个字节前两位本就是0，不用处理，直接copy
    else if (value_ <= TwoByteLimit && len >= 2) 
    {
        unsigned short reduced =
            htobe16(static_cast<unsigned short>(value_) | 0x4000);//主机字节序变为大端字节序
        memcpy(buf, &reduced, 2);
        return true;
    }//2B，
    else if (value_ <= FourByteLimit && len >= 4) {
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
    unsigned char first = *buf;//buffer第一个字节的内容
    unsigned char type = (first >> 6) & 0x03;//比较第一个字节的前两位
    first &= 0x3F;//第一个字节将前两位置为0，记录实际存储的内容

    switch (type) {
    case 0://1B
        value_ = static_cast<unsigned long long>(first);//第一个字节剩下的内容即存储的内容
        return true;
    case 1://2B
        if (len <= 1) return false;
        {
            unsigned short value;
            *((char *) &value) = first;//value是个整数，但存储的内容实际是很多二进制，将第一个字节存进value
            memcpy((char *) &value + 1, buf + 1, 1);//后面的直接copy
            value_ = be16toh(value);//大端字节序转化为主机字节序
        }
        return true;
    //以下同理
    case 2://4B
        if (len <= 3) return false;
        {
            unsigned int value;
            *((char *) &value) = first;
            memcpy((char *) &value + 1, buf + 1, 3);
            value_ = be32toh(value);
        }
        return true;
    case 3://8B
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
