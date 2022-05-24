////
// @file checksum.h
// @brief
// inet校验和
// 按照网络字节序输出unsigned short校验和
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#ifndef __DB_CHECKSUM_H__
#define __DB_CHECKSUM_H__

#include "./endian.h"

namespace db {

// 网络字节序checksum
inline unsigned short checksum(const unsigned char *buf, int len)
{
    unsigned int sum = 0;
    while (len > 1) {
        sum += (*buf++) << 8; // 高位
        sum += *buf++;        // 低位
        len -= sizeof(unsigned short);
    }
    if (len) { sum += (*buf) << 8; }
    return htons((unsigned short) (~sum) + 1);
}
inline unsigned int checksum32(const unsigned char *buf, int len)
{
    unsigned long long sum = 0;

    while (len > 3) {
        sum += (*buf++) << 24;
        sum += (*buf++) << 16;
        sum += (*buf++) << 8;
        sum += *buf++;
        len -= sizeof(unsigned int);
    }
    // clang-format off
    if (len) { sum += (*buf++) << 24; --len;
        if (len) { sum += (*buf++) << 16; --len;
            if (len) { sum += (*buf++) << 8; --len; } } }
    // clang-format on

    return htonl(static_cast<unsigned int>(~sum) + 1);
}

} // namespace db

#endif // __DB_CHECKSUM_H__
