////
// @file record.h
// @brief
// 定义数据库记录
// 采用类似MySQL的记录方案，一条记录分为四个部分：
// Header+记录总长度+字段长度数组+字段
// 1. 记录总长度从最开始算；
// 2. 字段长度是一个逆序变长数组，记录每条记录从Header开始的头部偏移量位置；
// 3. Header存放一些相关信息，1B；
// 4. 然后是各字段顺序摆放；
//
// Header中1B的表示如下：
// | T | M | x | x | x | x | x | x |
//   ^   ^
//   |   |
//   |   +-- 最小记录
//   +-- tombstone
//
// 记录的分配按照4B对齐，同时要求block头部至少按照4B对齐
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#ifndef __DB_RECORD_H__
#define __DB_RECORD_H__

#include <utility>
#include <vector>
#include "./config.h"
#include "./integer.h"

const int ALIGN_SIZE = 8; // 按8B对齐
#define ALIGN_TO_SIZE(x) ((x + ALIGN_SIZE - 1) / ALIGN_SIZE * ALIGN_SIZE)

const unsigned char RECORD_MASK_TOMBSTONE = 0x04; // tombstone掩码
const unsigned char RECORD_MASK_FULL = 0x03;      // 记录是否完整

const unsigned char RECORD_FULL_ALL = 0x00;   // 记录完整
const unsigned char RECORD_FULL_START = 0x01; // 记录开始
const unsigned char RECORD_FULL_MID = 0x02;   // 记录中间
const unsigned char RECORD_FULL_END = 0x03;   // 记录结束

struct iovec
{
    void *iov_base; /* Pointer to data.  */
    size_t iov_len; /* Length of data.  */
};

namespace db {

// 物理记录
class Record
{
  public:
    static const int HEADER_SIZE = 1; // 头部1B

  public:
    unsigned char *buffer_; // 记录buffer
    unsigned short length_; // buffer长度

  public:
    Record()
        : buffer_(NULL)
        , length_(0)
    {}

    // 关联buffer
    inline void attach(unsigned char *buffer, unsigned short length)
    {
        buffer_ = buffer;
        length_ = length;
    }
    inline void detach()
    {
        buffer_ = nullptr;
        length_ = 0;
    }
    // 整个记录长度+header偏移量
    static size_t size(std::vector<struct iovec> &iov);

    // 向buffer里写各个域，返回按照对齐后的长度
    bool set(std::vector<struct iovec> &iov, const unsigned char *header);
    // 从buffer拷贝各字段
    bool get(std::vector<struct iovec> &iov, unsigned char *header);
    // 从buffer拷贝某个字段
    bool getByIndex(char *buffer, unsigned int *len, unsigned int index);
    // 从buffer引用各字段
    bool ref(std::vector<struct iovec> &iov, unsigned char *header);
    // 从buffer引用某个字段
    bool
    refByIndex(unsigned char **buffer, unsigned int *len, unsigned int index);
    // TODO:
    void dump(char *buf, size_t len);

    // 获得记录总长度，包含头部+变长偏移数组+长度+记录
    size_t length();
    // 分配长度
    inline size_t allocLength() { return length_; }
    // 获取记录字段个数
    size_t fields();
    // 变长偏移数组的起始位置
    size_t startOfoffsets();
    // 记录起始位置
    size_t startOfFields();

    // 标记TomeStone
    inline void die() { *buffer_ |= RECORD_MASK_TOMBSTONE; }
    // 判断是否活跃
    inline bool isactive() { return !(*buffer_ & RECORD_MASK_TOMBSTONE); }

    // 判断是否完整
    inline bool isfull()
    {
        return (*buffer_ & RECORD_MASK_FULL) == RECORD_FULL_ALL;
    }
    // 判定是否是开始
    inline bool isstart()
    {
        return (*buffer_ & RECORD_MASK_FULL) == RECORD_FULL_START;
    }
    // 判定是否是中间
    inline bool ismid()
    {
        return (*buffer_ & RECORD_MASK_FULL) == RECORD_FULL_MID;
    }
    // 判定是结尾
    inline bool isend()
    {
        return (*buffer_ & RECORD_MASK_FULL) == RECORD_FULL_END;
    }
};

} // namespace db

#endif // __DB_RECORD_H__
