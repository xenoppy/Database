////
// @file table.h
// @brief
// 存储管理
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#ifndef __DB_TABLE_H__
#define __DB_TABLE_H__

#include <string>
#include <vector>
#include "./record.h"
#include "./schema.h"
#include "./block.h"
#include "./buffer.h"

namespace db {

////
// @brief
// 表操作接口
//
class Table
{
  public:
    // 表的迭代器
    struct BlockIterator
    {
        DataBlock block;
        BufDesp *bufdesp;

        BlockIterator();
        ~BlockIterator();
        BlockIterator(const BlockIterator &other);

        // 前置操作
        BlockIterator &operator++();
        // 后置操作
        BlockIterator operator++(int);
        // 数据块指针
        DataBlock *operator->();

        // 释放buffer
        void release();
    };

  public:
    std::string name_;   // 表名
    RelationInfo *info_; // 表的元数据
    unsigned int maxid_; // 最大的blockid
    unsigned int idle_;  // 空闲链
    unsigned int first_; // 数据链

  public:
    Table()
        : info_(NULL)
        , maxid_(0)
        , idle_(0)
        , first_(0)
    {}

    // 打开一张表
    int open(const char *name);

    // 采用枚举的方式定位一个key在哪个block
    unsigned int locate(void *keybuf, unsigned int len);
    // 定位一个block后，插入一条记录
    int insert(unsigned int blkid, std::vector<struct iovec> &iov);
    int remove(unsigned int blkid, void *keybuf, unsigned int len);
    int update(unsigned int blkid, std::vector<struct iovec> &iov);
    // btree搜索
    unsigned int search(void *keybuf, unsigned int len);

    // 返回表上总的记录数目
    size_t recordCount();
    // 返回表上数据块个数
    unsigned int dataCount();
    // 返回表上空闲块个数
    unsigned int idleCount();

    // block迭代器
    BlockIterator beginblock();
    BlockIterator endblock();

    // 新分配一个block，返回blockid，但并没有将该block插入数据链上
    unsigned int allocate();
    // 回收一个block
    void deallocate(unsigned int blockid);
};

inline bool
operator==(const Table::BlockIterator &x, const Table::BlockIterator &y)
{
    if (x.block.table_ != y.block.table_)
        return false;
    else if (x.block.buffer_ == y.block.buffer_)
        return true;
    else
        return false;
}
inline bool
operator!=(const Table::BlockIterator &x, const Table::BlockIterator &y)
{
    if (x.block.table_ != y.block.table_)
        return true;
    else if (x.block.buffer_ != y.block.buffer_)
        return true;
    else
        return false;
}

} // namespace db

#endif // __DB_TABLE_H__
