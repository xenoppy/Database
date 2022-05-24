////
// @file block.h
// @brief
// 定义block
// block是记录、索引的存储单元。在MySQL和HBase中，存储单元与分配单位是分开的，一般来说，
// 最小分配单元要比block大得多。
// block的布局如下，每个slot占用2B，这要求block最大为64KB。由于记录和索引要求按照4B对
// 齐，BLOCK_DATA、BLOCK_TRAILER也要求8B对齐。
//
// +--------------------+
// |   common header    |
// +--------------------+
// |  data/index header |
// +--------------------+ <--- BLOCK_DATA
// |                    |
// |     data/index     |
// |                    |
// +--------------------+ <--- BLOCK_FREE
// |     free space     |
// +--------------------+
// |       slots        |
// +--------------------+ <--- BLOCK_TRAILER
// |      trailer       |
// +--------------------+
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#ifndef __DB_BLOCK_H__
#define __DB_BLOCK_H__

#include "./checksum.h"
#include "./endian.h"
#include "./timestamp.h"
#include "./record.h"
#include "./schema.h"
#include "./datatype.h"

namespace db {

const unsigned short BLOCK_TYPE_IDLE = 0;  // 空闲
const unsigned short BLOCK_TYPE_SUPER = 1; // 超块
const unsigned short BLOCK_TYPE_DATA = 2;  // 数据
const unsigned short BLOCK_TYPE_INDEX = 3; // 索引
const unsigned short BLOCK_TYPE_META = 4;  // 元数据
const unsigned short BLOCK_TYPE_LOG = 5;   // wal日志

const unsigned int SUPER_SIZE = 1024 * 4;  // 超块大小为4KB
const unsigned int BLOCK_SIZE = 1024 * 16; // 一般块大小为16KB

#if BYTE_ORDER == LITTLE_ENDIAN
static const int MAGIC_NUMBER = 0x31306264; // magic number
#else
static const int MAGIC_NUMBER = 0x64623031; // magic number
#endif

// TODO: LSN
// 公共头部
struct CommonHeader
{
    unsigned int magic;       // magic number(4B)
    unsigned int spaceid;     // 表空间id(4B)
    unsigned short type;      // block类型(2B)
    unsigned short freespace; // 空闲记录链表(2B)
};

// slots结构
struct Slot
{
    unsigned short offset; // 记录偏移量
    unsigned short length; // 记录大小
};

// 尾部
struct Trailer
{
    Slot slots[1];         // slots数组
    unsigned int checksum; // 校验和(4B)
};

// 超块头部
struct SuperHeader : CommonHeader
{
    unsigned int first;      // 第1个数据块(4B)
    long long stamp;         // 时戳(8B)
    unsigned int idle;       // 空闲块(4B)
    unsigned int datacounts; // 数据块个数
    unsigned int idlecounts; // 空闲块个数
    unsigned int self;       // 本块id(4B)
    unsigned int maxid;      // 最大的blockid(4B)
    unsigned int pad;        // 填充位(4B)
    long long records;       // 记录数目(8B)
};

// 空闲块头部
struct IdleHeader : CommonHeader
{
    unsigned int next; // 后继指针(4B)
};

// 数据块头部
struct DataHeader : CommonHeader
{
    unsigned int next;       // 下一个数据块(4B)
    long long stamp;         // 时戳(8B)
    unsigned short slots;    // slots[]长度(2B)
    unsigned short freesize; // 空闲空间大小(2B)
    unsigned int self;       // 本块id(4B)
};

// 元数据块头部
using MetaHeader = DataHeader;

////
// @brief
// 公共block
//
class Block
{
  public:
    unsigned char *buffer_; // block对应的buffer

  public:
    Block()
        : buffer_(NULL)
    {}

    // 关联buffer
    inline void attach(unsigned char *buffer) { buffer_ = buffer; }
    inline void detach() { buffer_ = NULL; }

    // 设定magic
    inline void setMagic()
    {
        CommonHeader *header = reinterpret_cast<CommonHeader *>(buffer_);
        header->magic = MAGIC_NUMBER;
    }
    inline int getMagic()
    {
        CommonHeader *header = reinterpret_cast<CommonHeader *>(buffer_);
        return header->magic;
    }

    // 获取表空间id
    inline unsigned int getSpaceid()
    {
        SuperHeader *header = reinterpret_cast<SuperHeader *>(buffer_);
        return be32toh(header->spaceid);
    }
    // 设定表空间id
    inline void setSpaceid(unsigned int spaceid)
    {
        SuperHeader *header = reinterpret_cast<SuperHeader *>(buffer_);
        header->spaceid = htobe32(spaceid);
    }

    // 获取类型
    inline unsigned short getType()
    {
        SuperHeader *header = reinterpret_cast<SuperHeader *>(buffer_);
        return be16toh(header->type);
    }
    // 设定类型
    inline void setType(unsigned short type)
    {
        SuperHeader *header = reinterpret_cast<SuperHeader *>(buffer_);
        header->type = htobe16(type);
    }

    // 获取freespace
    inline unsigned short getFreeSpace()
    {
        SuperHeader *header = reinterpret_cast<SuperHeader *>(buffer_);
        return be16toh(header->freespace);
    }
};

////
// @brief
// 超块
//
class SuperBlock : public Block
{
  public:
    // 关联buffer
    inline void attach(unsigned char *buffer) { buffer_ = buffer; }
    // 清超块
    void clear(unsigned short spaceid);

    // 获取第1个数据块
    inline unsigned int getFirst()
    {
        SuperHeader *header = reinterpret_cast<SuperHeader *>(buffer_);
        return be32toh(header->first);
    }
    // 设定数据块链头
    inline void setFirst(unsigned int first)
    {
        SuperHeader *header = reinterpret_cast<SuperHeader *>(buffer_);
        header->first = htobe32(first);
    }

    // 获取空闲块
    inline unsigned int getIdle()
    {
        SuperHeader *header = reinterpret_cast<SuperHeader *>(buffer_);
        return be32toh(header->idle);
    }
    // 设定空闲块链头
    inline void setIdle(unsigned int idle)
    {
        SuperHeader *header = reinterpret_cast<SuperHeader *>(buffer_);
        header->idle = htobe32(idle);
    }

    // 获取最大blockid
    inline unsigned int getMaxid()
    {
        SuperHeader *header = reinterpret_cast<SuperHeader *>(buffer_);
        return be32toh(header->maxid);
    }
    // 设定最大blockid
    inline void setMaxid(unsigned int maxid)
    {
        SuperHeader *header = reinterpret_cast<SuperHeader *>(buffer_);
        header->maxid = htobe32(maxid);
    }

    // 获取时戳
    inline TimeStamp getTimeStamp()
    {
        SuperHeader *header = reinterpret_cast<SuperHeader *>(buffer_);
        TimeStamp ts;
        ::memcpy(&ts, &header->stamp, sizeof(TimeStamp));
        *((long long *) &ts) = be64toh(*((long long *) &ts));
        return ts;
    }
    // 设定时戳
    inline void setTimeStamp()
    {
        SuperHeader *header = reinterpret_cast<SuperHeader *>(buffer_);
        TimeStamp ts;
        ts.now();
        *((long long *) &ts) = htobe64(*((long long *) &ts));
        ::memcpy(&header->stamp, &ts, sizeof(TimeStamp));
    }

    // 设置datacounts
    inline void setDataCounts(unsigned int counts)
    {
        SuperHeader *header = reinterpret_cast<SuperHeader *>(buffer_);
        header->datacounts = htobe32(counts);
    }
    // 获取datacounts
    inline unsigned int getDataCounts()
    {
        SuperHeader *header = reinterpret_cast<SuperHeader *>(buffer_);
        return be32toh(header->datacounts);
    }

    // 设置idlecounts
    inline void setIdleCounts(unsigned int counts)
    {
        SuperHeader *header = reinterpret_cast<SuperHeader *>(buffer_);
        header->idlecounts = htobe32(counts);
    }
    // 获取datacounts
    inline unsigned int getIdleCounts()
    {
        SuperHeader *header = reinterpret_cast<SuperHeader *>(buffer_);
        return be32toh(header->idlecounts);
    }

    // 设置self
    inline void setSelf()
    {
        MetaHeader *header = reinterpret_cast<MetaHeader *>(buffer_);
        header->self = htobe32(0);
    }
    // 获取self
    inline unsigned int getSelf()
    {
        MetaHeader *header = reinterpret_cast<MetaHeader *>(buffer_);
        return be32toh(header->self);
    }

    // 设定checksum
    inline void setChecksum()
    {
        Trailer *trailer =
            reinterpret_cast<Trailer *>(buffer_ + SUPER_SIZE - sizeof(Trailer));
        trailer->checksum = 0; // 先要清0，以防checksum计算在内
        trailer->checksum = checksum32(buffer_, SUPER_SIZE);
    }
    // 获取checksum
    inline unsigned int getChecksum()
    {
        Trailer *trailer =
            reinterpret_cast<Trailer *>(buffer_ + SUPER_SIZE - sizeof(Trailer));
        return trailer->checksum;
    }
    // 检验checksum
    inline bool checksum()
    {
        unsigned int sum = 0;
        sum = checksum32(buffer_, SUPER_SIZE);
        return !sum;
    }
    // 设定空闲链头
    inline void setFreeSpace(unsigned short freespace)
    {
        SuperHeader *header = reinterpret_cast<SuperHeader *>(buffer_);
        header->freespace = htobe16(freespace);
    }

    inline void setRecords(long long s)
    {
        SuperHeader *header = reinterpret_cast<SuperHeader *>(buffer_);
        header->records = htobe64(s);
    }
    inline long long getRecords()
    {
        SuperHeader *header = reinterpret_cast<SuperHeader *>(buffer_);
        return be64toh(header->records);
    }
};

////
// @brief
// 数据块
//
class MetaBlock : public Block
{
  public:
    // 清数据块
    void clear(unsigned short spaceid, unsigned int self, unsigned short type);

    // 获取空闲块
    inline unsigned int getNext()
    {
        MetaHeader *header = reinterpret_cast<MetaHeader *>(buffer_);
        return be32toh(header->next);
    }
    // 设定block链头
    inline void setNext(unsigned int next)
    {
        MetaHeader *header = reinterpret_cast<MetaHeader *>(buffer_);
        header->next = htobe32(next);
    }

    // 获取时戳
    inline TimeStamp getTimeStamp()
    {
        MetaHeader *header = reinterpret_cast<MetaHeader *>(buffer_);
        TimeStamp ts;
        ts.retrieve(header->stamp);
        return ts;
    }
    // 设定时戳
    inline void setTimeStamp()
    {
        MetaHeader *header = reinterpret_cast<MetaHeader *>(buffer_);
        TimeStamp ts;
        ts.now();
        ts.store(&header->stamp);
    }

    // 获取空闲空间大小
    inline unsigned short getFreeSize()
    {
        MetaHeader *header = reinterpret_cast<MetaHeader *>(buffer_);
        return be16toh(header->freesize);
    }
    // 设定空闲空间大小
    inline void setFreeSize(unsigned short size)
    {
        MetaHeader *header = reinterpret_cast<MetaHeader *>(buffer_);
        header->freesize = htobe16(size);
    }

    // 设置slots数目
    inline void setSlots(unsigned short slots)
    {
        MetaHeader *header = reinterpret_cast<MetaHeader *>(buffer_);
        header->slots = htobe16(slots);
    }
    // 获取slots数目
    inline unsigned short getSlots()
    {
        MetaHeader *header = reinterpret_cast<MetaHeader *>(buffer_);
        return be16toh(header->slots);
    }

    // 设置self
    inline void setSelf(unsigned int id)
    {
        MetaHeader *header = reinterpret_cast<MetaHeader *>(buffer_);
        header->self = htobe32(id);
    }
    // 获取self
    inline unsigned int getSelf()
    {
        MetaHeader *header = reinterpret_cast<MetaHeader *>(buffer_);
        return be32toh(header->self);
    }

    // 设定checksum
    inline void setChecksum()
    {
        Trailer *trailer =
            reinterpret_cast<Trailer *>(buffer_ + BLOCK_SIZE - sizeof(Trailer));
        trailer->checksum = 0; // 先要清0，防止checksum计算出错
        trailer->checksum = checksum32(buffer_, BLOCK_SIZE);
    }
    // 获取checksum
    inline unsigned int getChecksum()
    {
        Trailer *trailer =
            reinterpret_cast<Trailer *>(buffer_ + BLOCK_SIZE - sizeof(Trailer));
        return trailer->checksum;
    }
    // 检验checksum
    inline bool checksum()
    {
        unsigned int sum = 0;
        sum = checksum32(buffer_, BLOCK_SIZE);
        return !sum;
    }

    // 获取trailer大小
    inline unsigned short getTrailerSize()
    {
        MetaHeader *header = reinterpret_cast<MetaHeader *>(buffer_);
        return ALIGN_TO_SIZE(
            be16toh(header->slots) * sizeof(Slot) + sizeof(unsigned int));
    }
    // 获取slots[]指针
    inline Slot *getSlotsPointer()
    {
        MetaHeader *header = reinterpret_cast<MetaHeader *>(buffer_);
        return reinterpret_cast<Slot *>(
            buffer_ + BLOCK_SIZE - sizeof(unsigned int) -
            be16toh(header->slots) * sizeof(Slot));
    }
    // 获取freespace空间大小
    inline unsigned short getFreespaceSize()
    {
        MetaHeader *header = reinterpret_cast<MetaHeader *>(buffer_);
        return BLOCK_SIZE - getTrailerSize() - be16toh(header->freespace);
    }
    // 设定freespace偏移量
    inline void setFreeSpace(unsigned short freespace)
    {
        MetaHeader *header = reinterpret_cast<MetaHeader *>(buffer_);
        // 判断是不是超过了Trailer的界限
        unsigned short upper = BLOCK_SIZE - getTrailerSize();
        if (freespace >= upper) freespace = 0; //超过界限则设置为0
        header->freespace = htobe16(freespace);
    }

    // 分配一个空间，直接返回指针，后续需要重新排列slots[]，second表示是否需要reorder
    std::pair<unsigned char *, bool>
    allocate(unsigned short space, unsigned short index);
    // 给定一条记录的槽位下标，回收一条记录，回收slots[]中分配的槽位
    void deallocate(unsigned short index);
    // 回收删除记录的资源，回收了slots资源，后续需要重排slots[]
    void shrink();
    // 对slots[]重排
    inline void reorder(DataType *type, unsigned int key)
    {
        type->sort(buffer_, key);
    }

    // 引用slots[]
    bool refslots(unsigned short index, Record &record)
    {
        if (buffer_ == nullptr || index >= getSlots()) return false;

        Slot *slots = getSlotsPointer();
        record.attach(
            buffer_ + be16toh(slots[index].offset),
            be16toh(slots[index].length));
        return true;
    }
};

////
// @brief
// DataBlock直接从MetaBlock派生
//
class Table;
class DataBlock : public MetaBlock
{
  public:
    struct RecordIterator
    {
        DataBlock *block;
        Record record;
        unsigned short index;

        RecordIterator();
        ~RecordIterator();
        RecordIterator(const RecordIterator &other);

        // 前置操作
        RecordIterator &operator++();
        // 后置操作
        RecordIterator operator++(int);
        // 前置操作
        RecordIterator &operator--();
        // 后置操作
        RecordIterator operator--(int);
        // 加减操作
        RecordIterator &operator+=(int);
        RecordIterator &operator-=(int);
        // 数据块指针
        Record *operator->();
    };

  public:
    Table *table_; // 指向table

  public:
    DataBlock()
        : table_(NULL)
    {}

    // 设定table
    inline void setTable(Table *table) { table_ = table; }
    // 获取table
    inline Table *getTable() { return table_; }

    // 查询记录
    // 给定一个关键字，从slots[]上搜索到该记录：
    // 1. 根据meta确定key的位置；
    // 2. 采用二分查找在slots[]上寻找
    // 返回值：
    // 返回lowerbound
    unsigned short searchRecord(void *key, size_t size);
    // 插入记录
    // 在block中插入记录，步骤如下：
    // 1. 先检查空间是否足够，如果够，则插入，然后重新排序；
    // 2. 不够，根据key寻找插入位置，从这个位置将block劈开；
    // 3. 计算劈开后前面序列的长度，然后判断新记录加入后空间是否足够，够则插入；
    // 4. 先将新的记录插入一个新的block，然后挪动原有记录到新的block；
    // 返回值：
    // true - 表示记录完全插入
    // false - 表示block被分裂
    std::pair<bool, unsigned short>
    insertRecord(std::vector<struct iovec> &iov);
    // 修改记录
    // 修改一条存在的记录
    // 先标定原记录为tomestone，然后插入新记录
    bool updateRecord(std::vector<struct iovec> &iov);
    // 分裂块位置
    // 给定新增的记录大小和位置，计算从何处开始分裂该block
    // 1. 先按照键排序
    // 2. 从0开始枚举所有记录，累加长度，何时超过一半，即为分裂位置
    std::pair<unsigned short, bool>
    splitPosition(size_t space, unsigned short index);
    // 拷贝一条记录
    // 如果新block空间不够，简单地返回false
    bool copyRecord(Record &record);

    // 记录分配长度
    unsigned short requireLength(std::vector<struct iovec> &iov);

    RecordIterator beginrecord();
    RecordIterator endrecord();
};

inline bool operator==(
    const DataBlock::RecordIterator &x,
    const DataBlock::RecordIterator &y)
{
    if (x.block == nullptr && y.block == nullptr)
        return true;
    else if (x.block == nullptr || y.block == nullptr)
        return false;
    else if (x.block == y.block && x.index == y.index)
        return true;
    else
        return false;
}
inline bool operator!=(
    const DataBlock::RecordIterator &x,
    const DataBlock::RecordIterator &y)
{
    return !operator==(x, y);
}
// 以下操作要求block相同
inline bool operator<(
    const DataBlock::RecordIterator &x,
    const DataBlock::RecordIterator &y)
{
    // ASSERT(x.block == y.block);
    return x.index < y.index;
}
inline bool operator>(
    const DataBlock::RecordIterator &x,
    const DataBlock::RecordIterator &y)
{
    // ASSERT(x.block == y.block);
    return x.index > y.index;
}
inline bool operator<=(
    const DataBlock::RecordIterator &x,
    const DataBlock::RecordIterator &y)
{
    // ASSERT(x.block == y.block);
    return x.index <= y.index;
}
inline bool operator>=(
    const DataBlock::RecordIterator &x,
    const DataBlock::RecordIterator &y)
{
    // ASSERT(x.block == y.block);
    return x.index >= y.index;
}

} // namespace db

#endif // __DB_BLOCK_H__
