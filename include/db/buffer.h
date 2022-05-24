////
// @file buffer.h
// @brief
// 数据库buffer层
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#ifndef __DB_BUFFER_H__
#define __DB_BUFFER_H__

#include <string>
#include <map>
#include <atomic>

namespace db {
// buffer描述符
struct BufDesp
{
    BufDesp *next;                  // 下一个描述符
    BufDesp *prev;                  // 前一个描述符
    const char *name;               // 表名
    unsigned char *buffer;          // 缓冲
    unsigned int blockid;           // block的id
    unsigned short size;            // 大小
    unsigned char type;             // 类型
    std::atomic<unsigned char> ref; // 引用计数

    BufDesp()
        : next(NULL)
        , prev(NULL)
        , name(NULL)
        , blockid(0)
        , buffer(NULL)
        , size(0)
        , type(0)
    {}
    inline void addref() { ++ref; }
    inline void relref() { --ref; }
};

////
// Buffer管理系统所有的buffer
// 1. 向上层提供borrow接口，出借buffer；
// 2. 上层借用buffer后，用完后需要即可归还，不要长时间持有buffer；
// 3. 上层调用write接口写，调用release释放buffer；
// 4. 完整的实现，Buffer应该由一个协程控制，上层用户通过rpc请求block；
// 5. Buffer应该自主刷盘，同时设置两个通道
// TODO: 日志刷盘
class FilePool;
class Buffer
{
  public:
    using BlockMap = std::map<std::pair<std::string, unsigned int>, BufDesp *>;

    unsigned char BUFFER_LOCKED = 0x1; // 锁定buffer
    unsigned char BUFFER_DIRTY = 0x2;  // 脏buffer
    unsigned char BUFFER_READY = 0x4;  // 可回写buffer

  private:
    BufDesp *idle_;         // 空闲buffer
    BufDesp lru_;           // 最近访问队列
    BlockMap map_;          // 块表 table+blockid --> BufDesp
    unsigned char *buffer_; // 所有buffer
    FilePool *filepool_;    // 文件池
    size_t idleCount_;      // 空闲块个数

  public:
    Buffer()
        : idle_(NULL)
        , buffer_(NULL)
        , filepool_(NULL)
        , idleCount_(0)
    {}
    ~Buffer();

    // 初始化缺省大小为256MB
    void init(FilePool *fp, size_t defaultSize = 256);
    // 用户请求一个block
    BufDesp *borrow(const char *table, unsigned int blockid);
    // 写一个block
    void writeBuf(BufDesp *desp);
    // 释放block
    inline void releaseBuf(BufDesp *desp) { desp->relref(); }

    // 空闲块个数
    inline size_t idles() { return idleCount_; }
    // 分配buffer
    BufDesp *allocFromIdle();
    // prepend到lru头部
    void prependLru(BufDesp *ptr);
};

// 全局buffer管理器
extern Buffer kBuffer;
} // namespace db

#endif // __DB_BUFFER_H__