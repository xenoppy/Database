////
// @file buffer.cc
// @brief
// 实现buffer
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include <malloc.h> // windows
#include <db/buffer.h>
#include <db/block.h>
#include <db/file.h>

namespace db {
Buffer::~Buffer()
{
    if (buffer_) {
        // 释放所有lru上的描述符，TODO: 恢复？
        while (lru_.next) {
            BufDesp *descriptor = lru_.next;
            lru_.next = descriptor->next;
            delete (descriptor);
        }

        // 释放所有buffer内存
        _aligned_free(buffer_);
    }
}

void Buffer::init(FilePool *fp, size_t size)
{
    // 已经初始化过
    if (buffer_) return;
    filepool_ = fp;

    // 按照4096B对齐，以1MB为单位分配内存
    unsigned char *buffer_ =
        (unsigned char *) _aligned_malloc(size * 1024 * 1024, 4096);

    // 初始化所有block
    BufDesp *prev = NULL;
    idleCount_ = 0;
    for (size_t i = 0; i < size * 1024 * 1024 / BLOCK_SIZE; ++i) {
        idle_ = (BufDesp *) (buffer_ + i * BLOCK_SIZE);
        idle_->next = prev;
        prev = idle_;
        idle_->prev = NULL;
        idle_->size = BLOCK_SIZE;
        idle_->name = NULL;
        idle_->type = 0;
        ++idleCount_;
    }
}

BufDesp *Buffer::allocFromIdle()
{
    // 从idle头部摘下一个buffer
    BufDesp *ptr = idle_;
    idle_ = idle_->next;

    // 分配描述符
    BufDesp *descriptor = new BufDesp;
    descriptor->buffer = (unsigned char *) ptr;
    descriptor->size = BLOCK_SIZE;
    descriptor->type = 0;

    // 插入lru
    prependLru(descriptor);

    return descriptor;
}

void Buffer::prependLru(BufDesp *descriptor)
{
    descriptor->next = lru_.next;
    lru_.next = descriptor;
    if (descriptor->next) descriptor->next->prev = descriptor;
    descriptor->prev = &lru_;
}

BufDesp *Buffer::borrow(const char *table, unsigned int blockid)
{
    // 利用文件池打开表
    File *file = filepool_->open(table);

    // 根据表名+offset查找
    std::pair<const char *, unsigned int> block(table, blockid);
    BlockMap::iterator it = map_.find(block);

    // 找到，将描述符移动到lru头部
    if (it != map_.end()) {
        // 将该描述符从队列中摘下
        BufDesp *prev = it->second->prev;
        prev->next = it->second->next;
        if (prev->next) prev->next->prev = it->second->prev;

        // prepend到lru的头部
        prependLru(it->second);

        // 增加引用计数
        it->second->addref();
        // 返回buffer指针
        return it->second;
    }

    // TODO: 如果空闲空间不够，从lru队列上释放buffer
    if (idle_ == NULL) {
        printf("OOM!!!!");
        return NULL;
    }

    // 然后从idle上分配一个block
    BufDesp *descriptor = allocFromIdle();
    descriptor->name = table;
    descriptor->blockid = blockid;
    // prepend到lru的头部
    prependLru(descriptor);

    // 从文件读数据
    unsigned long long offset =
        blockid == 0 ? 0 : blockid * BLOCK_SIZE + SUPER_SIZE;
    int ret = file->read(offset, (char *) descriptor->buffer, BLOCK_SIZE);
    if (ret) memset(descriptor->buffer, 0, BLOCK_SIZE); // 读取出错，直接清零

    // 将block加入map
    BlockMap::value_type val(
        std::pair<const char *, unsigned int>(table, blockid), descriptor);
    map_.insert(val);

    // 增加引用计数
    descriptor->addref();
    return descriptor;
}

void Buffer::writeBuf(BufDesp *desp)
{
    // 设定dirty
    desp->type |= BUFFER_DIRTY;

    // 将该描述符从队列中摘下
    BufDesp *prev = desp->prev;
    prev->next = desp->next;
    prev->next->prev = desp->prev;

    // prepend到lru的头部
    prependLru(desp);
}

// 全局变量
Buffer kBuffer;

} // namespace db
