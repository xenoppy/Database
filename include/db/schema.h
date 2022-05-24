////
// @file schema.h
// @brief
// 定义schema
// meta.db存放所有表的元信息，每张表用meta.db的一行来描述。一行是变长的，它包含：
// 1. 以表名做键；
// 2. 文件路径；
// 3. 域的个数；
// 4. 各域的描述；（变长）
// 5. 各种统计信息，表的大小，行数等；
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#ifndef __DB_SCHEMA_H__
#define __DB_SCHEMA_H__

#include <string>
#include <map>
#include <vector>
#include "./datatype.h"
#include "./record.h"

namespace db {

// 描述关系的域
// 持久化的信息包括：name、index、length、type->name
// name是字段名，index是字段的下标，length表示字段的长度，type->name是字段的类型名
struct FieldInfo
{
    std::string name;         // 域名
    unsigned long long index; // 位置
    long long length;         // 长度，高位表示是否固定大小
    DataType *type;           // 指向数据类型

    FieldInfo()
        : index(0)
        , length(0)
        , type(NULL)
    {}
    FieldInfo(const FieldInfo &o) = default;
};
// 内存中描述关系
struct RelationInfo
{
    std::string path;              // 文件路径
    unsigned short count;          // 域的个数
    unsigned short type;           // 类型
    unsigned int key;              // 键的域
    unsigned long long size;       // 大小
    unsigned long long rows;       // 行数
    std::vector<FieldInfo> fields; // 各域的描述

    RelationInfo()
        : count(0)
        , type(0)
        , key(0)
        , size(0)
        , rows(0)
    {}
    RelationInfo(const char *p)
        : path(p)
        , count(0)
        , type(0)
        , key(0)
        , size(0)
        , rows(0)
    {}
    // 根据关系属性得到iov的维度
    int iovSize() { return 7 + count * 4; }
};

////
// @brief
// Schema描述表空间
//
class Buffer;
class Schema
{
  public:
    using TableSpace = std::map<std::string, RelationInfo>;

  public:
    static const char *META_FILE; // "_meta.db"

  private:
    Buffer *buffer_;        // 缓冲层
    TableSpace tablespace_; // 表空间
    unsigned int maxid_;    // 最大的blockid
    unsigned int idle_;     // 空闲链
    unsigned int first_;    // meta链

  public:
    Schema()
        : buffer_(NULL)
        , maxid_(0)
        , idle_(0)
        , first_(0)
    {}

    // 初始化全局schema
    void init(Buffer *buffer);

    // 打开并加载元数据
    void open();
    // 创建表
    int create(const char *table, RelationInfo &rel);
    // 搜索表
    std::pair<TableSpace::iterator, bool> lookup(const char *table);

  public:
    // 将table的关系的相关属性，塞到iov里
    void initIov(
        const char *table,
        RelationInfo &rel,
        std::vector<struct iovec> &iov);
    void retrieveInfo(
        std::string &table,
        RelationInfo &rel,
        std::vector<struct iovec> &iov);
    void betoh(std::vector<struct iovec> &iov);
    void htobe(std::vector<struct iovec> &iov);
};

// 初始化数据库全局变量，缺省buffer大小为256MB
void dbInit(size_t bufsize = 256);

// 全局schema
extern Schema kSchema;
} // namespace db

#endif // __DB_SCHEMA_H__
