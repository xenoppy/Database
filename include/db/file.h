////
// @file file.h
// @brief
// 文件操作
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#ifndef __DB_FILE_H__
#define __DB_FILE_H__

#include "./config.h"
#include <map>

namespace db {

class File
{
  public:
    HANDLE handle_; // 文件描述符句柄

  public:
    File()
        : handle_(INVALID_HANDLE_VALUE)
    {}
    ~File() { close(); }

    // 打开文件
    int open(const char *path);
    // 关闭文件
    void close();
    // 读文件
    int read(unsigned long long offset, char *buffer, size_t length);
    // 写文件
    int write(unsigned long long offset, const char *buffer, size_t length);
    // 文件长度
    int length(unsigned long long &len);
    // 删除文件
    static int remove(const char *path);
};

// 文件池
class Schema;
class FilePool
{
  private:
    Schema *schema_;                   // 指向元数据
    std::map<const char *, File> map_; // 表名 --> 描述符

  public:
    FilePool()
        : schema_(NULL)
    {}

    // 初始化
    void init(Schema *schema);
    // 打开table
    File *open(const char *table);
};

// 全局文件池
extern FilePool kFiles;

} // namespace db

#endif // __DB_FILE_H__
