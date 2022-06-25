#ifndef __DB_BPT_H__
#define __DB_BPT_H__
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <queue>
#include <stack>
#include "./block.h"
#include "./table.h"
#include "./buffer.h"
namespace db {
class bplus_tree
{
  public:
    Table *table_;
    std::stack<unsigned int> route;
    std::stack<unsigned short> routeslot;
    bplus_tree(bool force_empty = false)
        : table_(NULL)
    {}

    /*顶层操作*/
    unsigned int search(void *key, size_t key_len);
    unsigned int insert(void *key, size_t key_len, unsigned int value);
    int remove(void *key, size_t key_len);
    /*底层操作*/
    inline Table *get_table() { return table_; }//获取表
    inline void set_table(Table *table) { table_ = table; }//设置该树所属表
    unsigned int index_create(IndexBlock *preindex);//该函数用于叶子节点的分裂，分配新节点的同时维护链表
    void insert_to_index(void* key, size_t key_len, unsigned int newblockid);//叶子节点分裂后向上传递的函数
    /*删除时的叶子节点是否需要合并的操作*/
    //1.是否从右边节点借 2.需要借记录的叶子节点id 3.每个节点最少多少条记录。
    bool borrow_key(bool from_right, IndexBlock& borrower, unsigned short limit);
    void remove_from_index(std::pair<void*, size_t>);

    //清空数据结构中的栈
    std::pair<bool, unsigned int> index_search(void* key, size_t key_len);
    inline void reset_route()
    {
        while (!route.empty()) {
            route.pop();
        }
    }
    inline void reset_routeslot()
    {
        while (!routeslot.empty()) {
            routeslot.pop();
        }
    }
    //清空树，用于测试
    void clear_tree(unsigned int root);
};
} // namespace db
#endif // __DB_BPT_H__