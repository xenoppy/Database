#ifndef __DB_BPT_H__
#define __DB_BPT_H__
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
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
    bplus_tree(bool force_empty = false)
        : table_(NULL)
    {}

    /*顶层操作*/
    unsigned int search(void *key, size_t key_len);
    unsigned int insert(void *key, size_t key_len, unsigned int value);
    int remove(void *key, size_t key_len);
    /*底层操作*/
    inline Table *get_table() { return table_; }
    inline void set_table(Table *table) { table_ = table; }
    unsigned int index_create(IndexBlock *preindex);
    void insert_to_index(void *key, size_t key_len, unsigned int newblockid);

    std::pair<bool, unsigned int> index_search(void *key, size_t key_len);
    inline void reset_route()
    {
        while (!route.empty()) {
            route.pop();
        }
    }
};
} // namespace db
#endif // __DB_BPT_H__