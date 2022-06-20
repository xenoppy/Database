#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

#include "./block.h"
#include "table.h"
#include "./buffer.h"

namespace db {
class bplus_tree
{
  public:
    Table *table_;
    bplus_tree(bool force_empty = false)
        : table_(NULL)
    {}

    /*顶层操作*/
    std::pair<bool, unsigned int>
    search(void *key, size_t key_len, unsigned int *value);
    unsigned int insert(void *key, size_t key_len, unsigned int *value);
    int remove(void *key, size_t key_len);
    inline Table *get_table() { return table_; }
    inline void set_table(Table *table) { table_ = table; }
    void index_create(IndexBlock *preindex, IndexBlock &nextindex);
    void insert_to_index();
};
} // namespace db
