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
    std::pair<bool, unsigned int>
    insert(void *key, size_t key_len, unsigned int *value);
    int remove(void *key, size_t key_len);
    Table *get_table() { return table_; }
    void set_table(Table *table) { table_ = table; }
};
} // namespace db
