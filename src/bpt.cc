#include "db/bpt.h"
#include "db/block.h"
#include "db/table.h"
#include "db/buffer.h"
#include <stdlib.h>

#include <list>
#include <algorithm>
#pragma warning(disable : 4996)
using std::swap;
using std::binary_search;
using std::lower_bound;
using std::upper_bound;

namespace db {
std::pair<bool,unsigned int> bplus_tree::index_search(void *key, size_t key_len)
    {
        if(!table_->indexCount())
            return {false,1};
        SuperBlock super;
        BufDesp*desp=kBuffer.borrow(table_->name_.c_str(),0);
        super.attach(desp->buffer);
        unsigned int root=super.getIndexroot();
        desp->relref();
        IndexBlock index;
        index.setTable(table_);
        BufDesp*desp2=kBuffer.borrow(table_->name_.c_str(),root);
        index.attach(desp2->buffer);
        
        unsigned int target=root;
        while(index.getMark()!=1)
        {
            std::pair<bool,unsigned short>ret=index.searchRecord(key,key_len);
            if(ret.second>=index.getSlots())//跳转到右指针
            {
                target=index.getNext();
                index.detach();
                desp2=kBuffer.borrow(table_->name_.c_str(),target);
                index.attach(desp2->buffer);
            }
            else//找到对应位置后，跳转到左指针
            {
                Record record;
                index.refslots(ret.second,record);
                unsigned int length=4;
                record.getByIndex((char*)&target,&length,1);
                target=be32toh(target);
                index.detach();
                desp2=kBuffer.borrow(table_->name_.c_str(),target);
                index.attach(desp2->buffer);
            }
        }
        return {true,target};
    }


}