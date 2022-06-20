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

namespace bpt {}
