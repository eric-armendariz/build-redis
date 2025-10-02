#include <set>
#include <string>
#include <unordered_map>
#include "avl.hpp"
#include "hashtable.hpp"

struct ZSet {
    AVLNode *root = NULL;
    HMap hmap;
};

struct ZNode {
    // structure nodes
    AVLNode tree;
    HNode   hmap;
    // data
    double  score = 0;
    size_t  len = 0;
    char    name[0];  
};

bool   zsetInsert(ZSet *zset, const char *name, size_t len, double score);
ZNode  *zsetLookup(ZSet *zset, const char *name, size_t len);
void   zsetDelete(ZSet *zset, ZNode *node);
ZNode  *zsetSeekge(ZSet *zset, double score, const char *name, size_t len);
ZNode  *znodeOffset(ZNode *znode, int64_t offset);
void zsetClear(ZSet *zset);