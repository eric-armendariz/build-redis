#pragma once

#include <stddef.h>
#include <stdint.h>

struct HNode {
    HNode *next = NULL;
    uint64_t hcode = 0;
};

struct HTab {
    HNode **tab = NULL;
    // Number of slots in hashtable
    size_t mask = 0;
    // Number of keys in hashtable
    size_t size = 0;
};

struct HMap {
    HTab newer;
    HTab older;
    size_t migratePos = 0;
};

HNode *hmLookup(HMap *hmap, HNode *key, bool(*eq)(HNode *, HNode *));
void hmInsert(HMap *hmap, HNode *node);
HNode *hmDelete(HMap *hmap, HNode *key, bool(*eq)(HNode *, HNode *));