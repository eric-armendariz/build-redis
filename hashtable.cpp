#include <assert.h>
#include <stdlib.h>
#include "hashtable.hpp"

const size_t kMaxLoadFactor = 8;
const size_t kRehashingWork = 128;

static void hashInit(HTab *ht, size_t n) {
    assert(n > 0 && ((n - 1) & n) == 0);
    ht->mask = n - 1;
    ht->tab = (HNode **)calloc(n, sizeof(HNode *));
    ht->size = 0;
}

static void hashInsert(HTab *ht, HNode *node) {
    size_t pos = node->hcode & ht->mask;
    HNode *next = ht->tab[pos];
    node->next = next;
    ht->tab[pos] = node;
    ht->size++;
}

static HNode **hashLookup(HTab *ht, HNode *key, bool(*eq)(HNode *, HNode *)) {
    if (!ht->tab) {
        return NULL;
    }
    size_t pos = key->hcode & ht->mask;
    HNode **from = &ht->tab[pos];
    for (HNode *curr; (curr = *from) != NULL; from = &curr->next) {
        if (curr->hcode == key->hcode && eq(curr, key)) {
            return from;
        }
    }
    return NULL;
}

static HNode *hashDetatch(HTab *ht, HNode **from) {
    HNode *node = *from;
    *from = node->next;
    ht->size--;
    return node;
}

static void hmTriggerRehashing(HMap *hmap) {
    hmap->older = hmap->newer;
    hashInit(&hmap->newer, (hmap->newer.mask + 1) * 2);
    hmap->migratePos = 0;
}

static void hmHelpRehashing(HMap *hmap) {
    size_t nwork = 0;
    while (nwork < kRehashingWork && hmap->older.size > 0) {
        // find non empty slot
        HNode **from = &hmap->older.tab[hmap->migratePos];
        if (!*from) {
            hmap->migratePos++;
            continue;
        }
        // move entry to new table
        hashInsert(&hmap->newer, hashDetatch(&hmap->older, from));
        nwork++;
    }
    // free old table if done
    if (hmap->older.size == 0) {
        free(hmap->older.tab);
        hmap->older.tab = NULL;
    }
}

HNode *hmLookup(HMap *hmap, HNode *key, bool(*eq)(HNode *, HNode *)) {
    HNode **from = hashLookup(&hmap->newer, key, eq);
    if (!from) {
        from = hashLookup(&hmap->older, key, eq);
    }
    return from ? *from : NULL;
}

HNode *hmDelete(HMap *hmap, HNode *key, bool(*eq)(HNode *, HNode *)) {
    if (HNode **from = hashLookup(&hmap->newer, key, eq)) {
        return hashDetatch(&hmap->newer, from);
    }
    if (HNode **from = hashLookup(&hmap->older, key, eq)) {
        return hashDetatch(&hmap->older, from);
    }
    return NULL;
}

void hmInsert(HMap *hmap, HNode *node) {
    if (!hmap->newer.tab) {
        hashInit(&hmap->newer, 4);
    }
    hashInsert(&hmap->newer, node);
    if (!hmap->older.tab) {
        size_t shreshold = (hmap->newer.mask + 1) * kMaxLoadFactor;
        if (hmap->newer.size >= shreshold) {
            hmTriggerRehashing(hmap);
        }
    }
    hmHelpRehashing(hmap);
}

size_t hmSize(HMap *hmap) {
    return hmap->newer.size + hmap->older.size;
}

void hmForEach(HMap *hmap, bool(*cb)(HNode *, void *), void *arg) {
    for (size_t i = 0; i < hmap->newer.size; i++) {
        HNode *node = hmap->newer.tab[i];
        while (node) {
            if (!cb(node, arg)) {
                return;
            }
            node = node->next;
        }
    }
    for (size_t i = 0; i < hmap->older.size; i++) {
        HNode *node = hmap->older.tab[i];
        while (node) {
            if (!cb(node, arg)) {
                return;
            }
            node = node->next;
        }
    }
}

void hmClear(HMap *hmap) {
    free(hmap->newer.tab);
    free(hmap->older.tab);
    *hmap = HMap{};
}