#include <assert.h>
#include <string.h>
#include <stdlib.h>
// proj
#include "zset.hpp"
#include "avl.hpp"
#include "hashtable.hpp"
#include "common.hpp"

static ZNode *znodeNew(const char *name, size_t len, double score) {
    ZNode *node = (ZNode *)malloc(sizeof(ZNode) + len);
    assert(node);
    avlInit(&node->tree);
    node->hmap.next = NULL;
    node->hmap.hcode = strHash((uint8_t *)name, len);
    node->score = score;
    node->len = len;
    memcpy(&node->name[0], name, len);
    return node;
}

static size_t min(size_t lhs, size_t rhs) {
    return lhs < rhs ? lhs : rhs;
}

static void znodeDel(ZNode *node) {
    free(node);
}

struct HKey {
    HNode node;
    const char *name = NULL;
    size_t len = 0;
};

static bool hcmp(HNode *node, HNode *key) {
    ZNode *znode = container_of(node, ZNode, hmap);
    HKey *hkey = container_of(key, HKey, node);
    if (znode->len != hkey->len) {
        return false;
    }
    return memcmp(znode->name, hkey->name, znode->len) == 0;
}

ZNode *zsetLookup(ZSet *zset, const char *name, size_t len) {
    if (!zset->root) {
        return NULL;
    }
    HKey key;
    key.node.hcode = strHash((uint8_t *)name, len);
    key.name = name;
    key.len = len;
    HNode *found = hmLookup(&zset->hmap, &key.node, &hcmp);
    return found ? container_of(found, ZNode, hmap) : NULL;
}

void treeDispose(AVLNode *node) {
    if (!node) {
        return;
    }
    treeDispose(node->left);
    treeDispose(node->right);
    znodeDel(container_of(node, ZNode, tree));
}

void zsetClear(ZSet *zset) {
    treeDispose(zset->root);
    hmClear(&zset->hmap);
    zset->root = NULL;
}

// compare by the (score, name) tuple
static bool zless(AVLNode *lhs, double score, const char *name, size_t len)
{
    ZNode *zl = container_of(lhs, ZNode, tree);
    if (zl->score != score) {
        return zl->score < score;
    }
    int rv = memcmp(zl->name, name, min(zl->len, len));
    if (rv != 0) {
        return rv < 0;
    }
    return zl->len < len;
}

static bool zless(AVLNode *lhs, AVLNode *rhs) {
    ZNode *zr = container_of(rhs, ZNode, tree);
    return zless(lhs, zr->score, zr->name, zr->len);
}
    
static void treeInsert(ZSet *zset, ZNode *node) {
    AVLNode *parent = NULL;
    AVLNode **from = &zset->root;
    // tree search
    while (*from) {
        parent = *from;
        from = zless(&node->tree, parent) ? &parent->left : &parent->right;
    }
    // attach new node
    *from = &node->tree;
    node->tree.parent = parent;
    zset->root = avlFix(&node->tree);
}

static void zsetUpdate(ZSet *zset, ZNode *node, double score) {
    // detach the existing node
    zset->root = avlDel(&node->tree);
    avlInit(&node->tree);
    // update and reinsert node
    node->score = score;
    treeInsert(zset, node);
}

bool zsetInsert(ZSet *zset, const char *name, size_t len, double score) {
    ZNode *node = zsetLookup(zset, name, len);
    if (node) {
        zsetUpdate(zset, node, score);
        return false;
    } else {
        node = znodeNew(name, len, score);
        hmInsert(&zset->hmap, &node->hmap);
        treeInsert(zset, node);
        return true;
    }
}

void zsetDelete(ZSet *zset, ZNode *node) {
    // remove from the hashtable
    HKey key;
    key.node.hcode = node->hmap.hcode;
    key.name = node->name;
    key.len = node->len;
    HNode *found = hmDelete(&zset->hmap, &key.node, &hcmp);
    // remove from tree
    zset->root = avlDel(&node->tree);
    // deallocate node
    znodeDel(node);
}

// find first pair with (score, name) >= passed args
ZNode *zsetSeekge(ZSet *zset, double score, const char *name, size_t len) {
    AVLNode *found = NULL;
    for (AVLNode *node = zset->root; node;) {
        if (zless(node, score, name, len)) {
            node = node->right;
        } else {
            found = node;
            node = node->left;
        }
    }
    return found ? container_of(found, ZNode, tree) : NULL;
}

ZNode *znodeOffset(ZNode *node, int64_t offset) {
    AVLNode *tnode = node ? avlOffset(&node->tree, offset) : NULL;
    return tnode ? container_of(tnode, ZNode, tree) : NULL;
}