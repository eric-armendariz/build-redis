#pragma once

#include <stddef.h>
#include <stdint.h>

struct AVLNode {
    AVLNode *parent = NULL;
    AVLNode *left = NULL;
    AVLNode *right = NULL;
    uint32_t height = 0;
    uint32_t cnt = 0;
};

inline void avlInit(AVLNode *node) {
    node->left = node->right = node->parent = NULL;
    node->height = 1;
    node->cnt = 1;
}

// helpers
inline uint32_t avlHeight(AVLNode *node) { return node ? node->height : 0; }
inline uint32_t avlCnt(AVLNode *node) { return node ? node->cnt : 0; }
static void avlUpdate(AVLNode *node);

// API
AVLNode *avlFix(AVLNode *node);
AVLNode *avlDel(AVLNode *node);
AVLNode *avlOffset(AVLNode *node, int64_t offset);