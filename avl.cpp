#include <assert.h>
#include "avl.hpp"


static uint32_t max(uint32_t lhs, uint32_t rhs) {
    return lhs < rhs ? rhs : lhs;
}

static void avlUpdate(AVLNode *node) {
    node->height = 1 + max(avlHeight(node->left), avlHeight(node->right));
    node->cnt = 1 + avlCnt(node->left) + avlCnt(node->right);
};

static AVLNode *rotLeft(AVLNode *node) {
    AVLNode *parent = node->parent;
    AVLNode *newNode = node->right;
    AVLNode *inner = newNode->left;
    // move lesser vals of new root node to left subtree
    node->right = inner;
    if (inner) {
        inner->parent = node;
    }
    // shift parent pointers
    newNode->parent = parent;
    newNode->left = node;
    node->parent = newNode;
    // update height data for nodes with subtree changes
    avlUpdate(node);
    avlUpdate(newNode);
    return newNode;
}

static AVLNode *rotRight(AVLNode *node) {
    AVLNode *newNode = node->left;
    AVLNode *parent = node->parent;
    AVLNode *inner = newNode->right;
    // move greater vals of new root to right subtree
    node->left = inner;
    if (inner) {
        inner->parent = node;
    }
    // shift parent pointers
    newNode->parent = parent;
    newNode->right = node;
    node->parent = newNode;
    // update height
    avlUpdate(node);
    avlUpdate(newNode);
    return newNode;
}

// left subtree is taller by 2
static AVLNode *avlFixLeft(AVLNode *node) {
    if (avlHeight(node->left->left) < avlHeight(node->left->right)) {
        node->left = rotLeft(node->left);
    }
    return rotRight(node);
}

// right subtree is taller by 2
static AVLNode *avlFixRight(AVLNode *node) {
    if (avlHeight(node->right->right) < avlHeight(node->right->left)) {
        node->right = rotRight(node->right);
    }
    return rotLeft(node);
}


// called on updated node:
// propagate height data
// fix imbalances
// return new root node
AVLNode *avlFix(AVLNode *node) {
    while (true) {
        // to store the fixed subtree
        AVLNode **from = &node;
        AVLNode *parent = node->parent;
        if (parent) {
            // attach fixed subtree to parent
            from = parent->left == node ? &parent->left : &parent->right;
        }
        // auxiliary data
        avlUpdate(node);
        // fix height differences of 2
        uint32_t lHeight = avlHeight(node->left);
        uint32_t rHeight = avlHeight(node->right);
        if (lHeight == rHeight + 2) {
            *from = avlFixLeft(node);
        } else if (lHeight + 2 == rHeight) {
            *from = avlFixRight(node);
        }
        // root node, stop
        if (!parent) {
            return *from;
        }
        node = parent;
    }
}

// detatch a node where 1 of its children is empty
static AVLNode *avlDelEasy(AVLNode *node) {
    assert(!node->left || !node->right);
    AVLNode *child = node->left ? node->left : node->right;
    AVLNode *parent = node->parent;
    // update child's parent pointer
    if (child) {
        child->parent = node->parent;
    }
    // return node if new head
    if (!parent) {
        return child;
    }
    // change parent pointer to point to new node
    AVLNode **from = parent->left == node ? &parent->left : &parent->right;
    *from = child;
    // rebalance updated tree
    return avlFix(parent);
}

// detach a node and return new root of tree
AVLNode *avlDel(AVLNode *node) {
    // easy case with 0-1 empty child
    if (!node->left || !node->right) {
        return avlDelEasy(node);
    }
    // find successor
    AVLNode *succ = node->right;
    while (succ->left) {
        succ = succ->left;
    }
    // detatch successor
    AVLNode *root = avlDelEasy(succ);
    // swap with successor
    *succ = *node;
    succ->parent = node->parent;
    if (succ->left) {
        succ->left->parent = succ;
    }
    if (succ->right) {
        succ->right->parent = succ;
    }
    AVLNode **from = &root;
    AVLNode *parent = node->parent;
    if (parent) {
        from = parent->left == node ? &parent->left : &parent->right;
    }
    *from = succ;
    return root;
}