#include "heap.h"
#include <assert.h>

size_t heapParent(size_t pos) {
    assert(pos > 0);
    return (pos + 1) / 2 - 1;
}

size_t heapLeft(size_t pos) {
    return pos * 2 + 1;
}

size_t heapRight(size_t pos) {
    return pos * 2 + 2;
}

static void heapUp(HeapItem *a, size_t pos) {
    HeapItem t = a[pos];
    while (pos > 0 && a[heapParent(pos)].val >t.val) {
        a[pos] = a[heapParent(pos)];
        *a[pos].ref = pos;
        pos = heapParent(pos);
    }
    a[pos] = t;
    *a[pos].ref = pos;
}

static void heapDown(HeapItem *a, size_t pos, size_t n) {
    HeapItem t = a[pos];
    while (true) {
        size_t l = heapLeft(pos);
        size_t r = heapRight(pos);
        size_t minPos = pos;
        uint64_t minVal = t.val;
        if (l < n && a[l].val < minVal) {
            minVal = a[l].val;
            minPos = l;
        }
        if (r < n && a[r].val < minVal) {
            minPos = r;
        }
        if (minPos == pos) {
            break;
        }
        a[pos] = a[minPos];
        *a[pos].ref = pos;
        pos = minPos;
    }
    a[pos] = t;
    *a[pos].ref = pos;
}

void heapUpdate(HeapItem *a, size_t pos, size_t n) {
    if (pos > 0 && a[heapParent(pos)].val > a[pos].val) {
        heapUp(a, pos);
    } else {
        heapDown(a, pos, n);
    }
}