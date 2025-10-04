#pragma once

#include <stddef.h>

struct DList {
    DList *prev = NULL;
    DList *next = NULL;
};

inline void dlistDetach(DList *node) {
    DList *prev = node->prev;
    DList *next = node->next;
    prev->next = next;
    next->prev = prev;
}

inline void dlistInit(DList *node) {
    node->prev = node->next = node;
}

inline bool dlistEmpty(DList *node) {
    return node->next == node;
}

inline void dlistInsertBefore(DList *target, DList *rookie) {
    DList *prev = target->prev;
    prev->next = rookie;
    rookie->next = target;
    rookie->prev = prev;
    target->prev = rookie;
}