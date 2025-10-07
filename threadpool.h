#pragma once

#include <deque>
#include <vector>
#include <pthread.h>
#include <stddef.h>

struct Work {
    void (*f)(void *) = NULL;
    void *arg = NULL;
};

struct ThreadPool {
    std::vector<pthread_t> threads;
    std::deque<Work> work;
    pthread_mutex_t mu;
    pthread_cond_t notEmpty;
};

void threadPoolInit(ThreadPool *pool, size_t numThreads);
void threadPoolQueue(ThreadPool *pool, void (*f)(void *), void *arg);