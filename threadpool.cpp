#include "threadpool.h"
#include <assert.h>

static void *worker(void *arg) {
    ThreadPool *pool = (ThreadPool *)arg;
    while (true) {
        pthread_mutex_lock(&pool->mu);
        // Wait for non empty work pool.
        while (pool->work.empty()) {
            // Signal received from producer and lock in same step.
            pthread_cond_wait(&pool->notEmpty, &pool->mu);
        }
        // Fetch and remove work, then unlock.
        Work work = pool->work.front();
        pool->work.pop_front();
        pthread_mutex_unlock(&pool->mu);
        // Do work.
        work.f(work.arg);
    }
    return NULL;
}

void threadPoolInit(ThreadPool *pool, size_t numThreads) {
    pool->threads.resize(numThreads);
    pthread_mutex_init(&pool->mu, NULL);
    pthread_cond_init(&pool->notEmpty, NULL);
    for (size_t i = 0; i < numThreads; i++) {
        int rv = pthread_create(&pool->threads[i], NULL, &worker, pool);
        assert(rv == 0);
    }
}

void threadPoolQueue(ThreadPool *pool, void (*f)(void *), void *arg) {
    // Acquire lock and push work on queue.
    pthread_mutex_lock(&pool->mu);
    pool->work.push_back(Work{f, arg});
    // Send signal to worker and release thread.
    pthread_cond_signal(&pool->notEmpty);
    pthread_mutex_unlock(&pool->mu);
}