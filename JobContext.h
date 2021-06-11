#ifndef OS_EX3_JOBCONTEXT_H
#define OS_EX3_JOBCONTEXT_H

#include <atomic>
#include "MapReduceFramework.h"
#include <pthread.h>
#include <iostream>
#include "Barrier.h"

#define MUTEX_INIT_FAIL "system error: initializing mutex for thread failed\n"
#define THREAD_INIT_FAIL "system error: creating thread failed\n"

class JobContext;

typedef std::vector<IntermediateVec> shuffleIntermediateVec;

struct ThreadContext //todo : warning ?
{
    IntermediateVec intermediateVec;
    JobContext *jobContext;
    int id;
};

class JobContext{

public:
    std::atomic<uint64_t> counter;
    std::atomic<int> nextInputIdx;
    std::atomic<int> numOfIntermediatePairs;
    std::atomic<int> numOfVecsToReduce;

    int numOfThreads;
    Barrier barrier;

    pthread_mutex_t lock{};
    pthread_mutex_t reduce_lock{};
    pthread_mutex_t stateChange_lock{};

    const InputVec& inputVec;
    OutputVec& outputVec;
    const MapReduceClient& client;
    pthread_t* threads;
    ThreadContext* contexts;
    bool threadsJoined;
    shuffleIntermediateVec shuffledVec;

    JobContext(pthread_t* threads, ThreadContext* contexts, OutputVec& outputVec, const InputVec& inputVec, const MapReduceClient& client, int numOfThreads);

    void setTotalKeys(uint64_t totalKeys);
    uint64_t getTotalKeys();
    uint64_t getProcessedKeys();
    stage_t getStage();
    ~JobContext();

};

#endif //OS_EX3_JOBCONTEXT_H
