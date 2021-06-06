#include "MapReduceFramework.h"
#include <iostream>
#include <pthread.h>
#include <atomic>

#define MUTEX_INIT_FAIL "system error: initializing mutex for thread failed\n"
#define THREAD_INIT_FAIL "system error: creating thread failed\n"
 struct JobContext
{
    OutputVec &outputVec;
    std::atomic<uint64_t> *counter;
    JobState state;
    pthread_t *threads;
    bool isWaiting;
};

struct ThreadContext //todo : warning ?
{
    pthread_mutex_t lock;
    InputPair inputPair;
    IntermediateVec intermediateVec;
    OutputPair outputPair;
    JobContext *jobContext;
};

void *thread_job(void *context)
{
    //todo : implement shit
    return nullptr;
}

JobHandle startMapReduceJob(const MapReduceClient &client,
                            const InputVec &inputVec, OutputVec &outputVec,
                            int multiThreadLevel)
{
    auto* counter = new std::atomic<uint64_t>(0);
    auto *threads = new pthread_t[multiThreadLevel];
    auto *jobContext = new JobContext{outputVec, counter, {UNDEFINED_STAGE, 0}, threads, false};
    auto *contexts = new ThreadContext[multiThreadLevel];
    for (int i = 0; i < multiThreadLevel; ++i)
    {
        pthread_mutex_t lock;
        if (pthread_mutex_init(&lock, nullptr))
        {
            std::cerr << MUTEX_INIT_FAIL; //todo: exit then free
        }
        contexts[i] = {lock, inputVec[i], IntermediateVec(), OutputPair(), jobContext};;
    }

    for (int i = 0; i < multiThreadLevel; ++i)
    {
        if (pthread_create(threads + i, nullptr, thread_job, contexts + i))
        {
            std::cerr << THREAD_INIT_FAIL; //todo : exit then free
        }
    }
    return jobContext;

}


void getJobState(JobHandle job, JobState* state)
{
    auto* context = static_cast<JobContext*>(job);
    state->percentage = context->state.percentage;
    state->stage = context->state.stage;
}




