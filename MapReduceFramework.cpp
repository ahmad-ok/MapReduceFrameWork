#include "MapReduceFramework.h"
#include <iostream>
#include <pthread.h>
#include <atomic>

#define MUTEX_INIT_FAIL "system error: initializing mutex for thread failed\n"
#define THREAD_INIT_FAIL "system error: creating thread failed\n"

class JobContext
{

public:
    std::atomic<uint64_t> counter;
    pthread_mutex_t lock; // job lock
    OutputVec *outputVec;
    JobState state;
    pthread_t *threads;
    bool isWaiting;

    JobContext(int multiThreadLevel)
    {
        this->threads = new pthread_t[multiThreadLevel];
        this->state = {UNDEFINED_STAGE, 0};
        this->outputVec = new OutputVec;
        this->isWaiting = false;
    }

    ~JobContext()
    {
        pthread_mutex_destroy(&lock);
    }

    pthread_t* job_threads()
    {
        return threads;
    }
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

    pthread_mutex_t job_lock;
    auto *jobContext = new JobContext(multiThreadLevel);
    auto *contexts = new ThreadContext[multiThreadLevel];
    for (int i = 0; i < multiThreadLevel; ++i)
    {
        pthread_mutex_t lock; //todo : same lock for all?
        if (pthread_mutex_init(&lock, nullptr))
        {
            std::cerr << MUTEX_INIT_FAIL; //todo: exit then free
        }
        contexts[i] = {lock, inputVec[i], IntermediateVec(), OutputPair(), jobContext};;
    }

    pthread_t *threads = jobContext->job_threads();
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

void emit2(K2 *key, V2 *value, void *context)
{
    //todo : wrap in function
    auto* tc = static_cast<ThreadContext*>(context);
    pthread_mutex_lock(&tc->lock);
    IntermediatePair pair;
    pair.first = key;
    pair.second = value;
    tc->intermediateVec.push_back(pair);
    pthread_mutex_unlock(&tc->lock);
}

void emit3(K3 *key, V3 *value, void *context)
{
    //todo : wrap in function
    auto* jc = static_cast<JobContext*>(context);
    pthread_mutex_lock(&jc->lock);
    OutputPair pair;
    pair.first = key;
    pair.second = value;
    jc->outputVec->push_back(pair);
    pthread_mutex_unlock(&jc->lock);
}

void* mapThread(void* arg)
{
    auto* tc = static_cast<ThreadContext*>(arg);

}




