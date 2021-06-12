#include <algorithm>
#include "MapReduceFramework.h"
#include "JobContext.h"



#define THREAD_INIT_FAIL "system error: creating thread failed\n"
#define THREAD_JOIN_FAIL "system error: joining thread failed\n"
#define MUTEX_INIT_FAIL "system error: initializing mutex for thread failed\n"
#define MUTEX_LOCK_FAIL "system error: locking mutex failed\n"
#define MUTEX_UNLOCK_FAIL "system error: unlocking mutex failed\n"
#define ERROR_WRAPPER(condition, msg) if(condition){print_sys_error(msg);}


void *MapReducePhase(void *arg);

void shufflePhase(void *arg);

void reducePhase(const ThreadContext *tc);

void print_sys_error(const std::string &error_msg)
{
    std::cerr << error_msg;
    exit(1);
}

JobHandle startMapReduceJob(const MapReduceClient &client,
                            const InputVec &inputVec, OutputVec &outputVec,
                            int multiThreadLevel)
{
    auto *threads = new pthread_t[multiThreadLevel];
    auto *contexts = new ThreadContext[multiThreadLevel];


    auto *jobContext = new JobContext(threads, contexts, outputVec, inputVec, client, multiThreadLevel);
    ERROR_WRAPPER((pthread_mutex_init(&jobContext->lock, nullptr) ||
                   pthread_mutex_init(&jobContext->reduce_lock, nullptr) ||
                   pthread_mutex_init(&jobContext->stateChange_lock, nullptr)||
                   pthread_mutex_init(&jobContext->wait_lock, nullptr)), MUTEX_INIT_FAIL)
    jobContext->setTotalKeys(inputVec.size());
    for (int i = 0; i < multiThreadLevel; ++i)
    {
        jobContext->contexts[i].id = i;
        jobContext->contexts[i].jobContext = jobContext;
        ERROR_WRAPPER(pthread_create(&jobContext->threads[i], nullptr, MapReducePhase,
                                     &jobContext->contexts[i]), THREAD_INIT_FAIL)
    }
    return jobContext;
}


void getJobState(JobHandle job, JobState *state)
{
    auto *jc = static_cast<JobContext *>(job);

    ERROR_WRAPPER(pthread_mutex_lock(&jc->stateChange_lock), MUTEX_LOCK_FAIL)
    state->percentage = (jc->getProcessedKeys() / (float) jc->getTotalKeys()) * 100;
    state->stage = (stage_t)((uint64_t)((jc->counter.load()&(uint64_t)
            ((uint64_t)3 << (uint64_t)62)) >> (uint64_t)62));
    ERROR_WRAPPER(pthread_mutex_unlock(&jc->stateChange_lock), MUTEX_UNLOCK_FAIL)
}

void emit2(K2 *key, V2 *value, void *context)
{
    auto *tc = static_cast<ThreadContext *>(context);
    tc->intermediateVec.push_back(IntermediatePair(key,value));
    tc->jobContext->numOfIntermediatePairs++;
}

void emit3(K3 *key, V3 *value, void *context)
{
    auto *jc = static_cast<JobContext *>(context);
    ERROR_WRAPPER(pthread_mutex_lock(&jc->lock), MUTEX_LOCK_FAIL)
    jc->outputVec.push_back(OutputPair(key, value));
    jc->counter ++;
    ERROR_WRAPPER(pthread_mutex_unlock(&jc->lock), MUTEX_UNLOCK_FAIL)
}

void *MapReducePhase(void *arg)
{
    auto *tc = static_cast<ThreadContext *>(arg);
    tc->jobContext->counter = (~( (uint64_t)3 << (uint64_t)62) & tc->jobContext->counter.load()) +
                                ((uint64_t)1 << (uint64_t)62);
    uint64_t i;

    while ((i = tc->jobContext->nextInputIdx ++)< tc->jobContext->getTotalKeys())
    {
        InputPair currPair = tc->jobContext->inputVec[i];
        tc->jobContext->client.map(currPair.first, currPair.second, tc);
        tc->jobContext->counter++;
    }


    //Sort the intermediate Vector
    //Note: no thread gets to this Stage if still it can process any pair of (k1,v1)
    std::sort(tc->intermediateVec.begin(), tc->intermediateVec.end(),
              [](const IntermediatePair &lhs, const IntermediatePair &rhs)
              {
                  return *lhs.first < *rhs.first;
              });


    // barrier before the Shuffle Phase
    tc->jobContext->barrier.barrier();


    //Shuffle Stage only thread 0 call it
    if (tc->id == 0)
    {
        shufflePhase(tc->jobContext);
        ERROR_WRAPPER(pthread_mutex_lock(&tc->jobContext->stateChange_lock), MUTEX_LOCK_FAIL)
        tc->jobContext->counter = (((uint64_t) tc->jobContext->numOfVecsToReduce << (uint64_t)31) +
                                   ((uint64_t)3 << (uint64_t)62));
        ERROR_WRAPPER(pthread_mutex_unlock(&tc->jobContext->stateChange_lock), MUTEX_UNLOCK_FAIL)

    }

    //reduce phase
    tc->jobContext->barrier.barrier();
    reducePhase(tc);
    return nullptr;
}

void reducePhase(const ThreadContext *tc)
{
    while (!tc->jobContext->shuffledVec.empty())
    {
        ERROR_WRAPPER(pthread_mutex_lock(&tc->jobContext->reduce_lock), MUTEX_LOCK_FAIL)
        if (tc->jobContext->shuffledVec.empty())
        {
            ERROR_WRAPPER(pthread_mutex_unlock(&tc->jobContext->reduce_lock), MUTEX_UNLOCK_FAIL)
            return;
        }
        IntermediateVec currVec = tc->jobContext->shuffledVec.back();
        tc->jobContext->shuffledVec.pop_back();
        ERROR_WRAPPER(pthread_mutex_unlock(&tc->jobContext->reduce_lock), MUTEX_UNLOCK_FAIL)

        tc->jobContext->client.reduce(&currVec, tc->jobContext);
    }
}

void shufflePhase(void *arg)
{
    auto *jc = static_cast<JobContext *>(arg);

    ERROR_WRAPPER(pthread_mutex_lock(&jc->stateChange_lock), MUTEX_LOCK_FAIL)
    jc->counter = (((uint64_t) jc->numOfIntermediatePairs << (uint64_t)31) + ((uint64_t)2 << (uint64_t)62));
    ERROR_WRAPPER(pthread_mutex_unlock(&jc->stateChange_lock), MUTEX_UNLOCK_FAIL)

    while (jc->getTotalKeys() != jc->getProcessedKeys())
    {
        IntermediatePair *maxPair = nullptr;
        for (int i = 0; i < jc->numOfThreads; ++i)
        {
            IntermediateVec &currVec = jc->contexts[i].intermediateVec;

            if (!currVec.empty())
            {
                if (maxPair == nullptr || *maxPair->first < *currVec.back().first)
                {
                    maxPair = &currVec.back();
                }
            }
        }

        std::vector<IntermediatePair> currKeyVector;
        for (int i = 0; i < jc->numOfThreads; i++)
        {
            while (!(jc->contexts[i].intermediateVec.empty()) &&
                   !(*jc->contexts[i].intermediateVec.back().first < *maxPair->first) &&
                   !(*maxPair->first < *jc->contexts[i].intermediateVec.back().first))
            {
                currKeyVector.push_back(jc->contexts[i].intermediateVec.back());
                jc->contexts[i].intermediateVec.pop_back();
                jc->counter++;
            }
        }
        jc->shuffledVec.push_back(currKeyVector);
        jc->numOfVecsToReduce++;
    }
}


void waitForJob(JobHandle job)
{
    auto *jc = static_cast<JobContext *>(job);

    ERROR_WRAPPER(pthread_mutex_lock(&jc->wait_lock), MUTEX_LOCK_FAIL);
    if(!jc->threadsJoined)
    {
        jc->threadsJoined = true;
        for (int i = 0; i < jc->numOfThreads; ++i)
        {
            ERROR_WRAPPER(pthread_join(jc->threads[i], nullptr), THREAD_JOIN_FAIL)
        }
    }
    ERROR_WRAPPER(pthread_mutex_unlock(&jc->wait_lock), MUTEX_LOCK_FAIL);
}


void closeJobHandle(JobHandle job)
{
    auto *jc = static_cast<JobContext *>(job);
    if(!jc->threadsJoined)
    {
        waitForJob(job);
    }
    delete jc;
}



