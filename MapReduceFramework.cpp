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
                   pthread_mutex_init(&jobContext->reduce_lock, nullptr)), MUTEX_INIT_FAIL)
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
    auto *context = static_cast<JobContext *>(job);
    context->state.percentage = (context->getProcessedKeys() / (float) context->getTotalKeys()) * 100;
    state->percentage = context->state.percentage;
    state->stage = context->state.stage;
}

void emit2(K2 *key, V2 *value, void *context)
{
    auto *tc = static_cast<ThreadContext *>(context);
    IntermediatePair pair;
    pair.first = key;
    pair.second = value;
    tc->intermediateVec.push_back(pair);
    tc->jobContext->numOfIntermediatePairs++;
}

void emit3(K3 *key, V3 *value, void *context)
{
    auto *jc = static_cast<JobContext *>(context);
    ERROR_WRAPPER(pthread_mutex_lock(&jc->lock), MUTEX_LOCK_FAIL)
    jc->outputVec.push_back(OutputPair(key, value));
    jc->counter += 1;
    ERROR_WRAPPER(pthread_mutex_unlock(&jc->lock), MUTEX_UNLOCK_FAIL)
}

void *MapReducePhase(void *arg)
{
    auto *tc = static_cast<ThreadContext *>(arg);
    tc->jobContext->state.stage = MAP_STAGE;
    uint64_t i;
    while ((unsigned int)tc->jobContext->nextInputIdx < tc->jobContext->getTotalKeys())
    {
        tc->jobContext->nextInputIdx++;
        i = tc->jobContext->nextInputIdx.load() - 1;
        InputPair currPair = tc->jobContext->inputVec[i];
        tc->jobContext->client.map(currPair.first, currPair.second, tc);
    }

    //Sort the intermediate Vector
    //Note: no thread gets to this Stage if still it can process any pair of (k1,v1)
    std::sort(tc->intermediateVec.begin(), tc->intermediateVec.end(),
              [](const IntermediatePair &lhs, const IntermediatePair &rhs)
              {
                  return *lhs.first < *rhs.first;
              });
    tc->jobContext->counter += 1;

    // barrier before the Shuffle Phase
    tc->jobContext->barrier.barrier();


    //Shuffle Stage only thread 0 call it
    if (tc->id == 0)
    {
        shufflePhase(tc->jobContext);
        tc->jobContext->state.stage = REDUCE_STAGE;
        tc->jobContext->setTotalKeys(tc->jobContext->numOfVecsToReduce);
        tc->jobContext->counter -= tc->jobContext->getProcessedKeys();

    }

    //reduce phase
    tc->jobContext->barrier.barrier();
    reducePhase(tc);
    return nullptr;
}

void reducePhase(const ThreadContext *tc)
{
    while (true)
    {
        ERROR_WRAPPER(pthread_mutex_lock(&tc->jobContext->reduce_lock), MUTEX_LOCK_FAIL)
        if (tc->jobContext->shuffledVec.empty())
        {
            ERROR_WRAPPER(pthread_mutex_unlock(&tc->jobContext->reduce_lock), MUTEX_UNLOCK_FAIL)
            break;
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
    jc->state.stage = SHUFFLE_STAGE;
    jc->counter -= jc->getProcessedKeys(); //zero the processed keys in shuffle Stage
    jc->setTotalKeys(jc->numOfIntermediatePairs);
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
                jc->counter += 1;
            }
        }
        jc->shuffledVec.push_back(currKeyVector);
        jc->numOfVecsToReduce++;
    }
}


void waitForJob(JobHandle job)
{
    auto *jc = static_cast<JobContext *>(job);
    for (int i = 0; i < jc->numOfThreads; ++i)
    {
        ERROR_WRAPPER(pthread_join(jc->threads[i], nullptr), THREAD_JOIN_FAIL)
    }
    jc->threadsJoined = true;
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



