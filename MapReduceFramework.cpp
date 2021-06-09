#include <algorithm>
#include "MapReduceFramework.h"
#include "JobContext.h"
#include "unistd.h"


#define MUTEX_INIT_FAIL "system error: initializing mutex for thread failed\n"
#define THREAD_INIT_FAIL "system error: creating thread failed\n"


void *MapReducePhase(void *arg);

void shufflePhase(void *arg);

JobHandle startMapReduceJob(const MapReduceClient & client,
                            const InputVec & inputVec, OutputVec & outputVec,
                            int multiThreadLevel)
{

    auto *threads = new pthread_t[multiThreadLevel];
    auto *contexts = new ThreadContext[multiThreadLevel];
    auto *jobContext = new JobContext(threads, contexts, outputVec, inputVec, client, multiThreadLevel);
    jobContext->setTotalKeys(inputVec.size());
    for (int i = 0; i < multiThreadLevel; ++i)
    {
        jobContext->contexts[i].id = i;
        jobContext->contexts[i].jobContext = jobContext;
        if (pthread_create(&jobContext->threads[i], nullptr, MapReducePhase, &jobContext->contexts[i]))
        {
            std::cerr << THREAD_INIT_FAIL;
            //todo: free and exit
        }

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
    //todo : wrap in function
    auto *tc = static_cast<ThreadContext *>(context);
    IntermediatePair pair;
    pair.first = key;
    pair.second = value;
    tc->intermediateVec.push_back(pair);
    tc->jobContext->numOfIntermediatePairs++;
}

void emit3(K3 *key, V3 *value, void *context)
{
    //todo : wrap in function
    auto *jc = static_cast<JobContext *>(context);
    pthread_mutex_lock(&jc->lock);
    jc->outputVec.push_back(OutputPair(key, value));
    // todo : add to correct place plz
    jc->counter += 1;
    pthread_mutex_unlock(&jc->lock);
}

void *MapReducePhase(void *arg)
{
    auto *tc = static_cast<ThreadContext *>(arg);
    tc->jobContext->state.stage = MAP_STAGE;
    uint64_t i = 0;
    while (tc->jobContext->nextInputIdx < tc->jobContext->getTotalKeys())
    {
        tc->jobContext->nextInputIdx++;
        i = tc->jobContext->nextInputIdx.load() - 1;
        InputPair currPair = tc->jobContext->inputVec[i];
        tc->jobContext->client.map(currPair.first, currPair.second, tc);
        tc->jobContext->counter += 1;
    }

    //Sort the intermediate Vector
    //Note: no thread gets to this Stage if still it can process any pair of (k1,v1)
    std::sort(tc->intermediateVec.begin(), tc->intermediateVec.end(), [](const IntermediatePair & lhs, const IntermediatePair & rhs)
    {
        return *lhs.first < *rhs.first;
    });

    // barrier before the Shuffle Phase
    tc->jobContext->barrier.barrier();


    //Shuffle Stage only thread 0 call it
    if (tc->id == 0)
    {
        shufflePhase(tc->jobContext);
        tc->jobContext->state.stage = REDUCE_STAGE;
        tc->jobContext->setTotalKeys(tc->jobContext->numOfVecsToReduce);
        tc->jobContext->counter -= tc->jobContext->getProcessedKeys();
        sem_post(&tc->jobContext->semaphore);
    }

    //reduce phase
    //Note: the Semaphore is 0 it gets to 1 only when shuffling is Done

    while(true)
    {
        sem_wait(&(tc->jobContext->semaphore));
        pthread_mutex_lock(&tc->jobContext->reduce_lock);
        if(tc->jobContext->shuffledVec.empty())
        {
            pthread_mutex_unlock(&tc->jobContext->reduce_lock);
            break;
        }
        std::cout << "thread num" << tc->id << " is reducing" << std::endl;
        IntermediateVec currVec = tc->jobContext->shuffledVec.back();
        tc->jobContext->shuffledVec.pop_back();
        pthread_mutex_unlock(&tc->jobContext->reduce_lock);

        //todo Delete This Print
        tc->jobContext->client.reduce(&currVec,tc->jobContext);

    }

    return nullptr;
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
            IntermediateVec& currVec = jc->contexts[i].intermediateVec;

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
        pthread_join(jc->threads[i], nullptr);
        //todo handle the error
    }
}

void closeJobHandle(JobHandle job)
{
    auto *jc = static_cast<JobContext *>(job);
    waitForJob(job);
    delete jc;
}



