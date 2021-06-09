#include <algorithm>
#include "MapReduceFramework.h"
#include "JobContext.h"


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

    for (int i = 0; i < multiThreadLevel; ++i)
    {
        if (pthread_create(&jobContext->threads[i], nullptr, MapReducePhase, &jobContext->contexts[i]))
        {
            std::cerr << THREAD_INIT_FAIL;
            //todo: free and exit
        }
        jobContext->contexts[i].id = i;
    }
    return jobContext;

}


void getJobState(JobHandle job, JobState *state)
{
    auto *context = static_cast<JobContext *>(job);
    context->state.percentage = context->getProcessedKeys() / (float) context->getTotalKeys();
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
    tc->jobContext->counter += 1;
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
    uint64_t i;
    while ((i = tc->jobContext->nextInputIdx++) < tc->jobContext->getTotalKeys())
    {
        InputPair currPair = tc->jobContext->inputVec[i];
        tc->jobContext->client.map(currPair.first, currPair.second, tc);
    }

    //Sort the intermediate Vector
    std::sort(tc->intermediateVec.begin(), tc->intermediateVec.end(), [](const K2 & lhs, const K2 & rhs)
    {
        return lhs < rhs;
    });

    // barrier before the Shuffle Phase
    tc->jobContext->barrier.barrier();

    //Shuffle Stage only thread 0 call it
    if (tc->id == 0)
    {

        shufflePhase(arg);
        sem_post(&tc->jobContext->semaphore);
    }

    //reduce phase
    sem_wait(&tc->jobContext->semaphore);


    //todo Reduce stage
    // a thread doesnt reduce until it's intermediate vector is already shuffled.

    return nullptr;
}

void shufflePhase(void *arg)
{
    auto *jc = static_cast<JobContext *>(arg);
    jc->state.stage = SHUFFLE_STAGE;
    jc->counter -= jc->getProcessedKeys(); //zero the processed keys in shuffle Stage

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

        for (int i = 0; i < jc->numOfThreads; i++)
        {
            if (!jc->contexts[i].intermediateVec.empty())
            {
                jc->contexts[i].intermediateVec.pop_back();
                std::vector<IntermediatePair> currKeyVector;
                for (int j = 0; j < jc->numOfThreads; j++)
                {
                    if (!(*jc->contexts[j].intermediateVec.back().first < *maxPair->first) &&
                        !(*maxPair->first < *jc->contexts[j].intermediateVec.back().first))
                    {
                        currKeyVector.push_back(jc->contexts[j].intermediateVec.back());
                        jc->counter += 1;
                    }
                }
                jc->shuffledVec.push_back(currKeyVector);
            }
        }
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



