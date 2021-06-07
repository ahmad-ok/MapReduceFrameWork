#include "MapReduceFramework.h"
#include "JobContext.h"



#define MUTEX_INIT_FAIL "system error: initializing mutex for thread failed\n"
#define THREAD_INIT_FAIL "system error: creating thread failed\n"


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
    auto* threads = new pthread_t[multiThreadLevel];
    auto *contexts = new ThreadContext[multiThreadLevel];
    auto *jobContext = new JobContext(threads, contexts, outputVec, inputVec, client, multiThreadLevel);

    for (int i = 0; i < multiThreadLevel; ++i)
    {
        if (pthread_create(jobContext->threads + i, nullptr, thread_job, jobContext->contexts + i))
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
    context->state.percentage = context->getProcessedKeys()/(float)context->getTotalKeys();
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
    tc->jobContext->counter +=  1;
}

void emit3(K3 *key, V3 *value, void *context)
{
    //todo : wrap in function
    auto *jc = static_cast<JobContext *>(context);
    pthread_mutex_lock(&jc->lock);
    jc->outputVec.push_back(OutputPair(key, value));
    // todo : add to correct place plz
    jc->counter +=  1;
    pthread_mutex_unlock(&jc->lock);
}

void *mapThread(void *arg)
{
    auto* tc = static_cast<ThreadContext*>(arg);
    tc->jobContext->state.stage = MAP_STAGE;
    uint64_t i;
    while((i = tc->jobContext->nextInputIdx++) < tc->jobContext->getTotalKeys()){
        InputPair currPair = tc->jobContext->inputVec[i];
        tc->jobContext->client.map(currPair.first, currPair.second, tc);
        //todo increment processed counter
    }
    tc->jobContext->barrier.barrier();

    //todo Reduce stage

    return nullptr;
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



