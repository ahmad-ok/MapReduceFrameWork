//
// Created by ahmadok on 07/06/2021.
//

#include "JobContext.h"

JobContext::JobContext(pthread_t *threads,ThreadContext* contexts , OutputVec &outputVec,const InputVec &inputVec,
                       const MapReduceClient &client, int numOfThreads) :inputVec(inputVec), outputVec(outputVec), contexts(contexts),
                       client(client), threads(threads), counter(0), threadsJoined(false),
                       nextInputIdx(0),numOfVecsToReduce(0),numOfIntermediatePairs(0), numOfThreads(numOfThreads), barrier(Barrier(numOfThreads))
{

}

void JobContext::setTotalKeys(uint64_t totalKeys)
{

    uint64_t mask = 0x7fffffff;
    counter = (counter & (~mask << (unsigned)31)) | (totalKeys << (unsigned)31);
}


uint64_t JobContext::getTotalKeys()
{
    return (counter >> (unsigned)31) & (unsigned)(0x7fffffff);
}

uint64_t JobContext::getProcessedKeys()
{
    return counter & (0x7fffffff);
}

stage_t JobContext::getStage()
{
    return static_cast<stage_t>((counter >> (unsigned)62) & (unsigned) 3);
}


JobContext::~JobContext()
{
    pthread_mutex_destroy(&lock);
    pthread_mutex_destroy(&reduce_lock);
    pthread_mutex_destroy(&stateChange_lock);
    pthread_mutex_destroy(&wait_lock);
    delete[] threads;
    delete[] contexts;
}