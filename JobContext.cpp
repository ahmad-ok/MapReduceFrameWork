//
// Created by ahmadok on 07/06/2021.
//

#include "JobContext.h"
JobContext::JobContext(pthread_t *threads,ThreadContext* contexts , OutputVec &outputVec,const InputVec &inputVec,
                       const MapReduceClient &client) :inputVec(inputVec), outputVec(outputVec), contexts(contexts),
                       client(client), threads(threads), counter(0),state({UNDEFINED_STAGE, 0}), isWaiting(false),
                       nextInputIdx(0)
{

}

void JobContext::setTotalKeys(uint64_t totalKeys)
{

    uint64_t mask = 0x7fffffff;
    //todo : 32 or 31 ???
    counter = (counter & (~mask << (unsigned)31)) | (totalKeys << (unsigned)31);
}

uint64_t JobContext::getTotalKeys()
{
    //todo : 32 or 31 ???
    return (counter >> (unsigned)31) & (unsigned)(0x7fffffff);
}

uint64_t JobContext::getProcessedKeys()
{
    return counter & (0x7fffffff);
}

stage_t JobContext::getStage()
{
    return static_cast<stage_t>((counter >> (unsigned)62 )& (unsigned) 3);
}



JobContext::~JobContext()
{
    pthread_mutex_destroy(&lock);
    delete[] threads;
}