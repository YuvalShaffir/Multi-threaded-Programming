#include "Barrier.h"
#include <cstdlib>
#include <iostream>

#define BARRIER_ERR_MSG(msg) std::cout<<"system error: "<<(msg)<<std::endl;
#define BARRIER_MUTEX_LOCK_ERR "failed to lock mutex"
#define BARRIER_MUTEX_UNLOCK_ERR "failed to unlock mutex"
#define BARRIER_MUTEX_DESTROY_ERR "failed to destroy mutex"
#define BARRIER_COND_DESTROY_ERR "error on pthread_cond_destroy"
#define BARRIER_COND_WAIT_ERR "error on pthread_cond_wait"
#define BARRIER_COND_BROADCAST_ERR "error on pthread_cond_broadcast"

Barrier::Barrier(int numThreads)
		: mutex(PTHREAD_MUTEX_INITIALIZER)
		, cv(PTHREAD_COND_INITIALIZER)
		, count(0)
		, numThreads(numThreads)
{ }


Barrier::~Barrier()
{
	if (pthread_mutex_destroy(&mutex) != 0) {
        BARRIER_ERR_MSG(BARRIER_MUTEX_DESTROY_ERR)
		exit(1);
	}
	if (pthread_cond_destroy(&cv) != 0){
        BARRIER_ERR_MSG(BARRIER_COND_DESTROY_ERR)
		exit(1);
	}
}


void Barrier::barrier()
{
	if (pthread_mutex_lock(&mutex) != 0){
        BARRIER_ERR_MSG(BARRIER_MUTEX_LOCK_ERR)
		exit(1);
	}
	if (++count < numThreads) {
		if (pthread_cond_wait(&cv, &mutex) != 0){
            BARRIER_ERR_MSG(BARRIER_COND_WAIT_ERR)
			exit(1);
		}
	} else {

		count = 0;
		if (pthread_cond_broadcast(&cv) != 0) {
            BARRIER_ERR_MSG(BARRIER_COND_BROADCAST_ERR)
			exit(1);
		}
	}
	if (pthread_mutex_unlock(&mutex) != 0) {
        BARRIER_ERR_MSG(BARRIER_MUTEX_UNLOCK_ERR)
		exit(1);
	}
}
