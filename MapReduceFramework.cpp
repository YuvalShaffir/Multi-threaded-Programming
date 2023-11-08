#ifndef MAPREDUCEFRAMEWORK_CPP
#define MAPREDUCEFRAMEWORK_CPP

#include "MapReduceFramework.h"
#include "Barrier.h"
#include <pthread.h>
#include <vector>
#include <algorithm>// std::sort
#include <atomic>
#include <iostream>
#include <unistd.h>
using namespace std;

#define ERR_MSG(msg) cout<<"system error: "<<(msg)<<endl;
#define THREAD_CREATION_ERR "failed to create thread"
#define MEMORY_ALLOCATION_ERR "failed to allocate memory"
#define MUTEX_LOCK_ERR "failed to lock mutex"
#define MUTEX_UNLOCK_ERR "failed to unlock mutex"
#define MUTEX_DESTROY_ERR "failed to destroy mutex"
#define MUTEX_INIT_ERR "failed to init mutex"
#define THREAD_JOIN_ERR "failed to join thread"

#define MUTEX_NUM 6

#define FAILURE 1
#define SUCCESS 0

#define INCREASE_PROCESSED(ac) (ac)+= 1UL<<31
#define INCREASE_STAGE(ac) (ac)+= 1UL<<62
typedef std::atomic <uint64_t> ato_counter;


enum mutexes {start_mutex, emit_mutex, map_mutex, shuffle_mutex, waiting_mutex, percentage_mutex};

typedef struct {
    const MapReduceClient& client;
    vector<pthread_t>* threads; // vector of threads
    JobState state;
    ato_counter* counter; // a 64 bit atomic variable: 31 bits keys processed + 31 bits num of keys + 2 bits stage
    const InputVec& input;
    vector<IntermediateVec*>* shuffle_vec_end; // output vector of vectors from the shuffle function
    vector<IntermediateVec*>* threads_outputs; // all the outputs of the threads
    bool waiting_for_me;
    OutputVec& output_vec;
    Barrier* barrier;
    pthread_mutex_t mutexes[MUTEX_NUM];
    int shuffle_counter;
    unsigned long input_size;
    bool shuffle_finished;
    bool map_started;
    bool reduce_started;
} JobContext;


typedef struct {
    JobContext* job_context;
    IntermediateVec* thread_output_vec;
}ThreadContext;


void reset_first_31_bits(void *job) {
    auto *the_job = (JobContext *) job;
    uint64_t val = ((*(the_job->counter))).load() & (0xffffffff80000000ul);
    ((*(the_job->counter))) = val;
}

void reset_31_62_bits(void *job) {
    auto *the_job = (JobContext *) job;
    uint64_t val = ((*(the_job->counter))).load() & (0xc00000007ffffffful);
    ((*(the_job->counter))) = val;
}

void lock_mutex(pthread_mutex_t *mutex) {
    /* Locks the mutex
     * @param mutex the mutex to lock
     * */
    if (pthread_mutex_lock(mutex) != 0) {
        ERR_MSG(MUTEX_LOCK_ERR)
        exit(FAILURE);
    }
}


void unlock_mutex(pthread_mutex_t *mutex) {
    /* Unlocks the mutex
     * @param mutex the mutex to unlock
     * */
    if (pthread_mutex_unlock(mutex) != 0) {
        ERR_MSG(MUTEX_UNLOCK_ERR)
        exit(FAILURE);
    }
}


void free_job(JobContext* jc){
    /* Frees the job context*/
    jc->threads->clear();
    delete jc->threads;
    jc->threads = nullptr;

    for(auto& vec : *(jc->threads_outputs)){
        vec->clear();
        delete vec;
        vec = nullptr;
    }
    jc->threads_outputs->clear();
    delete jc->threads_outputs;
    jc->threads_outputs = nullptr;

    for(auto& vec : *(jc->shuffle_vec_end)){
        vec->clear();
        delete vec;
        vec = nullptr;
    }
    delete jc->shuffle_vec_end;
    jc->shuffle_vec_end = nullptr;

    delete jc->counter;
    jc->counter = nullptr;

    jc->barrier->~Barrier();
    delete jc->barrier;
    jc->barrier = nullptr;

    for(int i=0; i<MUTEX_NUM; ++i){
        if(pthread_mutex_destroy(&jc->mutexes[i])!=0){
            ERR_MSG(MUTEX_DESTROY_ERR)
            exit(FAILURE);
        }
    }

    delete jc;
}


void map_stage(ThreadContext* tc) {
    /* The map stage - runs the client map function on all the input pairs
     * @param tc - Thread content holding a pointer to the JobContext and the thread_output_vec*/
    lock_mutex(&tc->job_context->mutexes[percentage_mutex]);
    if (!(tc->job_context->map_started))
    {
        tc->job_context->map_started = true;
        INCREASE_STAGE(*(tc->job_context->counter));
        tc->job_context->state.stage = MAP_STAGE;
    }
    unlock_mutex(&tc->job_context->mutexes[percentage_mutex]);
    auto jc = tc->job_context;
//    printf(" - map stage: (thread id = %lu)\n", pthread_self());
    uint32_t size = jc->input.size();
    ato_counter* counter = jc->counter;
    const InputVec& input = jc->input;

    uint32_t old_value;
    while ((old_value = ((*counter))++ & (0x7ffffffful)) < size) {
//        printf("   - old_value = %u\n", old_value);
        InputPair p = input[old_value];
        jc->client.map(p.first, p.second, tc);
        INCREASE_PROCESSED(*(tc->job_context->counter));
    }
}


void reduce_stage(ThreadContext* tc) {
    lock_mutex(&tc->job_context->mutexes[percentage_mutex]);
    if (!(tc->job_context->reduce_started))
    {
        reset_31_62_bits(tc->job_context);
        reset_first_31_bits(tc->job_context);
        tc->job_context->state.stage = REDUCE_STAGE;
        tc->job_context->reduce_started = true;
        INCREASE_STAGE(*(tc->job_context->counter));
        tc->job_context->input_size = tc->job_context->shuffle_vec_end->size();
    }
    unlock_mutex(&tc->job_context->mutexes[percentage_mutex]);
//    printf(" - reduce stage: (thread id = %lu)\n", pthread_self());
    uint32_t size = tc->job_context->shuffle_vec_end->size();
  ato_counter* counter = tc->job_context->counter;
  auto input = tc->job_context->shuffle_vec_end;
  uint32_t old_value;
    while ((old_value = ((*counter))++ & (0x7ffffffful)) < size)
    {
        tc->job_context->client.reduce(input->at (old_value), tc);
        INCREASE_PROCESSED(*(tc->job_context->counter));
//        printf("   - old_value = %u, (thread id= %lu)\n", old_value, pthread_self());
    }

}

bool comparator(const IntermediatePair p1, const IntermediatePair p2) {
    /* Compares two pairs by their keys
     * @param p1 the first pair
     * @param p2 the second pair
     * @return true if the first pair's key is smaller than the second pair's key, false otherwise*/
    return *(p1.first) < *(p2.first);
}

void sort_stage(IntermediateVec* vec_to_sort) {
    /* Sorts the given vector
     * @param vec_to_sort the vector to sort
     * */
//    printf(" - sort stage: (thread id = %lu, vec size = %lu)\n", pthread_self(), vec_to_sort->size());
    std::sort(vec_to_sort->begin(), vec_to_sort->end(), comparator);
}


bool key_comp(K2 *k1, K2 *k2) {
//    bool f_s = (*(k1) < *(k2));
//    bool s_f = (*(k2) < *(k1));
//    if (!f_s && !s_f){return true;}
//    return false;
    return !(*(k1) < *(k2)) && !(*(k2) < *(k1));
}


void shuffle_stage(JobContext* jc)
{
    /* The shuffle stage - sorts the intermediate vector and inserts all pairs with the same key to a single vector
     * @param jc - the JobContext*/
    uint32_t new_input_size =0;
    for (auto& vec : *(jc->threads_outputs))
    {
        new_input_size += vec->size();
    }
    lock_mutex(&jc->mutexes[percentage_mutex]);
    reset_31_62_bits(jc);
    jc->input_size = new_input_size;
    INCREASE_STAGE(*(jc->counter));
    jc->state.stage = SHUFFLE_STAGE;
    unlock_mutex(&jc->mutexes[percentage_mutex]);
    auto outputs_vec = jc->threads_outputs;
//    printf(" - shuffle stage: (thread id = %lu)\n", pthread_self());
    IntermediateVec temp;
    // insert all pairs in the output vectors of all threads to a single vector:
    for (const auto& output : *outputs_vec) {
        temp.insert(temp.end(), output->begin(), output->end());
    }
    // sort the vector:
    std::sort(temp.begin(), temp.end(), comparator);

    // insert all pairs with the same key to a single vector and insert it to shuffled_vec_end:
    if (!temp.empty())
    {
        auto temp_size = temp.size();
        K2* first = temp[0].first;
        auto vec = jc->shuffle_vec_end;
        while (jc->shuffle_counter < temp_size)
        {
            auto* to_insert = new IntermediateVec();

            // insert all pairs with the same key to a single vector:
            while (key_comp(static_cast<K2*>(first), static_cast<K2*>(temp[jc->shuffle_counter].first)))
            {
                to_insert->push_back(temp[jc->shuffle_counter]);
                INCREASE_PROCESSED(*(jc->counter));
                jc->shuffle_counter++;
                if(jc->shuffle_counter >= temp_size) {break;}
            }

            // check if jc->shuffle_counter is still within range to prevent accessing out-of-bounds elements:
            first = (jc->shuffle_counter < temp_size) ? temp[jc->shuffle_counter].first : nullptr;
            vec->push_back(to_insert);
        }
    }
}


void* thread_scheme(void* arg) {
    /* The thread scheme is as follows: Map -> Sort -> Shuffle -> Reduce
     * @param arg aka the job handler
     * */

    auto* tc = (ThreadContext*)arg;
//    printf(" - thread_scheme: (thread id = %lu)\n", pthread_self());
    // map:
    map_stage(tc);

    // sort:
    sort_stage(tc->thread_output_vec);

    (*(tc->job_context->barrier)).barrier(); // wait for all the threads to be here.

    // shuffle:
    lock_mutex(&tc->job_context->mutexes[shuffle_mutex]);
    // only one thread should shuffle:
    if(!(tc->job_context->shuffle_finished)) {
        shuffle_stage(tc->job_context);
        tc->job_context->shuffle_finished = true;
    }
    unlock_mutex(&tc->job_context->mutexes[shuffle_mutex]);

    (*(tc->job_context->barrier)).barrier(); // wait for all the threads to be here.

    // reduce:
    reduce_stage(tc);
//    usleep(150000);
//    printf(" - thread_scheme: (thread id = %lu) finished\n", pthread_self());
//    delete tc->thread_output_vec;
    delete tc;
    return nullptr;
}


int create_threads(int num, JobContext* job) {
/* Creates threads
     * @param num the number of threads to create
     * @param t the vector of threads
     * @param job the job handler
     * */
    for (size_t i = 0; i < num; ++i) {
        auto* thread_context = new ThreadContext();
        thread_context->job_context = job;
        thread_context->thread_output_vec = job->threads_outputs->at(i);
        if(pthread_create(&job->threads->at(i), nullptr, thread_scheme, thread_context) != 0){
            ERR_MSG(THREAD_CREATION_ERR)
            exit(FAILURE);
        }
//        printf("created thread (%lu)\n", i);

    }
    return SUCCESS;
}

JobContext* create_job(const MapReduceClient &client,
               const InputVec &inputVec, OutputVec &outputVec,
               int multiThreadLevel)
{
    auto* c = new atomic<uint64_t>(0);


    // Initialize the job and it's variables
    auto *job = new JobContext{
        .client = client,
        .threads = new vector<pthread_t>(multiThreadLevel),
        .state = {UNDEFINED_STAGE, 0},
        .counter = c,
        .input = inputVec,
        .shuffle_vec_end = new vector<IntermediateVec*>(),
        .threads_outputs = new vector<IntermediateVec*>(multiThreadLevel),
        .waiting_for_me = false,
        .output_vec = outputVec,
        .barrier = new Barrier(multiThreadLevel),
        .mutexes = {0} ,
        .shuffle_counter = 0,
        .input_size = inputVec.size(),
        .shuffle_finished = false,
        .map_started = false,
        .reduce_started = false,
    };
    // Initialize the mutexes
    for(auto & mutex : job->mutexes)
    {
        if(pthread_mutex_init(&mutex, nullptr)!=0){
            ERR_MSG(MUTEX_INIT_ERR)
            exit(FAILURE);
        }
    }
    for(auto& output : *job->threads_outputs)
    {
        output = new IntermediateVec();
    }
    if(!(job->threads || job->counter || job->shuffle_vec_end || job->threads_outputs ||
     job->barrier))
    {
        ERR_MSG(MEMORY_ALLOCATION_ERR)
        exit(FAILURE);
    }
    return job;
}



/*
context for is a IntermediateVec&
*/
void emit2(K2 *key, V2 *value, void *context)
{
    auto tc = (ThreadContext*) context;
//    lock_mutex(&tc->job_context->mutexes[emit_mutex]);
    tc->thread_output_vec->push_back(IntermediatePair(key, value));
//    unlock_mutex(&tc->job_context->mutexes[emit_mutex]);

}


void emit3(K3 *key, V3 *value, void *context) {
    auto ouput_vector = &((ThreadContext *) context)->job_context->output_vec;
    OutputPair p = OutputPair(key, value);
    lock_mutex( &((ThreadContext *) context)->job_context->mutexes[emit_mutex]);
    ouput_vector->push_back(p);
    unlock_mutex(&((ThreadContext *) context)->job_context->mutexes[emit_mutex]);
}


JobHandle startMapReduceJob(const MapReduceClient &client,
                            const InputVec &inputVec, OutputVec &outputVec,
                            int multiThreadLevel)
{
    JobContext* job = create_job(client, inputVec, outputVec, multiThreadLevel);
    // Create the threads that run all the stages
    lock_mutex(&job->mutexes[start_mutex]);
    create_threads(multiThreadLevel, job);
    unlock_mutex(&job->mutexes[start_mutex]);

    return static_cast<JobHandle>(job);
}

void waitForJob(JobHandle job)
{
    auto *the_job = (JobContext*) job;
    lock_mutex(&the_job->mutexes[waiting_mutex]);
    if (!(the_job->waiting_for_me)){
        the_job->waiting_for_me = true;
        for(auto& thread : *the_job->threads)
        {
            if(pthread_join(thread, nullptr) != 0){
                ERR_MSG(THREAD_JOIN_ERR)
                exit(FAILURE);
            }
        }
    }
    unlock_mutex(&the_job->mutexes[waiting_mutex]);

}

void getJobState(JobHandle job, JobState *state) {
    auto *the_job = (JobContext*) job;
    lock_mutex(&the_job->mutexes[percentage_mutex]);
    state->stage = the_job->state.stage;
    auto num_processed = (the_job->counter->load() >> 31) & (0x7ffffffful);
//    printf("num_processed: %lu , input_size = %lu\n", num_processed, the_job->input_size);
    state->percentage = ((float)num_processed)/(float)the_job->input_size*100;
    unlock_mutex(&the_job->mutexes[percentage_mutex]);
}

void closeJobHandle(JobHandle job) {
    auto*the_job = (JobContext*) job;
    waitForJob(the_job);
    free_job(the_job);
//    printf("closed job\n");
}


#endif //MAPREDUCEFRAMEWORK_CPP