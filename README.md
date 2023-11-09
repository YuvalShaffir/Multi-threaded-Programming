# Multi-threaded-Programming
![image](https://github.com/YuvalShaffir/Multi-threaded-Programming/assets/34415892/bee20622-cc9e-4e17-a9e0-c6bc46156ad8)

## Introduction
Performance is the primary motivation for multi-threaded programming, as it allows multiple processors to execute tasks simultaneously, reducing the overall computation time. However, challenges arise in dividing tasks into smaller, parallelizable parts and managing synchronization and communication between threads.

In this exercise, we will implement the MapReduce framework, a design that addresses these challenges. MapReduce parallelizes tasks with a specific structure by employing two key functions: map and reduce. This framework splits the implementation into two parts: the client, responsible for defining map and reduce functions specific to the task, and the framework, handling the overall execution, synchronization, and communication between threads.

## Features

Client Overview:
The client contains two main functions: map and reduce.
Elements are represented as pairs (key, value) for linear ordering.
Three types of elements: input, intermediary, and output, each with its own key and value types.

Framework Interface:
The framework supports running MapReduce operations as asynchronous jobs.
Provides functionalities like starting a job, waiting for a job to finish, querying the current state of a job, and closing a job handle.
Framework Implementation Details:

Threads execute phases such as Map, Sort, Shuffle, and Reduce, controlled by a barrier.
Map Phase: Threads read input pairs and call the map function, solving synchronization challenges using atomic variables.
Sort Phase: Each thread sorts its intermediate vector independently.
Shuffle Phase: Thread 0 combines vectors, creating new sequences based on identical keys.
Reduce Phase: Reducing threads wait for shuffled vectors to perform reduce operations.
## How to Use
Client Implementation:
Define the map and reduce functions in the client, extending the provided MapReduceClient.h.

Framework Implementation:
Implement the MapReduce framework using the provided functions in MapReduceFramework.h.
Use startMapReduceJob to initiate a job, providing the client, input vector, output vector, and the desired number of worker threads.

Job Monitoring:
Use waitForJob to wait until a job is finished.
Retrieve the current state of a job using getJobState.

Closing a Job Handle:
Release resources using closeJobHandle. Ensure the job is finished before calling this function.

Emit Functions:
Use emit2 and emit3 in the client's map and reduce functions, respectively, to produce and save key-value pairs.
Follow the provided design and guidelines for efficient implementation.
