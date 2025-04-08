#ifndef BENCHMARK_H
#define BENCHMARK_H

#include <assert.h>
#include <math.h>
#include <pthread.h>
#include <semaphore.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <chrono>
#include <queue>
#include <random>
#include <string>
#include <unordered_set>
#include <atomic>

using namespace std;

#define FALSE 0
#define TRUE 1
#define YCSB_KEY_NUM 400
#define KMEANS_KEY_NUM 400000
#define BANK_KEY_NUM 2000000

struct Request {
    enum Type {
        GET,
        PUT,
        KMEANS,
        TransactSavings,
        DepositChecking,
        SendPayment,
        WriteCheck,
        Amalgamate,
        Query
    };
    Type type;
    string key;
    string value;
    bool is_prep;
};

class RequestQueue {
   public:
    queue<struct Request> rq_queue;
    pthread_mutex_t mutex;
    sem_t full;

    RequestQueue() {
        pthread_mutex_init(&mutex, NULL);
        sem_init(&full, 0, 0);
    }

    ~RequestQueue() {
        pthread_mutex_destroy(&mutex);
        sem_destroy(&full);
    }
};

int test_get_only();
int test_get_put_mix();
void prepopulate();
int64_t benchmark_throughput(bool is_validator);
int zipf(double alpha, int n);
double rand_val(int seed);
void kmeans(vector<int> &A, int K);
string get_balance_str(uint64_t balance, size_t length);
void start_client();

#endif