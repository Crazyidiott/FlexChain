#ifndef LOG_H
#define LOG_H

#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>  // 为abort()添加

extern pthread_mutex_t logger_lock;

#define log_err(M, ...)                                       \
    do {                                                      \
        pthread_mutex_lock(&logger_lock);                     \
        fprintf(stderr, "[ERROR] (%s:%d:%s) " M "\n",         \
                __FILE__, __LINE__, __func__, ##__VA_ARGS__); \
        pthread_mutex_unlock(&logger_lock);                   \
    } while (0)

#define log_warn(M, ...)                                      \
    do {                                                      \
        pthread_mutex_lock(&logger_lock);                     \
        fprintf(stderr, "[WARN] (%s:%d:%s) " M "\n",          \
                __FILE__, __LINE__, __func__, ##__VA_ARGS__); \
        pthread_mutex_unlock(&logger_lock);                   \
    } while (0)

#define log_info(fp, M, ...)                                  \
    do {                                                      \
        pthread_mutex_lock(&logger_lock);                     \
        if (fp != NULL) {                                     \
            int ret = fprintf(fp, "[INFO]" M "\n", ##__VA_ARGS__); \
            if (ret < 0) {                                    \
                fprintf(stderr, "[ERROR] Log write failed: %s\n", strerror(errno)); \
            }                                                 \
            if (fp != stderr && fp != stdout) {               \
                fflush(fp);                                   \
            }                                                 \
        } else {                                              \
            fprintf(stderr, "[ERROR] Attempted to log to NULL file pointer\n"); \
        }                                                     \
        pthread_mutex_unlock(&logger_lock);                   \
    } while (0)

#ifdef DEBUG
#define log_debug(fp, M, ...)                                 \
    do {                                                      \
        pthread_mutex_lock(&logger_lock);                     \
        if (fp != NULL) {                                     \
            int ret = fprintf(fp, "[DEBUG]" M "\n", ##__VA_ARGS__); \
            if (ret < 0) {                                    \
                fprintf(stderr, "[ERROR] Log write failed: %s\n", strerror(errno)); \
            }                                                 \
            if (fp != stderr && fp != stdout) {               \
                fflush(fp);                                   \
            }                                                 \
        } else {                                              \
            fprintf(stderr, "[ERROR] Attempted to log to NULL file pointer\n"); \
        }                                                     \
        pthread_mutex_unlock(&logger_lock);                   \
    } while (0)
#else
#define log_debug(fp, M, ...)
#endif

#endif