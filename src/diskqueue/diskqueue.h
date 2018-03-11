#ifndef DISKQUEUE_H_
#define DISKQUEUE_H_

#include <stdio.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "queue.h"

typedef uint8_t     u8;
typedef uint16_t    u16;
typedef uint32_t    u32;
typedef uint64_t    u64;
typedef FILE        File;

typedef struct {
    u64     readPos;
    u64     writePos;
    u64     readFileNum;
    u64     writeFileNum;
    u64     depth;

    pthread_mutex_t mutex;


    u64     maxBytesPerFile;
    u32     minMsgSize;
    u32     maxMsgSize;
    u64     syncEvery; // number of writes per fsync
    u64     syncTimeout; // duration of time per fsync
    u32     exitFlag;
    u16      needSync;

    u64     nextReadPos;
    u64     nextReadFileNum;

    File    *readFile;
    File    *writeFile;

    char    *reader;
    char    *writeBuf;

    int     readFd[2];
    int     writeFd[2];    
    int     noticeFd[2];  
    
    // metadata
    char    *name;
    char    *dataPath;

    ngx_queue_t readQueue;
} diskqueue;

typedef struct {
    u32 dataLen;
    void *data;
    ngx_queue_t queue;
} qchunk;

void *New(const char *name, const char *dataPath, u64 maxBytesPerFile, u32 minMsgSize, u32 maxMsgSize, u64 syncEvery);
void *readOne(diskqueue *d);
void *ReadChan(diskqueue *d);

#endif // DISKQUEUE_H_
