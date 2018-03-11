#include "diskqueue.h"

#define META_FMT_STR "%s/%s.diskqueue.meta.dat"
#define DATA_FMT_STR "%s/%s.diskqueue.%06d.dat"
#define TEMP_FMT_STR "%s.%d.tmp"

static int retrieveMetaData(diskqueue *d);
static char *metaDataFileName(diskqueue *d);
static char *fileName(diskqueue *d, u32 filenum);
static void ioLoop(diskqueue *d);
static void workLoop(void *arg);
static void moveForward(diskqueue *d);
static void checkTailCorruption(diskqueue *d);
static void skipToNextRWFile(diskqueue *d);
static void dqsync(diskqueue *d);
static int persistMetaData(diskqueue *d);

u32 ntoh32(u32 value){  
    return (value & 0x000000FFU) << 24 | (value & 0x0000FF00U) << 8 |   
        (value & 0x00FF0000U) >> 8 | (value & 0xFF000000U) >> 24;   
}  

void *New(const char *name, const char *dataPath, u64 maxBytesPerFile, u32 minMsgSize, u32 maxMsgSize, u64 syncEvery)
{
    int res;

    u32 nameLen = strlen(name)+1;
    u32 pathLen = strlen(dataPath)+1;
    diskqueue *d = (diskqueue *)malloc(sizeof(diskqueue));

    if(d == NULL) 
        return NULL;
    
    d->name = (char *)malloc((nameLen));
    if(d->name == NULL)
        goto failed;
    memcpy(d->name, name, nameLen);

    d->dataPath = (char *)malloc((pathLen));
    if(d->dataPath == NULL)
        goto failed;
    memcpy(d->dataPath, dataPath, pathLen);

    pthread_mutex_init(&d->mutex, NULL);

    int ret = pipe(d->readFd);
    if(ret != 0) {
        goto failed;
    }

    ret = pipe(d->noticeFd);
    if(ret != 0) {
        goto failed;
    }

    d->maxBytesPerFile = maxBytesPerFile;
    d->minMsgSize = minMsgSize;
    d->maxMsgSize = maxMsgSize;
    d->syncEvery = syncEvery;

    ngx_queue_init(&d->readQueue);
    
    res = retrieveMetaData(d);

    ioLoop(d);
    
    return d;

failed:
    if(d->name != NULL)
        free(d->name);
    if(d->dataPath != NULL)
        free(d->dataPath);
    free(d);
    return NULL;
}

static
int retrieveMetaData(diskqueue *d)
{
    File *f;
    int err;
    const char *fileName = NULL;

    fileName = metaDataFileName(d);

    f = fopen(fileName, "r");
    if(f == NULL) {
        printf("%s, %s\n", fileName, strerror(errno));
        goto failed;
    }
    fscanf(f, "%lld\n%lld,%lld\n%lld,%lld\n", 
                &d->depth, 
                &d->readFileNum, &d->readPos,
                &d->writeFileNum, &d->writePos);

    printf("depth=%lld,readFileNum=%lld,readPos=%lld,writeFileNum=%lld,writePos=%lld\n",
            d->depth, d->readFileNum, d->readPos, d->writeFileNum, d->writePos);
    d->nextReadPos = d->readPos;
    d->nextReadFileNum = d->readFileNum;
    fclose(f);
    return 1;
failed:
    if(fileName != NULL)
        free((void *)fileName);
    exit(-1);
    return 0;
}

static
char *metaDataFileName(diskqueue *d)
{
    u32 len = strlen(d->dataPath) + strlen(d->name) + 19 + 2;
    char *fileName = malloc(len+1);
    printf("%s,%s,%ld\n", d->dataPath, d->name, strlen(d->name));
    sprintf(fileName, META_FMT_STR, d->dataPath, d->name);
    return fileName;
}

static 
char *fileName(diskqueue *d, u32 filenum) {
    u32 len = strlen(d->dataPath) + strlen(d->name) + 19 + 7;
    char *fileName = malloc(len+1);
    sprintf(fileName, DATA_FMT_STR, d->dataPath, d->name, filenum);
    return fileName;
}

static 
void ioLoop(diskqueue *d)
{
    pthread_t   thread;
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, 1); // Preclude the need to do pthread_join on the thread after it exits.
    pthread_create(&thread, &attr, (void *(*)(void*))workLoop, (void *)d);
    pthread_attr_destroy(&attr);
    pthread_detach(thread);
}

static
void workLoop(void *arg)
{
    diskqueue *d = (diskqueue *)arg;
    qchunk *chunk = NULL;

    for(;;) {
        if(((d->readFileNum < d->writeFileNum) || (d->readPos < d->writePos))) {
            if(d->nextReadPos == d->readPos) {
                chunk = readOne(d);
            }
        } else {
            chunk = NULL;
        }

        char cmd;
        read(d->noticeFd[0], &cmd, 1);

        switch(cmd) {
        case 'n':
            if(chunk != NULL) {
                ngx_queue_insert_tail(&d->readQueue, &chunk->queue);
            } 
            if(!ngx_queue_empty(&d->readQueue)) {
                write(d->readFd[1], "r", 1);
            }
            break;
        case 'm':
            moveForward(d);
            break;
        case 's':
            dqsync(d);
            break;
        }
    }
}

void *readOne(diskqueue *d) {
    int err, rc;
    u32 msgSize;

    if(d->readFile == NULL) {
        char *curFileName = fileName(d, d->readFileNum);

        d->readFile = fopen(curFileName, "r");
        if(d->readFile == NULL) {
            free(curFileName);
            return NULL;
        }

        if(d->readPos > 0) {
            err = fseek(d->readFile, d->readPos, 0);
            if(err != 0) {
                fclose(d->readFile);
                d->readFile = NULL;
                return NULL;
            }
        }
    }

    rc = fread(&msgSize, 1, sizeof(u32), d->readFile); 
    if(rc <= 0) {
        fclose(d->readFile);
        d->readFile = NULL;
        return 0;
    }
    msgSize = ntoh32(msgSize);

    if(msgSize < d->minMsgSize || msgSize > d->maxMsgSize) {
        fclose(d->readFile);
        d->readFile = NULL;
        return NULL;
    }

    qchunk *chunk = (qchunk *)malloc(sizeof(qchunk));
    chunk->dataLen = msgSize;
    chunk->data = (char *)malloc(msgSize+1);

    rc = fread(chunk->data, 1, msgSize, d->readFile);
    if(rc <= 0) {
        fclose(d->readFile);
        d->readFile = NULL;
        return 0;
    }
    
    u64 totalBytes = (4 + msgSize);

    d->nextReadPos = d->readPos + totalBytes;
    d->nextReadFileNum = d->readFileNum;

    if(d->nextReadPos > d->maxBytesPerFile) {
        if(d->readFile != NULL) {
            fclose(d->readFile);
            d->readFile = NULL;
        }
        d->nextReadFileNum++;
        d->nextReadPos = 0;
    }

    return chunk;
}

void *ReadChan(diskqueue *d) {
    ngx_queue_t *q;
    qchunk *chunk = NULL;

    write(d->noticeFd[1], "n", 1); // next
    
    while(1) {
        char ch;
        int n = read(d->readFd[0], &ch, 1);
        if(n < 0) continue;
        switch(ch) {
            case 'r':
                q = ngx_queue_head(&d->readQueue);
                chunk = (qchunk *)ngx_queue_data(q, qchunk, queue);
                ngx_queue_remove(&chunk->queue);
                write(d->noticeFd[1], "m", 1);
                return chunk;
        }
    }
}

static
void moveForward(diskqueue *d) {
    u64 oldReadFileNum = d->readFileNum;
	d->readFileNum = d->nextReadFileNum;
	d->readPos = d->nextReadPos;
    d->depth -= 1;

    if(oldReadFileNum != d->nextReadFileNum) {
        // needSync
        write(d->noticeFd[1], "s", 1);
        
        const char *fn = fileName(d, oldReadFileNum);
        int ret = remove(fn);
        if(ret != 0) {
            printf("remove file failed = %s\n", fn);
        }
        free((void *)fn);
    }

    //
    checkTailCorruption(d);
}

static
void checkTailCorruption(diskqueue *d)
{
    u64 depth = d->depth;

    if(d->readFileNum < d->writeFileNum || d->readPos < d->writePos)
        return;

    if(depth) {
        d->depth = 0;
        // needSync
    }

    if(d->readFileNum != d->writeFileNum) {
        skipToNextRWFile(d);
        //needSync
    }
}

static
void skipToNextRWFile(diskqueue *d)
{
    int err;

    if(d->readFile != NULL) {
        fclose(d->readFile);
        d->readFile = NULL;
    }

    if(d->writeFile != NULL) {
        fclose(d->writeFile);
        d->writeFile = NULL;
    }

    for(u64 i = d->readFileNum; i <= d->writeFileNum; i++) {
        const char *fn = fileName(d, i);
        remove(fn);
        free((void *)fn);
    }

    d->writeFileNum++;
    d->writePos = 0;
    d->readFileNum = d->writeFileNum;
    d->readPos = 0;
    d->nextReadFileNum = d->writeFileNum;
    d->nextReadPos = 0;
    d->depth = 0;
}

static
void dqsync(diskqueue *d)
{
    if(d->writeFile != NULL) {
        fflush(d->writeFile);
        fclose(d->writeFile);
        d->writeFile = NULL;
        return;
    }
    persistMetaData(d);
}

static
int persistMetaData(diskqueue *d)
{
    File *f;
    int err;
    srand((unsigned)time(NULL));

    const char *fileName = metaDataFileName(d);
    int tmpLen = strlen(fileName) + 4 + 2 + 3;
    char tmpFileName[tmpLen+1];
    sprintf(tmpFileName, TEMP_FMT_STR, fileName, (int)random()%6379);

    f = fopen(tmpFileName, "w+");
    if(f == NULL) {
        printf("%s, %s\n", tmpFileName, strerror(errno));
        return 0;
    }
    fprintf(f, "%lld\n%lld,%lld\n%lld,%lld\n",
            d->depth, 
            d->readFileNum, d->readPos,
            d->writeFileNum, d->writePos);

    fflush(f);
    fclose(f);
    rename(tmpFileName, fileName);
    return 1;
}