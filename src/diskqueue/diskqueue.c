#include "diskqueue.h"

#define META_FMT_STR "%s/%s.diskqueue.meta.dat"
#define DATA_FMT_STR "%s/%s.diskqueue.%06d.dat"
#define TEMP_FMT_STR "%s.%d.tmp"

static int retrieveMetaData(diskqueue *d);
static char *metaDataFileName(diskqueue *d);
static char *fileName(diskqueue *d, u32 filenum);
static void ioLoop(diskqueue *d);
static void workReadLoop(void *arg);
static void workWriteLoop(void *arg);
static void moveForward(diskqueue *d);
static void checkTailCorruption(diskqueue *d);
static void skipToNextRWFile(diskqueue *d);
static void dqsync(diskqueue *d);
static int persistMetaData(diskqueue *d);
int writeOne(diskqueue *d, char *data, u32 dataLen);

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

    d->maxBytesPerFile = maxBytesPerFile;
    d->minMsgSize = minMsgSize;
    d->maxMsgSize = maxMsgSize;
    d->syncEvery = syncEvery;

    ngx_queue_init(&d->readQueue);
    ngx_queue_init(&d->writeQueue);
    
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
    pthread_create(&thread, &attr, (void *(*)(void*))workReadLoop, (void *)d);
    pthread_attr_destroy(&attr);
    pthread_detach(thread);

    pthread_t   thread2;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, 1); // Preclude the need to do pthread_join on the thread after it exits.
    pthread_create(&thread2, &attr, (void *(*)(void*))workWriteLoop, (void *)d);
    pthread_attr_destroy(&attr);
    pthread_detach(thread2);
}

static
void workWriteLoop(void *arg)
{
    diskqueue *d = (diskqueue *)arg;
    qchunk *chunk = NULL;
    ngx_queue_t *q;
}

static
void workReadLoop(void *arg)
{
    diskqueue *d = (diskqueue *)arg;
    qchunk *chunk = NULL;
    ngx_queue_t *q;

    for(;;) {
        if(((d->readFileNum < d->writeFileNum) || (d->readPos < d->writePos))) {
            if(d->nextReadPos == d->readPos) {
                chunk = readOne(d);
            }
        } else {
            chunk = NULL;
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
    msgSize = ntohl(msgSize);

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

void *readData(diskqueue *d) {
    ngx_queue_t *q;
    qchunk *chunk = NULL;
    return NULL;
}

static
void moveForward(diskqueue *d) {
    u64 oldReadFileNum = d->readFileNum;
	d->readFileNum = d->nextReadFileNum;
	d->readPos = d->nextReadPos;
    d->depth -= 1;

    if(oldReadFileNum != d->nextReadFileNum) {
        
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

    printf("%s\n", tmpFileName);
    f = fopen(tmpFileName, "w");
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

int
putData(diskqueue *d, char *data, const u32 dataLen)
{
    pthread_mutex_lock(&d->mutex); 
    qchunk *chunk = malloc(sizeof(qchunk));
    chunk->data = data;
    chunk->dataLen = dataLen;
    ngx_queue_insert_tail(&d->writeQueue, &chunk->queue);

    return 0;
}

int writeOne(diskqueue *d, char *data, u32 dataLen)
{
    int err;

    if(d->writeFile == NULL) {
        char *curFileName = fileName(d, d->writeFileNum);
        d->writeFile = fopen(curFileName, "a");
        if(d->writeFile == NULL) {
            free(curFileName);
            printf("open file error");
            return 0;
        }

        if(d->writePos > 0) {
            err = fseek(d->writeFile, d->writePos, 0);
            if(err != 0) {
                free(curFileName);
                fclose(d->writeFile);
                d->writeFile = NULL;
                return 0;
            }
        }
    }

    if(dataLen < d->minMsgSize || dataLen > d->maxMsgSize) {
        return 0;
    }
    
    u32 len32 = htonl(dataLen);
    
    // 需要合并一次写入
    err = fwrite(&len32, 4, 1, d->writeFile);
    if(err <= 0) { 
       return 0;
    }
    err = fwrite(data, dataLen, 1, d->writeFile);
    if(err <= 0) {
        return 0;
    }
    // need flush
    fflush(d->writeFile);

    d->writePos += (4+dataLen);
    d->depth += 1;

    printf("pos=%lld,depth=%lld\n", d->writePos, d->depth);

    if(d->writePos > d->maxBytesPerFile) {
        d->writeFileNum++;
        d->writePos = 0;
        dqsync(d);

        if(d->writeFile != NULL) {
            fclose(d->writeFile);
            d->writeFile = NULL;
        }
    }
    return 1;
}