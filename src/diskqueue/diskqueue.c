#include "diskqueue.h"
#include "../util/log.h"

#define META_FMT_STR "%s/%s.diskqueue.meta.dat"
#define DATA_FMT_STR "%s/%s.diskqueue.%06d.dat"
#define TEMP_FMT_STR "%s.%d.tmp"

static int retrieveMetaData(diskqueue *d);
static char *metaDataFileName(diskqueue *d);
static char *fileName(diskqueue *d, u32 filenum);
static void moveForward(diskqueue *d);
static void checkTailCorruption(diskqueue *d);
static void skipToNextRWFile(diskqueue *d);
static void dqsync(diskqueue *d);
static int persistMetaData(diskqueue *d);
static int writeOne(diskqueue *d, char *data, u32 dataLen);
static void handleReadError(diskqueue *d);

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
    d->writeFile = NULL;
    d->readFile = NULL;
    d->depth = 0;
    d->readPos = 0;
    d->writePos = 0;
    d->writeFileNum = 0;
    d->nextReadFileNum = 0;
    d->readFileNum = 0;

    res = retrieveMetaData(d);

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

    log_debug("depth=%lld,readFileNum=%lld,readPos=%lld,writeFileNum=%lld,writePos=%lld",
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
    log_debug("%s,%s,%ld", d->dataPath, d->name, strlen(d->name));
    sprintf(fileName, META_FMT_STR, d->dataPath, d->name);
    return fileName;
}

static
char *fileName(diskqueue *d, u32 filenum) {
    u32 len = strlen(d->dataPath) + strlen(d->name) + 19 + 7;
    char *fileName = malloc(len+1);
    sprintf(fileName, DATA_FMT_STR, d->dataPath, d->name, filenum);
    fileName[len+1] = '\0';
    return fileName;
}

void *readOne(diskqueue *d, u32 *dataLen) {
    int err, rc;
    u32 msgSize;

    if(d->readFile == NULL) {
        char *curFileName = fileName(d, d->readFileNum);

        d->readFile = fopen(curFileName, "r");
        if(d->readFile == NULL) {
            log_debug("curFileName is error %s", curFileName);
            free(curFileName);
            return NULL;
        }

        if(d->readPos > 0) {
            err = fseek(d->readFile, d->readPos, 0);
            if(err != 0) {
                fclose(d->readFile);
                d->readFile = NULL;
                free(curFileName);
                return NULL;
            }
        }
        free(curFileName);
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

    char *data = (char *)malloc(msgSize+1);
    *dataLen = msgSize;

    rc = fread(data, 1, msgSize, d->readFile);
    if(rc <= 0) {
        fclose(d->readFile);
        d->readFile = NULL;
        free(data);
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

    return data;
}

void *readData(diskqueue *d, u32 *len) {
    char *data = NULL;
    if(((d->readFileNum < d->writeFileNum) || (d->readPos < d->writePos))) {
        if(d->nextReadPos == d->readPos) {
            data = readOne(d, len);
            if(data == NULL) {
                handleReadError(d);
            } else {
                moveForward(d);
            }
        }
    } else {
        data = NULL;
    }
    return data;
}

static
void moveForward(diskqueue *d) {
    u64 oldReadFileNum = d->readFileNum;
	d->readFileNum = d->nextReadFileNum;
	d->readPos = d->nextReadPos;
    d->depth -= 1;

    // 
    dqsync(d);
    if(oldReadFileNum != d->nextReadFileNum) {

        const char *fn = fileName(d, oldReadFileNum);
        int ret = remove(fn);
        if(ret != 0) {
            log_debug("remove file failed = %s", fn);
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
        dqsync(d);
    }

    if(d->readFileNum != d->writeFileNum) {
        skipToNextRWFile(d);
        dqsync(d);
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

    log_debug("%s", tmpFileName);
    f = fopen(tmpFileName, "w");
    if(f == NULL) {
        free((char *)fileName);
        log_debug("%s, %s\n", tmpFileName, strerror(errno));
        return 0;
    }
    fprintf(f, "%lld\n%lld,%lld\n%lld,%lld\n",
            d->depth,
            d->readFileNum, d->readPos,
            d->writeFileNum, d->writePos);

    fflush(f);
    fclose(f);
    rename(tmpFileName, fileName);
    free((char *)fileName);
    return 1;
}

int
putData(diskqueue *d, char *data, const u32 dataLen)
{
    pthread_mutex_lock(&d->mutex);
    writeOne(d, data, dataLen);
    pthread_mutex_unlock(&d->mutex);
    return 1;
}

static
int writeOne(diskqueue *d, char *data, u32 dataLen)
{
    int err;

    if(d->writeFile == NULL) {
        char *curFileName = fileName(d, d->writeFileNum);

        d->writeFile = fopen(curFileName, "a");
        if(d->writeFile == NULL) {
            free(curFileName);
            log_debug("open file error");
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
        free(curFileName);
    }

    if(dataLen < d->minMsgSize || dataLen > d->maxMsgSize) {
        return 0;
    }

    u32 len32 = htonl(dataLen);

    // 需要合并一次写入
    err = fwrite((void *)&len32, 4, 1, d->writeFile);

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

    log_debug("pos=%lld,depth=%lld", d->writePos, d->depth);
    // force flush meta file TODO
    dqsync(d);

    if(d->writePos > d->maxBytesPerFile) {
        d->writeFileNum++;
        d->writePos = 0;
        if(d->writeFile != NULL) {
            fclose(d->writeFile);
            d->writeFile = NULL;
        }
    }
    return 1;
}

static
void handleReadError(diskqueue *d)
{
    if(d->readFileNum == d->writeFileNum) {
        if(d->writeFile != NULL) {
            fclose(d->writeFile);
            d->writeFile = NULL;
        }
        d->writeFileNum++;
        d->writePos = 0;
    }

    char *badFn = fileName(d, d->readFileNum);
    char *badRenameFn = malloc(sizeof(strlen(badFn)) + 5);
    sprintf(badRenameFn, "%s.bad", badFn);

    int ret = rename(badFn, badRenameFn);
    if(ret < 0) {
        log_debug("rename error");
    }

    d->readFileNum++;
    d->readPos = 0;
    d->nextReadFileNum = d->readFileNum;
    d->nextReadPos = 0;

    dqsync(d);

    free(badFn);
    free(badRenameFn);
}
