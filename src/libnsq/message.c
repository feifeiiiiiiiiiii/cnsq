#include "nsq.h"

#ifdef DEBUG
#define _DEBUG(...) fprintf(stdout, __VA_ARGS__)
#else
#define _DEBUG(...) do {;} while (0)
#endif

uint64_t ntoh64(const uint8_t *data) {
    return (uint64_t)(data[7]) | (uint64_t)(data[6])<<8 |
        (uint64_t)(data[5])<<16 | (uint64_t)(data[4])<<24 |
        (uint64_t)(data[3])<<32 | (uint64_t)(data[2])<<40 |
        (uint64_t)(data[1])<<48 | (uint64_t)(data[0])<<56;
}

static uint32_t decodeInt32(unsigned char *enc) {
    return enc[0]|(enc[1]<<8)|(enc[2]<<16)|(enc[3]<<24);
}

static int64_t decodeInt64(unsigned char *enc) {
    int64_t n = 0;
    n = (((int64_t)enc[0] << 56) & 0xFF00000000000000U)
        | (((int64_t)enc[1] << 48) & 0x00FF000000000000U)
        | (((int64_t)enc[2] << 40) & 0x0000FF0000000000U)
        | (((int64_t)enc[3] << 32) & 0x000000FF00000000U)
        | ((enc[4] << 24) & 0x00000000FF000000U)
        | ((enc[5] << 16) & 0x0000000000FF0000U)
        | ((enc[6] <<  8) & 0x000000000000FF00U)
        | (enc[7]        & 0x00000000000000FFU);
    return n;
}

struct NSQMessage *nsq_decode_message(const char *data, size_t data_length)
{
    struct NSQMessage *msg;
    msg = (struct NSQMessage *)malloc(sizeof(struct NSQMessage));
    
    msg->timestamp = decodeInt64((unsigned char *)data);
    msg->attempts = decodeInt32((unsigned char *)data + 8);
    memcpy(&msg->id, (unsigned char *)data+12, 37);
    msg->body_length = decodeInt32((unsigned char *)data + 49);
    msg->body = (char *)malloc(msg->body_length);
    memcpy(msg->body, data+53, msg->body_length);

    return msg;
}

void free_nsq_message(struct NSQMessage *msg)
{
    if (msg) {
        free(msg->body);
        free(msg);
    }
}
