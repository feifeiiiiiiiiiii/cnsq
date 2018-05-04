#include "common.h"
#include "../util/uuid.h"
#include "../util/number.h"

uint64_t ntoh64(const uint8_t *data) {
    return (uint64_t)(data[7]) | (uint64_t)(data[6])<<8 |
        (uint64_t)(data[5])<<16 | (uint64_t)(data[4])<<24 |
        (uint64_t)(data[3])<<32 | (uint64_t)(data[2])<<40 |
        (uint64_t)(data[1])<<48 | (uint64_t)(data[0])<<56;
}

NSQMessage *newMessage(const char *data, uint32_t data_len) {
    NSQMessage *msg;
    msg = (NSQMessage *)s_malloc(sizeof(NSQMessage));
    msg->attempts = 0;
    msg->body_length = data_len;
    msg->body = (char *)s_malloc(data_len);
    memcpy(msg->body, data, data_len);
    uuid4_generate(msg->id);
    return msg;
}


NSQMessage *nsq_decode_message(const char *data, uint32_t data_length)
{
    NSQMessage *msg;
    size_t body_length;

    msg = (NSQMessage *)s_malloc(sizeof(NSQMessage));
    msg->timestamp = (int64_t)ntoh64((uint8_t *)data);
    msg->attempts = ntohs(*(uint16_t *)(data+8));
    memcpy(&msg->id, data+10, 16);
    body_length = data_length - 26;
    msg->body = (char *)s_malloc(body_length);
    memcpy(msg->body, data+26, body_length);
    msg->body_length = body_length;

    return msg;
}

void *nsq_encode_message(NSQMessage *msg, uint32_t *dataLen) {
    // timestamp(8) + attempts(4) + id(37) + dataLen(4) + body(dataLen)
    *dataLen = MSG_HEADER_LEN + msg->body_length;
    char *buf = malloc(*dataLen + 1);
    unsigned char tmp[32];

    encodeInt64(msg->timestamp, tmp);
    memcpy(buf, tmp, 8);

    encodeInt32(msg->attempts, tmp);
    memcpy(buf + 8, tmp, 4);

    memcpy(buf + 12, msg->id, 37);
    
    encodeInt32(msg->body_length, tmp);
    memcpy(buf + 49, tmp, 4);

    memcpy(buf + 53, msg->body, msg->body_length);

    return buf;
}

void free_nsq_message(NSQMessage *msg)
{
    if (msg) {
        free(msg->body);
        free(msg);
    }
}
