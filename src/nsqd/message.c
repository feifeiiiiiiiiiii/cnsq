#include "common.h"
#include "../util/uuid.h"

uint64_t ntoh64(const uint8_t *data) {
    return (uint64_t)(data[7]) | (uint64_t)(data[6])<<8 |
        (uint64_t)(data[5])<<16 | (uint64_t)(data[4])<<24 |
        (uint64_t)(data[3])<<32 | (uint64_t)(data[2])<<40 |
        (uint64_t)(data[1])<<48 | (uint64_t)(data[0])<<56;
}

NSQMessage *newMessage(const char *data, size_t data_len) {
    NSQMessage *msg;
    msg = (NSQMessage *)s_malloc(sizeof(NSQMessage));
    msg->attempts = 0;
    msg->body_length = data_len;
    msg->body = (char *)s_malloc(data_len);
    memcpy(msg->body, data, data_len);
    uuid4_generate(msg->id);
    return msg;
}


NSQMessage *nsq_decode_message(const char *data, size_t data_length)
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

void free_nsq_message(NSQMessage *msg)
{
    if (msg) {
        free(msg->body);
        free(msg);
    }
}
