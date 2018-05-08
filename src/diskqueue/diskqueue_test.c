#include "diskqueue.h"

struct NSQMessage {
    int64_t timestamp;
    uint16_t attempts;
    char id[16+1];
    size_t body_length;
    char *body;
};

uint64_t ntoh64(const uint8_t *data) {
    return (uint64_t)(data[7]) | (uint64_t)(data[6])<<8 |
        (uint64_t)(data[5])<<16 | (uint64_t)(data[4])<<24 |
        (uint64_t)(data[3])<<32 | (uint64_t)(data[2])<<40 |
        (uint64_t)(data[1])<<48 | (uint64_t)(data[0])<<56;
}

struct NSQMessage *nsq_decode_message(const char *data, size_t data_length)
{
    struct NSQMessage *msg;
    size_t body_length;

    msg = (struct NSQMessage *)malloc(sizeof(struct NSQMessage));
    msg->timestamp = (int64_t)ntoh64((uint8_t *)data);
    msg->attempts = ntohs(*(uint16_t *)(data+8));
    memcpy(&msg->id, data+10, 16);
    body_length = data_length - 26;
    msg->body = (char *)malloc(body_length);
    memcpy(msg->body, data+26, body_length);
    msg->body_length = body_length;

    return msg;
}

int main() {
    diskqueue *d = New("name", "/Users/orz/Workspace/nsqd-data-path", 100, 1, 200, 100);
    if(d == NULL) {
        printf("failed");
        return 0;
    }


    for(int i = 0; i < 100; ++i) {
        putData(d, "hello", 5);
    }

    for(int j = 0; j < 100; ++j) {
        qchunk *chunk = readData(d);
        if(chunk != NULL) {
            printf("%s\n", chunk->data);
        } else {
            printf("error\n");
        }
    }


}
