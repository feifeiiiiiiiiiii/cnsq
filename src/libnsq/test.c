#include "nsq.h"

#ifdef DEBUG
#define _DEBUG(...) fprintf(stdout, __VA_ARGS__)
#else
#define _DEBUG(...) do {;} while (0)
#endif

static void message_handler(struct NSQReader *rdr, struct NSQDConnection *conn, struct NSQMessage *msg, void *ctx)
{
    _DEBUG("%s: %lld, %d, %s, %d, %.*s\n", __FUNCTION__, msg->timestamp, msg->attempts, msg->id,
        msg->body_length, msg->body_length, msg->body);
    int ret = 0;

    buffer_reset(conn->command_buf);

    if(ret < 0){
        //nsq_requeue(conn->command_buf, msg->id, 100);
    }else{
        //nsq_finish(conn->command_buf, msg->id);
    }
    //buffered_socket_write_buffer(conn->bs, conn->command_buf);

    //buffer_reset(conn->command_buf);
    //nsq_ready(conn->command_buf, rdr->max_in_flight);
    //buffered_socket_write_buffer(conn->bs, conn->command_buf);

    free_nsq_message(msg);
}

int main(int argc, char **argv)
{
    struct NSQReader *rdr;
    struct ev_loop *loop;
    void *ctx = NULL; //(void *)(new TestNsqMsgContext());

    loop = ev_default_loop(0);
    rdr = new_nsq_reader(loop, "topic", "ch", (void *)ctx,
        NULL, NULL, NULL, message_handler);
#ifdef NSQD_STANDALONE
    nsq_reader_connect_to_nsqd(rdr, "127.0.0.1", 6379);
    //nsq_reader_connect_to_nsqd(rdr, "127.0.0.1", 14150);
#else
    nsq_reader_add_nsqlookupd_endpoint(rdr, "127.0.0.1", 4161);
#endif
    nsq_run(loop);

    return 0;
}
