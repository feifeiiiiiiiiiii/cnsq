#include "diskqueue.h"
#include <assert.h>

int main() {
    diskqueue *dq = New("topic", "/tmp", 1024 * 1024, 1, 1024, 10);
    uint32_t len;
 
    for(int i =0; i < 10000; i++) {
    //putData(dq, "hello world", strlen("hello world"));
    }
    
    
    for(int i = 0; i < 10000; ++i) {
        char *data = readData(dq, &len);
        if(data == NULL) {
            printf("data is empty");
        } else {
            printf("%s\n", data);
        }
    }
}
