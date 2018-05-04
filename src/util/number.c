#include "number.h"

int encodeInt32(u_int32_t value, unsigned char *enc) {
    enc[0] = value&0xFF;
    enc[1] = (value>>8)&0xFF;
    enc[2] = (value>>16)&0xFF;
    enc[3] = (value>>24)&0xFF;
    return 4;
}

uint32_t decodeInt32(unsigned char *enc) {
    return enc[0]|(enc[1]<<8)|(enc[2]<<16)|(enc[3]<<24);
}

void encodeInt64(int64_t value, unsigned char *enc) {
    for(int i = 0; i < 8; i++) enc[i] = value >> (8-1-i)*8;
    return;
}

int64_t decodeInt64(unsigned char *enc) {
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

#ifdef FALSE
int main() {
    unsigned char buf[32];

    int len = encodeInt32(1024, buf);
    printf("aaa %d %d\n", buf[3], decodeInt32(buf));

    unsigned char buf2[32];

    encodeInt64(1111102411111222, buf2);

    printf("%d %lld\n", len, decodeInt64(buf2));
    return 0;
}
#endif