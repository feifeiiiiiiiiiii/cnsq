#ifndef NUMBER_H
#define NUMBER_H

#include <stdio.h>
#include <inttypes.h>

int encodeInt32(uint32_t value, unsigned char *enc);
uint32_t decodeInt32(unsigned char *enc);
void encodeInt64(int64_t value, unsigned char *enc);
int64_t decodeInt64(unsigned char *enc);

#endif