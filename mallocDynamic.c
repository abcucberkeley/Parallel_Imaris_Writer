#include "mallocDynamic.h"
#include <stdlib.h>
#include <stdio.h>

void* mallocDynamic(uint64_t x, uint64_t bits){
    switch(bits){
    case 8:
        return malloc(x*sizeof(uint8_t));
    case 16:
        return malloc(x*sizeof(uint16_t));
    case 32:
        return malloc(x*sizeof(float));
    case 64:
        return malloc(x*sizeof(double));
    default:
        printf("Image is not 8/16 bit, single, or double. Using single.");
        return malloc(x*sizeof(float));
    }
}
