#ifndef PARALLELREADZARR_H
#define PARALLELREADZARR_H
#include <stdint.h>

void setValuesFromJSON(char* fileName,uint64_t *chunkXSize,uint64_t *chunkYSize,uint64_t *chunkZSize,char* dtype, char* order,uint64_t *shapeX,uint64_t *shapeY,uint64_t *shapeZ, char** cname);

uint64_t dTypeToBits(char* dtype);

void* readZarrParallelWrapper(char* folderName,uint8_t crop, uint64_t startX, uint64_t startY, uint64_t startZ, uint64_t endX, uint64_t endY,uint64_t endZ);

//void** readZarrParallelWrapper2(char* folderName,uint8_t crop, uint64_t startX, uint64_t startY, uint64_t startZ, uint64_t endX, uint64_t endY,uint64_t endZ);

#endif // PARALLELREADZARR_H
