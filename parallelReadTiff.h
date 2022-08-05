#ifndef PARALLELREADTIFF_H
#define PARALLELREADTIFF_H
#include "tiffio.h"
#include <stdio.h>
#include <stdint.h>
#include "omp.h"

void DummyHandler(const char* module, const char* fmt, va_list ap);

void readTiffParallel(uint64_t x, uint64_t y, uint64_t z, char* fileName, void* tiff, uint64_t bits, uint64_t startSlice, uint64_t stripSize);

void* readTiffParallelWrapper(char* fileName);

uint64_t* getImageSize(char* fileName);

uint64_t getDataType(char* fileName);

#endif // PARALLELREADTIFF_H
