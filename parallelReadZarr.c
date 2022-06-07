#define _GNU_SOURCE
#include "parallelReadZarr.h"
#include "mallocDynamic.h"
#include <stdio.h>
#include <dirent.h>
#include <blosc2.h>
#include <cjson/cJSON.h>
#include <omp.h>
#include <stdlib.h>
#include <stdio.h>
//#include "numpy/arrayobject.h"

//mex -v COPTIMFLAGS="-O3 -fwrapv -DNDEBUG" CFLAGS='$CFLAGS -O3 -fopenmp' LDFLAGS='$LDFLAGS -O3 -fopenmp' '-I/global/home/groups/software/sl-7.x86_64/modules/cBlosc/2.0.4/include/' '-I/global/home/groups/software/sl-7.x86_64/modules/cJSON/1.7.15/include/' '-L/global/home/groups/software/sl-7.x86_64/modules/cBlosc/2.0.4/lib64' -lblosc2 -L/global/home/groups/software/sl-7.x86_64/modules/cJSON/1.7.15/lib64' -lcjson zarrMex.c

const char fileSep =
#ifdef _WIN32
    '\\';
#else
    '/';
#endif

struct chunkInfo{
    char** chunkNames;
    int64_t numChunks;
};

struct chunkAxisVals{
    uint64_t x;
    uint64_t y;
    uint64_t z;
};

struct chunkAxisVals getChunkAxisVals(char* fileName){
    struct chunkAxisVals cAV;
    char* ptr;
    cAV.x = strtol(fileName, &ptr, 10);
    ptr++;
    cAV.y = strtol(ptr, &ptr, 10);
    ptr++;
    cAV.z = strtol(ptr, &ptr, 10);
    return cAV;
}

struct chunkInfo getChunkInfo(char* folderName, uint64_t startX, uint64_t startY, uint64_t startZ, uint64_t endX, uint64_t endY,uint64_t endZ,uint64_t chunkXSize,uint64_t chunkYSize,uint64_t chunkZSize){
    //int file_count = 0;
    //DIR * dirp;
    //struct dirent * entry;
    struct chunkInfo cI;
    cI.numChunks = 0;
    cI.chunkNames = NULL;

    //dirp = opendir(folderName);
    //if(!dirp){
    //    printf("Failed to open dir\n");
    //    return cI;
    //}
    uint64_t xStartAligned = startX-(startX%chunkXSize);
    uint64_t yStartAligned = startY-(startY%chunkYSize);
    uint64_t zStartAligned = startZ-(startX%chunkZSize);
    uint64_t xStartChunk = (xStartAligned/chunkXSize);
    uint64_t yStartChunk = (yStartAligned/chunkYSize);
    uint64_t zStartChunk = (zStartAligned/chunkZSize);

    uint64_t xEndAligned = endX;
    uint64_t yEndAligned = endY;
    uint64_t zEndAligned = endZ;

    if(xEndAligned%chunkXSize) xEndAligned = endX-(endX%chunkXSize)+chunkXSize;
    if(yEndAligned%chunkYSize) yEndAligned = endY-(endY%chunkYSize)+chunkYSize;
    if(zEndAligned%chunkZSize) zEndAligned = endZ-(endZ%chunkZSize)+chunkZSize;
    uint64_t xEndChunk = (xEndAligned/chunkXSize);
    uint64_t yEndChunk = (yEndAligned/chunkYSize);
    uint64_t zEndChunk = (zEndAligned/chunkZSize);

    uint64_t xChunks = (xEndChunk-xStartChunk);
    uint64_t yChunks = (yEndChunk-yStartChunk);
    uint64_t zChunks = (zEndChunk-zStartChunk);

    uint64_t file_count = xChunks*yChunks*zChunks;
    //uint64_t file_count = (xEndChunk)*(yEndChunk)*(zEndChunk);
    /*
    while ((entry = readdir(dirp)) != NULL) {
        if (entry->d_name[0] != '.') {
            struct chunkAxisVals cAV = getChunkAxisVals(entry->d_name);
            if((cAV.x+1)*chunkXSize < startX || (cAV.y+1)*chunkYSize < startY || (cAV.z+1)*chunkZSize < startZ) continue;
            if((cAV.x)*chunkXSize >= endX || (cAV.y)*chunkYSize >= endY || (cAV.z)*chunkZSize >= endZ) continue;
            file_count++;
        }
    }
    */
    //printf("File Count: %d\n",file_count);
    //rewinddir(dirp);
    // Change to not save names later
    //printf("%d\n",file_count);
    //mexErrMsgIdAndTxt("zarr:inputError","Testing\n");
    char** chunkNames = malloc(file_count*sizeof(char*));
    //#pragma omp parallel for collapse(3)
    for(uint64_t x = xStartChunk; x < xEndChunk; x++){
        for(uint64_t y = yStartChunk; y < yEndChunk; y++){
            for(uint64_t z = zStartChunk; z < zEndChunk; z++){
                uint64_t currFile = (z-zStartChunk)+((y-yStartChunk)*zChunks)+((x-xStartChunk)*yChunks*zChunks);
                //chunkNames[currFile] = (char*)malloc(34);
                asprintf(&chunkNames[currFile],"%llu.%llu.%llu",x,y,z);
                //mexErrMsgIdAndTxt("zarr:inputError","Testing\n");
                //printf("currFile: %d\n",currFile);
                //printf("Chunk: %s\n",chunkNames[currFile]);
                //fflush(stdout);
            }
        }
        //mexErrMsgIdAndTxt("zarr:inputError","Testing\n");
    }
    //printf("here1\n");
    //fflush(stdout);
    //mexErrMsgIdAndTxt("zarr:inputError","Testing\n");
    //printf("here2\n");
    //fflush(stdout);

    /*
    int currDir = 0;
    while ((entry = readdir(dirp)) != NULL) {
        if (entry->d_name[0] != '.') {
            struct chunkAxisVals cAV = getChunkAxisVals(entry->d_name);
            if((cAV.x+1)*chunkXSize < startX || (cAV.y+1)*chunkYSize < startY || (cAV.z+1)*chunkZSize < startZ) continue;
            if((cAV.x)*chunkXSize >= endX || (cAV.y)*chunkYSize >= endY || (cAV.z)*chunkZSize >= endZ) continue;
            chunkNames[currDir] = malloc(strlen(entry->d_name)+1);
            strcpy(chunkNames[currDir],entry->d_name);
            currDir++;
        }
    }

    closedir(dirp);
     */
    cI.chunkNames = chunkNames;
    cI.numChunks = file_count;
    return cI;
}

void setChunkShapeFromJSON(cJSON *json, uint64_t *x, uint64_t *y, uint64_t *z){
    *x = json->child->valueint;
    *y = json->child->next->valueint;
    *z = json->child->next->next->valueint;
}

void setDTypeFromJSON(cJSON *json, char* dtype){
    dtype[0] = json->valuestring[0];
    dtype[1] = json->valuestring[1];
    dtype[2] = json->valuestring[2];
    dtype[3] = json->valuestring[3];
}

void setOrderFromJSON(cJSON *json, char* order){
    *order = json->valuestring[0];
}

void setShapeFromJSON(cJSON *json, uint64_t *x, uint64_t *y, uint64_t *z){
    *x = json->child->valueint;
    *y = json->child->next->valueint;
    *z = json->child->next->next->valueint;
}

void setValuesFromJSON(char* fileName,uint64_t *chunkXSize,uint64_t *chunkYSize,uint64_t *chunkZSize,char* dtype, char* order,uint64_t *shapeX,uint64_t *shapeY,uint64_t *shapeZ){

    char* zArray = ".zarray";
    char* fnFull = (char*)malloc(strlen(fileName)+9);
    fnFull[0] = '\0';
    char fileSepS[2];
    fileSepS[0] = fileSep;
    fileSepS[1] = '\0';

    strcat(fnFull,fileName);
    strcat(fnFull,fileSepS);
    strcat(fnFull,zArray);

    FILE *fileptr = fopen(fnFull, "rb");
    if(!fileptr) //mexErrMsgIdAndTxt("zarr:inputError","Failed to open JSON File: %s\n",fnFull);
    free(fnFull);

    fseek(fileptr, 0, SEEK_END);
    long filelen = ftell(fileptr);
    rewind(fileptr);
    char* buffer = (char *)malloc((filelen));
    fread(buffer, filelen, 1, fileptr);
    fclose(fileptr);
    cJSON *json = cJSON_ParseWithLength(buffer,filelen);
    uint8_t flags[4] = {0,0,0,0};

    while(!(flags[0] && flags[1] && flags[2] && flags[3])){
        if(!json->string){
            json = json->child;
            continue;
        }
        else if(!strcmp(json->string,"chunks")){
            setChunkShapeFromJSON(json, chunkXSize,chunkYSize,chunkZSize);
            flags[0] = 1;
        }
        else if(!strcmp(json->string,"dtype")){
            setDTypeFromJSON(json, dtype);
            flags[1] = 1;
        }
        else if(!strcmp(json->string,"order")){
            setOrderFromJSON(json, order);
            flags[2] = 1;
        }
        else if(!strcmp(json->string,"shape")){
            setShapeFromJSON(json, shapeX,shapeY,shapeZ);
            flags[3] = 1;
        }
        json = json->next;
    }
    cJSON_Delete(json);
}

uint64_t dTypeToBits(char* dtype){

if(dtype[1] == 'u' && dtype[2] == '1'){
    return 8;
}
else if(dtype[1] == 'u' && dtype[2] == '2'){
    return 16;
}
else if(dtype[1] == 'f' && dtype[2] == '4'){
    return 32;
}
else if(dtype[1] == 'f' && dtype[2] == '8'){
    return 64;
}
else{
    return 0;
}

}

void readZarrParallel(void* zarr, char* folderName,uint64_t startX, uint64_t startY, uint64_t startZ, uint64_t endX, uint64_t endY,uint64_t endZ,uint64_t chunkXSize,uint64_t chunkYSize,uint64_t chunkZSize,uint64_t shapeX,uint64_t shapeY,uint64_t shapeZ, uint64_t bits, char order){
    char fileSepS[2];
    fileSepS[0] = fileSep;
    fileSepS[1] = '\0';

    /* Initialize the Blosc compressor */
    int32_t numWorkers = omp_get_max_threads();
    blosc_init();
    blosc_set_nthreads(numWorkers);

    struct chunkInfo cI = getChunkInfo(folderName, startX, startY, startZ, endX, endY, endZ,chunkXSize,chunkYSize,chunkZSize);
    //if(!cI.chunkNames) mexErrMsgIdAndTxt("zarr:inputError","File \"%s\" cannot be opened",folderName);


    //mexErrMsgIdAndTxt("zarr:inputError","Testing\n");

    //mexErrMsgIdAndTxt("zarr:inputError","Testing");

    int32_t batchSize = (cI.numChunks-1)/numWorkers+1;
    uint64_t s = chunkXSize*chunkYSize*chunkZSize;
    int32_t w;
    int err = 0;
    char errString[10000];
    #pragma omp parallel for
    for(w = 0; w < numWorkers; w++){
        void* bufferDest = mallocDynamic(s,bits);
        uint64_t lastFileLen = 0;
        char *buffer = NULL;
        for(int64_t f = w*batchSize; f < (w+1)*batchSize; f++){
            if(f>=cI.numChunks || err) break;
            struct chunkAxisVals cAV = getChunkAxisVals(cI.chunkNames[f]);

            //malloc +2 for null term and filesep
            char *fileName = malloc(strlen(folderName)+strlen(cI.chunkNames[f])+2);
            fileName[0] = '\0';
            strcat(fileName,folderName);
            strcat(fileName,fileSepS);
            strcat(fileName,cI.chunkNames[f]);

            FILE *fileptr = fopen(fileName, "rb");
            if(!fileptr){
                #pragma omp critical
                {
                    err = 1;
                    sprintf(errString,"Could not open file: %s\n",fileName);
                }
                break;
            }
            free(fileName);

            fseek(fileptr, 0, SEEK_END);
            long filelen = ftell(fileptr);
            rewind(fileptr);
            if(lastFileLen < filelen){
                //char *buffer = (char *)malloc((filelen));
                free(buffer);
                buffer = (char*) malloc(filelen);
                lastFileLen = filelen;
            }
            fread(buffer, filelen, 1, fileptr);
            fclose(fileptr);

            // Decompress
            int dsize = -1;
            switch(bits){
                case 8:
                    dsize = blosc2_decompress(buffer, filelen,bufferDest, s*sizeof(uint8_t));
                    break;
                case 16:
                    dsize = blosc2_decompress(buffer, filelen,bufferDest, s*sizeof(uint16_t));
                    break;
                case 32:
                    dsize = blosc2_decompress(buffer, filelen,bufferDest, s*sizeof(float));
                    break;
                case 64:
                    dsize = blosc2_decompress(buffer, filelen,bufferDest, s*sizeof(double));
                    break;
            }

            if(dsize < 0){
                #pragma omp critical
                {
                    err = 1;
                    sprintf(errString,"Decompression error. Error code: %d\n",dsize);
                }
                break;
            }

            //printf("ChunkName: %s\n",cI.chunkNames[f]);
            //printf("w: %d b: %d\n",w,f);
            if(order == 'F'){
                //if((cAV.x+1)*chunkXSize > endX || (cAV.y+1)*chunkYSize > endY || (cAV.z+1)*chunkZSize > endZ) break;
                //memcpy((uint16_t*)(zarr+(((cAV.x*chunkXSize)-startX)+(((cAV.y*chunkYSize)-startY)*shapeX)+(((cAV.z*chunkZSize)-startZ)*shapeX*shapeY))),(uint16_t*)bufferDest,s);

                for(int64_t z = cAV.z*chunkZSize; z < (cAV.z+1)*chunkZSize; z++){
                    if(z>=endZ) break;
                    else if(z<startZ) continue;
                    for(int64_t y = cAV.y*chunkYSize; y < (cAV.y+1)*chunkYSize; y++){
                        if(y>=endY) break;
                        else if(y<startY) continue;
                        //for(int64_t x = cAV.x*chunkXSize; x < (cAV.x+1)*chunkXSize; x++){
                            //if(x>=endX) break;
                            //if((cAV.x+1)*chunkXSize>=endX) break;
                            //else if(x<startX) continue;
                            //printf("x: %d y: %d z: %d\n",cAV.x,cAV.y,cAV.z);
                            if(((cAV.x*chunkXSize) < startX && ((cAV.x+1)*chunkXSize) > startX) || (cAV.x+1)*chunkXSize>=endX){
                                if(((cAV.x*chunkXSize) < startX && ((cAV.x+1)*chunkXSize) > startX) && (cAV.x+1)*chunkXSize>=endX){
                                    memcpy((uint16_t*)zarr+((cAV.x*chunkXSize)+((y-startY)*shapeX)+((z-startZ)*shapeX*shapeY)),(uint16_t*)bufferDest+((startX%chunkXSize)+((y%chunkYSize)*chunkXSize)+((z%chunkZSize)*chunkXSize*chunkYSize)),((endX%(cAV.x*chunkXSize))-(startX%chunkXSize))*(bits/8));
                                }
                                else if((cAV.x+1)*chunkXSize>=endX){
                                    memcpy((uint16_t*)zarr+(((cAV.x*chunkXSize)-startX)+((y-startY)*shapeX)+((z-startZ)*shapeX*shapeY)),(uint16_t*)bufferDest+(((y%chunkYSize)*chunkXSize)+((z%chunkZSize)*chunkXSize*chunkYSize)),(endX%(cAV.x*chunkXSize))*(bits/8));
                                }
                                else if((cAV.x*chunkXSize) < startX && ((cAV.x+1)*chunkXSize) > startX){
                                    memcpy((uint16_t*)zarr+((cAV.x*chunkXSize)+((y-startY)*shapeX)+((z-startZ)*shapeX*shapeY)),(uint16_t*)bufferDest+((startX%chunkXSize)+((y%chunkYSize)*chunkXSize)+((z%chunkZSize)*chunkXSize*chunkYSize)),((startX%chunkXSize))*(bits/8));
                                }
                            }
                            else{
                                switch(bits){
                                    case 8:
                                        //((uint8_t*)zarr)[(x-startX)+((y-startY)*shapeX)+((z-startZ)*shapeX*shapeY)] = ((uint8_t*)bufferDest)[(x%chunkXSize)+((y%chunkYSize)*chunkXSize)+((z%chunkZSize)*chunkXSize*chunkYSize)];
                                        break;
                                    case 16:
                                        //((uint16_t*)zarr)[(x-startX)+((y-startY)*shapeX)+((z-startZ)*shapeX*shapeY)] = ((uint16_t*)bufferDest)[(x%chunkXSize)+((y%chunkYSize)*chunkXSize)+((z%chunkZSize)*chunkXSize*chunkYSize)];
                                        //memcpy(zarr+((((cAV.x*chunkXSize)-startX)+((y-startY)*shapeX)+((z-startZ)*shapeX*shapeY))*2),bufferDest+((((y%chunkYSize)*chunkXSize)+((z%chunkZSize)*chunkXSize*chunkYSize))*2),chunkXSize*2);
                                        memcpy((uint16_t*)zarr+(((cAV.x*chunkXSize)-startX)+((y-startY)*shapeX)+((z-startZ)*shapeX*shapeY)),(uint16_t*)bufferDest+(((y%chunkYSize)*chunkXSize)+((z%chunkZSize)*chunkXSize*chunkYSize)),chunkXSize*(bits/8));
                                        break;
                                    case 32:
                                        //((float*)zarr)[(x-startX)+((y-startY)*shapeX)+((z-startZ)*shapeX*shapeY)] = ((float*)bufferDest)[(x%chunkXSize)+((y%chunkYSize)*chunkXSize)+((z%chunkZSize)*chunkXSize*chunkYSize)];
                                        break;
                                    case 64:
                                        //((double*)zarr)[(x-startX)+((y-startY)*shapeX)+((z-startZ)*shapeX*shapeY)] = ((double*)bufferDest)[(x%chunkXSize)+((y%chunkYSize)*chunkXSize)+((z%chunkZSize)*chunkXSize*chunkYSize)];
                                        break;
                                }
                            }
                        //}
                    }
                }

            }
            else if (order == 'C'){
                for(int64_t x = cAV.x*chunkXSize; x < (cAV.x+1)*chunkXSize; x++){
                    if(x>=endX) break;
                    else if(x<startX) continue;
                    for(int64_t y = cAV.y*chunkYSize; y < (cAV.y+1)*chunkYSize; y++){
                        if(y>=endY) break;
                        else if(y<startY) continue;
                        for(int64_t z = cAV.z*chunkZSize; z < (cAV.z+1)*chunkZSize; z++){
                            if(z>=endZ) break;
                            else if(z<startZ) continue;
                            switch(bits){
                                case 8:
                                    ((uint8_t*)zarr)[(x-startX)+((y-startY)*shapeX)+((z-startZ)*shapeX*shapeY)] = ((uint8_t*)bufferDest)[(z%chunkZSize)+((y%chunkYSize)*chunkZSize)+((x%chunkXSize)*chunkZSize*chunkYSize)];
                                    break;
                                case 16:
                                    ((uint16_t*)zarr)[(x-startX)+((y-startY)*shapeX)+((z-startZ)*shapeX*shapeY)] = ((uint16_t*)bufferDest)[(z%chunkZSize)+((y%chunkYSize)*chunkZSize)+((x%chunkXSize)*chunkZSize*chunkYSize)];
                                    break;
                                case 32:
                                    ((float*)zarr)[(x-startX)+((y-startY)*shapeX)+((z-startZ)*shapeX*shapeY)] = ((float*)bufferDest)[(z%chunkZSize)+((y%chunkYSize)*chunkZSize)+((x%chunkXSize)*chunkZSize*chunkYSize)];
                                    break;
                                case 64:
                                    ((double*)zarr)[(x-startX)+((y-startY)*shapeX)+((z-startZ)*shapeX*shapeY)] = ((double*)bufferDest)[(z%chunkZSize)+((y%chunkYSize)*chunkZSize)+((x%chunkXSize)*chunkZSize*chunkYSize)];
                                    break;
                            }
                        }
                    }
                }

            }

        }
        free(bufferDest);
        free(buffer);
    }
    #pragma omp parallel for
    for(int i = 0; i < cI.numChunks; i++){
        free(cI.chunkNames[i]);
    }
    free(cI.chunkNames);

    /* After using it, destroy the Blosc environment */
    blosc_destroy();

    //if(err) mexErrMsgIdAndTxt("zarr:threadError",errString);
}

void* readZarrParallelWrapper(char* folderName,uint8_t crop, uint64_t startX, uint64_t startY, uint64_t startZ, uint64_t endX, uint64_t endY,uint64_t endZ){
    //uint64_t startX = 899;
    //uint64_t startY = 19899;
    //uint64_t startZ = 0;
    //uint64_t endX = 3700;
    //uint64_t endY = 26000;
    //uint64_t endZ = 3000;

    /*
    if(!nrhs) mexErrMsgIdAndTxt("zarr:inputError","This functions requires at least one argument");
    else if(nrhs == 2){
        if(mxGetN(prhs[1]) != 6) mexErrMsgIdAndTxt("zarr:inputError","Input range is not 6");
        startX = (uint64_t)*(mxGetPr(prhs[1]))-1;
        startY = (uint64_t)*((mxGetPr(prhs[1])+1))-1;
        startZ = (uint64_t)*((mxGetPr(prhs[1])+2))-1;
        endX = (uint64_t)*((mxGetPr(prhs[1])+3));
        endY = (uint64_t)*((mxGetPr(prhs[1])+4));
        endZ = (uint64_t)*((mxGetPr(prhs[1])+5));

        if(startX+1 < 1 || startY+1 < 1 || startZ+1 < 1) mexErrMsgIdAndTxt("zarr:inputError","Lower bounds must be at least 1");
    }
    else if (nrhs > 2) mexErrMsgIdAndTxt("zarr:inputError","Number of input arguments must be 1 or 2");
    if(!mxIsChar(prhs[0])) mexErrMsgIdAndTxt("zarr:inputError","The first argument must be a string");
    char* folderName = mxArrayToString(prhs[0]);
    */

    uint64_t shapeX = 0;
    uint64_t shapeY = 0;
    uint64_t shapeZ = 0;
    uint64_t chunkXSize = 0;
    uint64_t chunkYSize = 0;
    uint64_t chunkZSize = 0;
    char dtype[4];
    char order;
    //char* folderName = "Y:/Data/20220127_Korra_ExM_BrainB1/Data/matlab_stitch_xcorr_feather_skewed_zarr/matlab_decon/DSR/Puncta_Removed/Scan_Iter_0000_CamB_ch0_CAM1_stack0000_488nm_0000000msec_0028239309msecAbs.zarr";
    setValuesFromJSON(folderName,&chunkXSize,&chunkYSize,&chunkZSize,dtype,&order,&shapeX,&shapeY,&shapeZ);


    if(!crop){
        startX = 0;
        startY = 0;
        startZ = 0;
        endX = shapeX;
        endY = shapeY;
        endZ = shapeZ;
    }
    else{
        startX--;
        startY--;
        startZ--;
    }

    if(endX > shapeX || endY > shapeY || endZ > shapeZ){
        printf("Upper bound is invalid\n");
        return NULL;
    }
    uint64_t dim[3];
    shapeX = endX-startX;
    shapeY = endY-startY;
    shapeZ = endZ-startZ;
    dim[0] = shapeX;
    dim[1] = shapeY;
    dim[2] = shapeZ;

    if(dtype[1] == 'u' && dtype[2] == '1'){
        uint64_t bits = 8;
        uint8_t* zarr = (uint8_t*)malloc(sizeof(uint8_t)*shapeX*shapeY*shapeZ);
        readZarrParallel((void*)zarr,folderName,startX,startY,startZ,endX,endY,endZ,chunkXSize,chunkYSize,chunkZSize,shapeX,shapeY,shapeZ,bits,order);
        return (void*)zarr;
    }
    else if(dtype[1] == 'u' && dtype[2] == '2'){
        uint64_t bits = 16;
        uint16_t* zarr = (uint16_t*)malloc(sizeof(uint16_t)*shapeX*shapeY*shapeZ);
        readZarrParallel((void*)zarr,folderName,startX,startY,startZ,endX,endY,endZ,chunkXSize,chunkYSize,chunkZSize,shapeX,shapeY,shapeZ,bits,order);
        return (void*)zarr;
    }
    else if(dtype[1] == 'f' && dtype[2] == '4'){
        uint64_t bits = 32;
        float* zarr = (float*)malloc(sizeof(float)*shapeX*shapeY*shapeZ);
        readZarrParallel((void*)zarr,folderName,startX,startY,startZ,endX,endY,endZ,chunkXSize,chunkYSize,chunkZSize,shapeX,shapeY,shapeZ,bits,order);
        return (void*)zarr;
    }
    else if(dtype[1] == 'f' && dtype[2] == '8'){
        uint64_t bits = 64;
        double* zarr = (double*)malloc(sizeof(double)*shapeX*shapeY*shapeZ);
        readZarrParallel((void*)zarr,folderName,startX,startY,startZ,endX,endY,endZ,chunkXSize,chunkYSize,chunkZSize,shapeX,shapeY,shapeZ,bits,order);
        return (void*)zarr;
    }
    else{
        //mexErrMsgIdAndTxt("tiff:dataTypeError","Data type not suppported");
        return NULL;
    }
}












































void readZarrParallel2(void** zarr, char* folderName,uint64_t startX, uint64_t startY, uint64_t startZ, uint64_t endX, uint64_t endY,uint64_t endZ,uint64_t chunkXSize,uint64_t chunkYSize,uint64_t chunkZSize,uint64_t shapeX,uint64_t shapeY,uint64_t shapeZ, uint64_t bits, char order){
    char fileSepS[2];
    fileSepS[0] = fileSep;
    fileSepS[1] = '\0';

    /* Initialize the Blosc compressor */
    int32_t numWorkers = omp_get_max_threads();
    blosc_init();
    blosc_set_nthreads(numWorkers);

    struct chunkInfo cI = getChunkInfo(folderName, startX, startY, startZ, endX, endY, endZ,chunkXSize,chunkYSize,chunkZSize);
    //if(!cI.chunkNames) mexErrMsgIdAndTxt("zarr:inputError","File \"%s\" cannot be opened",folderName);

    //printf("numChunks: %d, ChunkSizeX: %d ChunkSizeY: %d ChunkSizeZ: %d \n",cI.numChunks,chunkXSize,chunkYSize,chunkZSize);
    //mexErrMsgIdAndTxt("zarr:inputError","TESTING");

    int32_t batchSize = (cI.numChunks-1)/numWorkers+1;
    uint64_t s = chunkXSize*chunkYSize*chunkZSize;
    int32_t w;
    int err = 0;
    char errString[10000];
    #pragma omp parallel for
    for(w = 0; w < numWorkers; w++){
        //void* bufferDest = mallocDynamic(s,bits);
        uint64_t lastFileLen = 0;
        char *buffer = NULL;
        for(int64_t f = w*batchSize; f < (w+1)*batchSize; f++){
            void* bufferDest = mallocDynamic(s,bits);
            if(f>=cI.numChunks || err) break;
            struct chunkAxisVals cAV = getChunkAxisVals(cI.chunkNames[f]);

            //malloc +2 for null term and filesep
            char *fileName = malloc(strlen(folderName)+strlen(cI.chunkNames[f])+2);
            fileName[0] = '\0';
            strcat(fileName,folderName);
            strcat(fileName,fileSepS);
            strcat(fileName,cI.chunkNames[f]);

            FILE *fileptr = fopen(fileName, "rb");
            if(!fileptr){
                #pragma omp critical
                {
                    err = 1;
                    sprintf(errString,"Could not open file: %s\n",fileName);
                }
                break;
            }
            free(fileName);

            fseek(fileptr, 0, SEEK_END);
            long filelen = ftell(fileptr);
            rewind(fileptr);
            if(lastFileLen < filelen){
                //char *buffer = (char *)malloc((filelen));
                free(buffer);
                buffer = (char*) malloc(filelen);
                lastFileLen = filelen;
            }
            fread(buffer, filelen, 1, fileptr);
            fclose(fileptr);

            // Decompress
            int dsize = -1;
            switch(bits){
                case 8:
                    dsize = blosc2_decompress(buffer, filelen,bufferDest, s*sizeof(uint8_t));
                    break;
                case 16:
                    dsize = blosc2_decompress(buffer, filelen,bufferDest, s*sizeof(uint16_t));
                    break;
                case 32:
                    dsize = blosc2_decompress(buffer, filelen,bufferDest, s*sizeof(float));
                    break;
                case 64:
                    dsize = blosc2_decompress(buffer, filelen,bufferDest, s*sizeof(double));
                    break;
            }

            if(dsize < 0){
                #pragma omp critical
                {
                    err = 1;
                    sprintf(errString,"Decompression error. Error code: %d\n",dsize);
                }
                break;
            }

            //printf("ChunkName: %s\n",cI.chunkNames[f]);
            //printf("w: %d b: %d\n",w,f);
            if(order == 'F'){
                for(int64_t z = cAV.z*chunkZSize; z < (cAV.z+1)*chunkZSize; z++){
                    if(z>=endZ) break;
                    else if(z<startZ) continue;
                    for(int64_t y = cAV.y*chunkYSize; y < (cAV.y+1)*chunkYSize; y++){
                        if(y>=endY) break;
                        else if(y<startY) continue;
                        for(int64_t x = cAV.x*chunkXSize; x < (cAV.x+1)*chunkXSize; x++){
                            if(x>=endX) break;
                            else if(x<startX) continue;
                            switch(bits){
                                case 8:
                                    ((uint8_t*)zarr)[(x-startX)+((y-startY)*shapeX)+((z-startZ)*shapeX*shapeY)] = ((uint8_t*)bufferDest)[(x%chunkXSize)+((y%chunkYSize)*chunkXSize)+((z%chunkZSize)*chunkXSize*chunkYSize)];
                                    break;
                                case 16:
                                    //((uint16_t*)zarr)[(x-startX)+((y-startY)*shapeX)+((z-startZ)*shapeX*shapeY)] = ((uint16_t*)bufferDest)[(x%chunkXSize)+((y%chunkYSize)*chunkXSize)+((z%chunkZSize)*chunkXSize*chunkYSize)];
                                    ((uint16_t**)zarr)[(x-startX)+((y-startY)*shapeX)+((z-startZ)*shapeX*shapeY)] = ((uint16_t*)bufferDest)+((x%chunkXSize)+((y%chunkYSize)*chunkXSize)+((z%chunkZSize)*chunkXSize*chunkYSize));
                                    break;
                                case 32:
                                    ((float*)zarr)[(x-startX)+((y-startY)*shapeX)+((z-startZ)*shapeX*shapeY)] = ((float*)bufferDest)[(x%chunkXSize)+((y%chunkYSize)*chunkXSize)+((z%chunkZSize)*chunkXSize*chunkYSize)];
                                    break;
                                case 64:
                                    ((double*)zarr)[(x-startX)+((y-startY)*shapeX)+((z-startZ)*shapeX*shapeY)] = ((double*)bufferDest)[(x%chunkXSize)+((y%chunkYSize)*chunkXSize)+((z%chunkZSize)*chunkXSize*chunkYSize)];
                                    break;
                            }
                        }
                    }
                }
            }
            else if (order == 'C'){
                for(int64_t x = cAV.x*chunkXSize; x < (cAV.x+1)*chunkXSize; x++){
                    if(x>=endX) break;
                    else if(x<startX) continue;
                    for(int64_t y = cAV.y*chunkYSize; y < (cAV.y+1)*chunkYSize; y++){
                        if(y>=endY) break;
                        else if(y<startY) continue;
                        for(int64_t z = cAV.z*chunkZSize; z < (cAV.z+1)*chunkZSize; z++){
                            if(z>=endZ) break;
                            else if(z<startZ) continue;
                            switch(bits){
                                case 8:
                                    ((uint8_t*)zarr)[(x-startX)+((y-startY)*shapeX)+((z-startZ)*shapeX*shapeY)] = ((uint8_t*)bufferDest)[(z%chunkZSize)+((y%chunkYSize)*chunkZSize)+((x%chunkXSize)*chunkZSize*chunkYSize)];
                                    break;
                                case 16:
                                    ((uint16_t*)zarr)[(x-startX)+((y-startY)*shapeX)+((z-startZ)*shapeX*shapeY)] = ((uint16_t*)bufferDest)[(z%chunkZSize)+((y%chunkYSize)*chunkZSize)+((x%chunkXSize)*chunkZSize*chunkYSize)];
                                    break;
                                case 32:
                                    ((float*)zarr)[(x-startX)+((y-startY)*shapeX)+((z-startZ)*shapeX*shapeY)] = ((float*)bufferDest)[(z%chunkZSize)+((y%chunkYSize)*chunkZSize)+((x%chunkXSize)*chunkZSize*chunkYSize)];
                                    break;
                                case 64:
                                    ((double*)zarr)[(x-startX)+((y-startY)*shapeX)+((z-startZ)*shapeX*shapeY)] = ((double*)bufferDest)[(z%chunkZSize)+((y%chunkYSize)*chunkZSize)+((x%chunkXSize)*chunkZSize*chunkYSize)];
                                    break;
                            }
                        }
                    }
                }
            }
        }
        //free(bufferDest);
        free(buffer);
    }
    #pragma omp parallel for
    for(int i = 0; i < cI.numChunks; i++){
        free(cI.chunkNames[i]);
    }
    free(cI.chunkNames);

    /* After using it, destroy the Blosc environment */
    blosc_destroy();

    //if(err) mexErrMsgIdAndTxt("zarr:threadError",errString);
}

void** readZarrParallelWrapper2(char* folderName,uint8_t crop, uint64_t startX, uint64_t startY, uint64_t startZ, uint64_t endX, uint64_t endY,uint64_t endZ){
    //uint64_t startX = 899;
    //uint64_t startY = 19899;
    //uint64_t startZ = 0;
    //uint64_t endX = 3700;
    //uint64_t endY = 26000;
    //uint64_t endZ = 3000;

    /*
    if(!nrhs) mexErrMsgIdAndTxt("zarr:inputError","This functions requires at least one argument");
    else if(nrhs == 2){
        if(mxGetN(prhs[1]) != 6) mexErrMsgIdAndTxt("zarr:inputError","Input range is not 6");
        startX = (uint64_t)*(mxGetPr(prhs[1]))-1;
        startY = (uint64_t)*((mxGetPr(prhs[1])+1))-1;
        startZ = (uint64_t)*((mxGetPr(prhs[1])+2))-1;
        endX = (uint64_t)*((mxGetPr(prhs[1])+3));
        endY = (uint64_t)*((mxGetPr(prhs[1])+4));
        endZ = (uint64_t)*((mxGetPr(prhs[1])+5));

        if(startX+1 < 1 || startY+1 < 1 || startZ+1 < 1) mexErrMsgIdAndTxt("zarr:inputError","Lower bounds must be at least 1");
    }
    else if (nrhs > 2) mexErrMsgIdAndTxt("zarr:inputError","Number of input arguments must be 1 or 2");
    if(!mxIsChar(prhs[0])) mexErrMsgIdAndTxt("zarr:inputError","The first argument must be a string");
    char* folderName = mxArrayToString(prhs[0]);
    */

    uint64_t shapeX = 0;
    uint64_t shapeY = 0;
    uint64_t shapeZ = 0;
    uint64_t chunkXSize = 0;
    uint64_t chunkYSize = 0;
    uint64_t chunkZSize = 0;
    char dtype[4];
    char order;
    //char* folderName = "Y:/Data/20220127_Korra_ExM_BrainB1/Data/matlab_stitch_xcorr_feather_skewed_zarr/matlab_decon/DSR/Puncta_Removed/Scan_Iter_0000_CamB_ch0_CAM1_stack0000_488nm_0000000msec_0028239309msecAbs.zarr";
    setValuesFromJSON(folderName,&chunkXSize,&chunkYSize,&chunkZSize,dtype,&order,&shapeX,&shapeY,&shapeZ);


    if(!crop){
        startX = 0;
        startY = 0;
        startZ = 0;
        endX = shapeX;
        endY = shapeY;
        endZ = shapeZ;
    }

    if(endX > shapeX || endY > shapeY || endZ > shapeZ){
        printf("Upper bound is invalid\n");
        return NULL;
    }
    uint64_t dim[3];
    shapeX = endX-startX;
    shapeY = endY-startY;
    shapeZ = endZ-startZ;
    dim[0] = shapeX;
    dim[1] = shapeY;
    dim[2] = shapeZ;

    if(dtype[1] == 'u' && dtype[2] == '1'){
        uint64_t bits = 8;
        uint8_t* zarr = (uint8_t*)malloc(sizeof(uint8_t)*shapeX*shapeY*shapeZ);
        readZarrParallel((void*)zarr,folderName,startX,startY,startZ,endX,endY,endZ,chunkXSize,chunkYSize,chunkZSize,shapeX,shapeY,shapeZ,bits,order);
        return (void*)zarr;
    }
    else if(dtype[1] == 'u' && dtype[2] == '2'){
        uint64_t bits = 16;
        uint16_t** zarr = (uint16_t**)malloc(sizeof(uint16_t*)*shapeX*shapeY*shapeZ);
        readZarrParallel2((void**)zarr,folderName,startX,startY,startZ,endX,endY,endZ,chunkXSize,chunkYSize,chunkZSize,shapeX,shapeY,shapeZ,bits,order);
        return (void**)zarr;
    }
    else if(dtype[1] == 'f' && dtype[2] == '4'){
        uint64_t bits = 32;
        float* zarr = (float*)malloc(sizeof(float)*shapeX*shapeY*shapeZ);
        readZarrParallel((void*)zarr,folderName,startX,startY,startZ,endX,endY,endZ,chunkXSize,chunkYSize,chunkZSize,shapeX,shapeY,shapeZ,bits,order);
        return (void*)zarr;
    }
    else if(dtype[1] == 'f' && dtype[2] == '8'){
        uint64_t bits = 64;
        double* zarr = (double*)malloc(sizeof(double)*shapeX*shapeY*shapeZ);
        readZarrParallel((void*)zarr,folderName,startX,startY,startZ,endX,endY,endZ,chunkXSize,chunkYSize,chunkZSize,shapeX,shapeY,shapeZ,bits,order);
        return (void*)zarr;
    }
    else{
        //mexErrMsgIdAndTxt("tiff:dataTypeError","Data type not suppported");
        return NULL;
    }
}
