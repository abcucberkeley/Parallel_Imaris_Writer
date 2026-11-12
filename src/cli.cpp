/***************************************************************************
 *   Copyright (c) 2020-present Bitplane AG Zuerich                        *
 *                                                                         *
 *   Licensed under the Apache License, Version 2.0 (the "License");       *
 *   you may not use this file except in compliance with the License.      *
 *   You may obtain a copy of the License at                               *
 *                                                                         *
 *       http://www.apache.org/licenses/LICENSE-2.0                        *
 *                                                                         *
 *   Unless required by applicable law or agreed to in writing, software   *
 *   distributed under the License is distributed on an "AS IS" BASIS,     *
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or imp   *
 *   See the License for the specific language governing permissions and   *
 *   limitations under the License.                                        *
 ***************************************************************************/

// The command line (piw::runCli): the cluster subcommands, else the single-node conversion with
// the original options, messages and exit codes. The conversion itself is piw::convert; the
// table that turns its Status into output and an exit code is cliExitCode below.
#include "helperfunctions.h"        // cpp-tiff's expandTilde(char*), used for the output folder as before
#include "parallelimariswriter.h"
#include "parallel.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <dirent.h>
#include <stdint.h>
#include <sys/stat.h>
#include <exception>
#include <string>
#include <vector>

#ifndef _WIN32
#include <wordexp.h>
// Expand the tilde to the home directory on Mac and Linux
const char* expandTilde(const char* path) {
    if(strchr(path,'~')){
        wordexp_t expPath;
        wordexp(path, &expPath, 0);
        return expPath.we_wordv[0];
    }
    else return path;
}
#endif

namespace piw {
namespace detail {
extern const char* const kNo64BitData;   // src/converter.cpp
}
}

namespace {

typedef struct
{
    unsigned int mImageIndex;
    int mProgress;
} bpCallbackData;

void ProgressCallback(float aProgress, unsigned long long aTotalBytesWritten, void* aUserData)
{
    bpCallbackData* vCallbackData = (bpCallbackData*)aUserData;

    int vProgress = (int)(aProgress * 100);
    if (vProgress - vCallbackData->mProgress < 5) {
        return;
    }

    unsigned int vImageIndex = vCallbackData->mImageIndex;
    if (aTotalBytesWritten < 10 * 1024 * 1024) {
        printf("Progress image %u: %d%% [%llu KB]\n", vImageIndex, vProgress, aTotalBytesWritten / 1024);
    }
    else {
        printf("Progress image %u: %d%% [%llu MB]\n", vImageIndex, vProgress, aTotalBytesWritten / (1024 * 1024));
    }
    vCallbackData->mProgress = vProgress;
}

void MessageCallback(const char* aText, void*)
{
    fputs(aText, stdout);
    if(!strncmp(aText, "Current File: ", 14)) fflush(stdout);
}

// Status -> output and exit code. Kept as the original converter behaved; the cases that print
// and return 0 are the ones A-3 turns into errors.
int cliExitCode(const piw::Status& aStatus)
{
    switch(aStatus.code){
        case piw::Code::Ok:
            return 0;
        case piw::Code::WriteFailed:            // ImarisWriter's message, as CheckErrors printed it
            printf("%s", aStatus.message.c_str());
            return 1;
        case piw::Code::ReadFailed:
            fprintf(stderr, "Error: %s\n", aStatus.message.c_str());
            return 1;
        case piw::Code::Unsupported:
            if(aStatus.message == piw::detail::kNo64BitData){
                printf("%s", aStatus.message.c_str());
                return 0;
            }
            break;
        default:
            break;
    }
    fprintf(stderr, "ERROR: %s\n", aStatus.message.c_str());
    return 1;
}

// strtok_r's view of a comma-separated list: the non-empty items (a trailing comma is fine).
std::vector<std::string> listItems(const std::string& aText)
{
    std::vector<std::string> vItems;
    size_t vStart = 0;
    while(vStart <= aText.size()){
        size_t vEnd = aText.find(',', vStart);
        if(vEnd == std::string::npos) vEnd = aText.size();
        if(vEnd > vStart) vItems.push_back(aText.substr(vStart, vEnd - vStart));
        vStart = vEnd + 1;
    }
    return vItems;
}

bool wholeNumber(const char* aStart, const char* aEnd)
{
    if(aEnd == aStart) return false;
    while(*aEnd == ' ' || *aEnd == '\t') ++aEnd;
    return *aEnd == '\0';
}

// aCount numbers read with strtof, as the original read -v, -b and -B.
bool parseFloats(const char* aText, unsigned aCount, float* aValues)
{
    std::vector<std::string> vItems = listItems(aText);
    if(vItems.size() != aCount) return false;
    for(unsigned i = 0; i < aCount; ++i){
        char* vEnd = NULL;
        aValues[i] = strtof(vItems[i].c_str(), &vEnd);
        if(!wholeNumber(vItems[i].c_str(), vEnd)) return false;
    }
    return true;
}

// -b and -B: strtof, then converted to whole voxels as before.
bool parseFloatSizes(const char* aText, unsigned aCount, uint64_t* aValues)
{
    float vValues[6];
    if(aCount > 6 || !parseFloats(aText, aCount, vValues)) return false;
    for(unsigned i = 0; i < aCount; ++i){
        if(!(vValues[i] >= 0.0f) || vValues[i] >= 18446744073709551616.0f) return false;
        aValues[i] = (uint64_t)vValues[i];
    }
    return true;
}

// -C: strtoull, as before.
bool parseSizes(const char* aText, unsigned aCount, uint64_t* aValues)
{
    std::vector<std::string> vItems = listItems(aText);
    if(vItems.size() != aCount) return false;
    for(unsigned i = 0; i < aCount; ++i){
        char* vEnd = NULL;
        aValues[i] = strtoull(vItems[i].c_str(), &vEnd, 10);
        if(!wholeNumber(vItems[i].c_str(), vEnd)) return false;
    }
    return true;
}

int badValue(char aOption, const char* aText, unsigned aCount)
{
    fprintf(stderr, "ERROR: Could not read -%c \"%s\": expected %u comma-separated numbers.\n", aOption, aText, aCount);
    return 1;
}

// getopt keeps its position between calls: start over so runCli can run more than once.
void resetGetopt()
{
#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__NetBSD__) || defined(__OpenBSD__) || defined(__DragonFly__)
    optreset = 1;
    optind = 1;
#else
    optind = 0;   // glibc, musl and MinGW-w64 reinitialise when optind is 0
#endif
}

int convertToImaris(int argc, char **argv)
{
    int64_t timepoints = -1;
    int64_t channels = -1;
    int option_index = 0;
    const char* vVoxelSizes = NULL;
    const char* folderNames = NULL;
    const char* patterns = NULL;
    const char* outputDirArg = NULL;
    const char* outName = NULL;
    const char* reader = NULL;
    const char* blockSizes = NULL;
    const char* blockProcessString = NULL;
    const char* boundingBoxString = NULL;
    resetGetopt();
    while (( option_index = getopt(argc, argv, ":c:P:t:f:F:o:r:v:n:b:B:C:")) != -1){
        switch (option_index) {
        case 'c':
            channels = atoi(optarg);
            break;
        case 'P':
            patterns = optarg;
            break;
        case 't':
            timepoints = atoi(optarg);
            break;
        case 'f':
            break;   // accepted and not used
        case 'F':
            folderNames = optarg;
            break;
        case 'o':
            outputDirArg = optarg;
            break;
        case 'r':
            reader = optarg;
            break;
        case 'v':
            vVoxelSizes = optarg;
            break;
        case 'n':
            outName = optarg;
            break;
        case 'b':
            blockSizes = optarg;
            break;
        case 'B':
            boundingBoxString = optarg;
            break;
        case 'C':
            blockProcessString = optarg;
            break;
        default:
            printf("Option incorrect\n");
        }
    }

    std::string folderNamesString = folderNames ? folderNames : "";
    // Expand the tilde to the home directory on Mac and Linux (item by item, as std::getline
    // split the list before: no empty item after a trailing comma)
    #ifndef _WIN32
    if(folderNames){
        std::string vExpanded;
        size_t vStart = 0;
        while(vStart < folderNamesString.size()){
            size_t vEnd = folderNamesString.find(',', vStart);
            if(vEnd == std::string::npos) vEnd = folderNamesString.size();
            vExpanded.append(expandTilde(folderNamesString.substr(vStart, vEnd - vStart).c_str()));
            vExpanded.append(",");
            vStart = vEnd + 1;
        }
        if(!vExpanded.empty()) vExpanded.erase(vExpanded.size() - 1);
        folderNamesString = vExpanded;
    }
    #endif
    const std::vector<std::string> folders = listItems(folderNamesString);
    const std::vector<std::string> patternList = patterns ? listItems(patterns) : std::vector<std::string>();
    if(folders.empty()){
        fprintf(stderr, "ERROR: No input folder given. Use -F folder1,folder2,... to give the folders to search.\n");
        return 1;
    }
    if(patternList.empty()){
        fprintf(stderr, "ERROR: No channel pattern given. Use -P pattern1,pattern2,... (one pattern per channel).\n");
        return 1;
    }

    piw::ConvertOptions vOptions;
    if(vVoxelSizes && !parseFloats(vVoxelSizes, 3, vOptions.voxelSize)) return badValue('v', vVoxelSizes, 3);
    if(blockSizes){
        if(!parseFloatSizes(blockSizes, 3, vOptions.blockSize)) return badValue('b', blockSizes, 3);
        vOptions.hasBlockSize = true;
    }
    if(boundingBoxString){
        if(!parseFloatSizes(boundingBoxString, 6, vOptions.boundingBox)) return badValue('B', boundingBoxString, 6);
        vOptions.hasBoundingBox = true;
    }
    if(blockProcessString){
        if(!parseSizes(blockProcessString, 3, vOptions.processBlock)) return badValue('C', blockProcessString, 3);
        vOptions.hasProcessBlock = true;
    }

    if(!reader){
        printf("Reader type not specified. Defaulting to tiff.\n");
        reader = "tiff";
    }
    if(strcmp(reader,"tiff") && strcmp(reader,"zarr")){
        printf("ERROR: Reader type \"%s\" is not a supported reader type.\n",reader);
        return 0;
    }

    std::string outputDirString;
    if(!outputDirArg){
        outputDirString = folders[0];
        printf("Output Directory not provided. Using %s as the output directory.\n",outputDirString.c_str());
    }
    else outputDirString = outputDirArg;
    #ifdef WIN32
    int madeDir = mkdir(outputDirString.c_str());
    #else
    // Expand the tilde to the home directory on Mac and Linux. A char* argument selects cpp-tiff's
    // expandTilde(char*), as before (it runs wordexp on every path).
    {
        std::vector<char> vPath(outputDirString.begin(), outputDirString.end());
        vPath.push_back('\0');
        const char* expandedOutputDirString = expandTilde(vPath.data());
        if(expandedOutputDirString) outputDirString = expandedOutputDirString;
    }
    int madeDir = mkdir(outputDirString.c_str(), 0775);
    #endif
    (void)madeDir;
    DIR* outputDir = opendir(outputDirString.c_str());
    if(!outputDir){
        printf("ERROR: Given Output Directory \"%s\" does not exist and was unable to be created.\n",outputDirString.c_str());
        return 0;
    }
    closedir(outputDir);
    // Check if we can write to the output dir
    if(access(outputDirString.c_str(), W_OK)){
        printf("ERROR: Given Output Directory \"%s\" exists but cannot be written to.\n",outputDirString.c_str());
        return 0;
    }
    if (!outName) outName = "output";
    #ifdef _WIN32
    const std::string outputFile = outputDirString + "\\" + outName + ".ims";
    const std::string timepointsFile = outputDirString + "\\timepoints.txt";
    #else
    const std::string outputFile = outputDirString + "/" + outName + ".ims";
    const std::string timepointsFile = outputDirString + "/timepoints.txt";
    #endif

    // Files and patterns: files[channel][timepoint], sorted by name
    piw::Status vStatus = piw::findInputFiles(folders, patternList, vOptions.files);
    if(vStatus.code == piw::Code::NotFound){
        printf("ERROR: %s\n", vStatus.message.c_str());
        return 0;
    }
    if(!vStatus.ok()) return cliExitCode(vStatus);
    const uint64_t nChannels = vOptions.files.size();
    uint64_t nTimepoints = 0;
    for(size_t c = 0; c < vOptions.files.size(); ++c) nTimepoints += vOptions.files[c].size();

    if(channels == -1) channels = nChannels;
    if((uint64_t)channels != nChannels){
        printf("Number of channels given \"%lld\" is incorrect",(long long)channels);
        return 0;
    }

    if(timepoints == -1) timepoints = nTimepoints/nChannels;
    if(nTimepoints/nChannels != (uint64_t)timepoints){
        printf("Number of timepoints given \"%lld\" is incorrect",(long long)timepoints);
        return 0;
    }

    FILE* fp;
    fp = fopen(timepointsFile.c_str(),"a");
    if(fp){
        fprintf(fp,"Folder Names: %s\n",folderNamesString.c_str());
        for(size_t c = 0; c < vOptions.files.size(); c++){
            fprintf(fp,"Channel Pattern: %s\n",patternList[c].c_str());
            for(size_t t = 0; t < vOptions.files[c].size(); t++) fprintf(fp,"%s\n",vOptions.files[c][t].c_str());
        }
        fclose(fp);
    }

    vOptions.reader = reader;
    vOptions.outputFile = outputFile;

    bpCallbackData aCallbackUserData;
    aCallbackUserData.mImageIndex = 0;
    aCallbackUserData.mProgress = -5;
    piw::Callbacks vCallbacks;
    vCallbacks.message = MessageCallback;
    vCallbacks.progress = ProgressCallback;
    vCallbacks.user = &aCallbackUserData;

    return cliExitCode(piw::convert(vOptions, vCallbacks));
}

}  // namespace

namespace piw {

int runCli(int argc, char** argv) noexcept
{
    int vExitCode = 1;
    try{
        // Cluster subcommands (setup/worker/monitor/...); otherwise the single-node conversion.
        if(argc >= 2 && isParallelCommand(argv[1])) vExitCode = runParallel(argc, argv);
        else vExitCode = convertToImaris(argc, argv);
    }
    catch(const std::string& vError){ fprintf(stderr, "ERROR: %s\n", vError.c_str()); vExitCode = 1; }
    catch(const std::exception& vError){ fprintf(stderr, "ERROR: %s\n", vError.what()); vExitCode = 1; }
    catch(...){ fprintf(stderr, "ERROR: unknown exception\n"); vExitCode = 1; }
    fflush(stdout);
    fflush(stderr);
    return vExitCode;
}

}  // namespace piw
