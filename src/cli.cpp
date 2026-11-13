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

// The command line (piw::runCli): the cluster subcommands, verify, else the single-node conversion. The
// conversion itself is piw::convert; every error of the conversion's command line ends with one
// "ERROR: <text>" line on stderr and exit code 1 (cliExitCode turns a Status into both).
#include "parallelimariswriter.h"
#include "imsmeta.h"
#include "parallel.h"
#include "mono.h"
#include <getopt.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <unistd.h>
#include <dirent.h>
#include <stdint.h>
#include <sys/stat.h>
#ifdef _WIN32
#include <direct.h>
#endif
#include <cmath>
#include <exception>
#include <string>
#include <vector>

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

// PIW_TIMING=1: one line on stderr when the file is written.
void TimingCallback(double aReadSeconds, double aFeedSeconds, double aFinishSeconds, double aVerifySeconds, void*)
{
    fprintf(stderr, "Timing: read %.2f s, feed %.2f s, finish and close %.2f s, verify %.2f s\n", aReadSeconds, aFeedSeconds,
            aFinishSeconds, aVerifySeconds);
}

int cliError(const std::string& aText)
{
    fprintf(stderr, "ERROR: %s\n", aText.c_str());
    return 1;
}

// Status -> output and exit code: 0, or one ERROR line and 1.
int cliExitCode(const piw::Status& aStatus)
{
    if(aStatus.ok()) return 0;
    return cliError(aStatus.message);
}

void printUsage(FILE* aOut)
{
    fprintf(aOut,
        "usage: parallelimariswriter -F FOLDER[,FOLDER...] -P PATTERN[,PATTERN...] [options]\n"
        "       parallelimariswriter setup|worker|monitor|combineone|link|linkparts|layout|monoinit|monoplace|monofinish|monostatus ...\n"
        "       parallelimariswriter plan|task|status|finish ...   (plan -h: the cluster steps as tasks)\n"
        "       parallelimariswriter verify FILE.ims [FILE.ims...]   (verify -h: checks that files are complete)\n"
        "Converts TIFF or Zarr stacks to one Imaris .ims file. Each pattern is one channel; the files in the\n"
        "folders whose names contain it are its time points, in the order of their names (full paths). The\n"
        "file is written as NAME.ims.partial, checked, and renamed to NAME.ims when complete, replacing an\n"
        "existing NAME.ims (which an error leaves as it was).\n"
        "  -F FOLDERS      comma-separated input folders\n"
        "  -P PATTERNS     comma-separated file-name patterns, one per channel\n"
        "  -r READER       tiff (.tif, .tiff) or zarr (.zarr); default tiff\n"
        "  -o FOLDER       output folder, created if needed (default: the first input folder)\n"
        "  -n NAME         output file name without .ims (default: output)\n"
        "  -v X,Y,Z        voxel size in um, each > 0 (default: 0.108,0.108,0.108)\n"
        "  -c N            number of channels: must equal the number of patterns\n"
        "  -t N            number of time points: must equal the number of files per pattern\n"
        "  -b X,Y,Z        size of the blocks handed to the Imaris writer, each >= 1 (default: the\n"
        "                  input's chunks; does not change the output)\n"
        "  -B X0,Y0,Z0,X1,Y1,Z1  crop to [X0,X1) x [Y0,Y1) x [Z0,Z1), 0-based\n"
        "  -C X,Y,Z        voxels of a time point held in memory at once, each >= 1 (TIFF: Z only;\n"
        "                  default: a whole time point)\n"
        "  --no-overwrite  if NAME.ims exists, say so and exit 0 without converting\n"
        "  --no-verify     skip the check of the written file (see verify) before it is renamed\n");
    const std::vector<imsmeta::OptionInfo>& vMetadataOptions = imsmeta::metadataOptions();
    for(size_t i = 0; i < vMetadataOptions.size(); ++i){
        fprintf(aOut, "  --%s %s\n                  %s\n", vMetadataOptions[i].name, vMetadataOptions[i].value, vMetadataOptions[i].help);
    }
    fprintf(aOut,
        "  -h, --help      this text\n"
        "Exit code 0 when the file was written (or, with --no-overwrite, exists); 1 on any error, after one\n"
        "\"ERROR: ...\" line on stderr.\n");
}

// The items of a comma-separated list, spaces around them removed; one trailing comma is fine
// (the MATLAB wrapper writes "-v a,b,c,"), other empty items are not. False for an empty list.
bool listValues(const std::string& aText, std::vector<std::string>& aItems)
{
    aItems.clear();
    size_t vStart = 0;
    for(;;){
        size_t vEnd = aText.find(',', vStart);
        const bool vLast = vEnd == std::string::npos;
        if(vLast) vEnd = aText.size();
        size_t vA = vStart, vB = vEnd;
        while(vA < vB && (aText[vA] == ' ' || aText[vA] == '\t')) ++vA;
        while(vB > vA && (aText[vB - 1] == ' ' || aText[vB - 1] == '\t')) --vB;
        if(vA == vB){
            if(!(vLast && !aItems.empty())) return false;   // empty, except after a trailing comma
        }
        else aItems.push_back(aText.substr(vA, vB - vA));
        if(vLast) return !aItems.empty();
        vStart = vEnd + 1;
    }
}

// A whole number written as digits only.
bool parseWhole(const std::string& aText, uint64_t& aValue)
{
    if(aText.empty()) return false;
    for(size_t i = 0; i < aText.size(); ++i) if(aText[i] < '0' || aText[i] > '9') return false;
    errno = 0;
    aValue = strtoull(aText.c_str(), NULL, 10);
    return errno == 0;
}

// aCount numbers > 0 (-v).
bool parsePositiveFloats(const char* aText, unsigned aCount, float* aValues)
{
    std::vector<std::string> vItems;
    if(!listValues(aText, vItems) || vItems.size() != aCount) return false;
    for(unsigned i = 0; i < aCount; ++i){
        char* vEnd = NULL;
        aValues[i] = strtof(vItems[i].c_str(), &vEnd);
        if(*vEnd != '\0' || !std::isfinite(aValues[i]) || !(aValues[i] > 0.0f)) return false;
    }
    return true;
}

// aCount whole numbers >= aMin (-b, -B, -C).
bool parseWholes(const char* aText, unsigned aCount, uint64_t aMin, uint64_t* aValues)
{
    std::vector<std::string> vItems;
    if(!listValues(aText, vItems) || vItems.size() != aCount) return false;
    for(unsigned i = 0; i < aCount; ++i){
        if(!parseWhole(vItems[i], aValues[i]) || aValues[i] < aMin) return false;
    }
    return true;
}

// The items of -F and -P: empty items (a trailing comma) are skipped.
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

// A leading "~" or "~/" is the home folder ($HOME) on Mac and Linux; nothing else in the path
// changes. Not on Windows.
std::string expandHome(const std::string& aPath)
{
    #ifndef _WIN32
    if(!aPath.empty() && aPath[0] == '~' && (aPath.size() == 1 || aPath[1] == '/')){
        const char* vHome = getenv("HOME");
        if(vHome && *vHome) return std::string(vHome) + aPath.substr(1);
    }
    #endif
    return aPath;
}

bool isSeparator(char aChar)
{
    #ifdef _WIN32
    return aChar == '/' || aChar == '\\';
    #else
    return aChar == '/';
    #endif
}

// Creates aPath and every missing folder above it. Returns 0, or the errno of the last mkdir
// that failed (the caller then checks whether the folder is there).
int makeFolders(const std::string& aPath)
{
    int vErrno = 0;
    for(size_t i = 1; i <= aPath.size(); ++i){
        if(i < aPath.size() && !isSeparator(aPath[i])) continue;
        if(isSeparator(aPath[i - 1])) continue;                // "//", or a trailing separator
        const std::string vPart = aPath.substr(0, i);
        if(vPart[vPart.size() - 1] == ':') continue;           // a Windows drive ("C:")
        #ifdef _WIN32
        const int vMade = _mkdir(vPart.c_str());
        #else
        const int vMade = mkdir(vPart.c_str(), 0775);
        #endif
        vErrno = (vMade == 0 || errno == EEXIST) ? 0 : errno;
    }
    return vErrno;
}

int badValue(char aOption, const char* aText, const char* aExpected)
{
    return cliError(std::string("Could not read -") + aOption + " \"" + aText + "\": expected " + aExpected + ".");
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

// The converter's options as the command line gives them (readConversion); plan takes the same.
struct ConversionArgs {
    piw::ConvertOptions options;                       // reader, files, -v, -b, -B, -C, the metadata options
    std::vector<std::string> folders, patterns;        // -F (tilde-expanded), -P
    std::string folderList;                            // -F as written into timepoints.txt
    const char* outputDir = NULL;                      // -o, -n
    const char* outName = NULL;
    std::vector<std::pair<int, std::string> > extra;   // a subcommand's own long options: (index, value)
};
// A subcommand's own long option.
struct ExtraOption {
    const char* name;
    bool value;
};

// Reads argv[1..] (getopt) and finds the input files (options.files; -c and -t checked against them). -1 when
// that worked, else the exit code (0 after -h, which prints aUsage; 1 after an ERROR line).
int readConversion(int argc, char** argv, ConversionArgs& aArgs, const std::vector<ExtraOption>& aExtra, void (*aUsage)(FILE*))
{
    const char* channelsString = NULL;
    const char* timepointsString = NULL;
    int option_index = 0;
    const char* vVoxelSizes = NULL;
    const char* folderNames = NULL;
    const char* patterns = NULL;
    const char* reader = NULL;
    const char* blockSizes = NULL;
    const char* blockProcessString = NULL;
    const char* boundingBoxString = NULL;
    piw::ConvertOptions& vOptions = aArgs.options;
    // The metadata options (--channel-names, --colors, ...; see imsmeta.h) are long options only.
    const int kMetadataBase = 1000, kExtraBase = 2000;
    const std::vector<imsmeta::OptionInfo>& vMetadataOptions = imsmeta::metadataOptions();
    std::vector<struct option> vLongOptions;
    for(size_t i = 0; i < vMetadataOptions.size(); ++i){
        struct option vOption = {vMetadataOptions[i].name, required_argument, NULL, kMetadataBase + (int)i};
        vLongOptions.push_back(vOption);
    }
    for(size_t i = 0; i < aExtra.size(); ++i){
        struct option vOption = {aExtra[i].name, aExtra[i].value ? required_argument : no_argument, NULL, kExtraBase + (int)i};
        vLongOptions.push_back(vOption);
    }
    struct option vHelp = {"help", no_argument, NULL, 'h'};
    vLongOptions.push_back(vHelp);
    struct option vEnd = {NULL, 0, NULL, 0};
    vLongOptions.push_back(vEnd);
    resetGetopt();
    while (( option_index = getopt_long(argc, argv, ":hc:P:t:F:o:r:v:n:b:B:C:", vLongOptions.data(), NULL)) != -1){
        if(option_index >= kMetadataBase && option_index < kMetadataBase + (int)vMetadataOptions.size()){
            const std::string vError = imsmeta::parseMetadataOption(vMetadataOptions[option_index - kMetadataBase].name, optarg, vOptions);
            if(!vError.empty()) return cliError(vError);
            continue;
        }
        if(option_index >= kExtraBase && option_index < kExtraBase + (int)aExtra.size()){
            aArgs.extra.push_back(std::make_pair(option_index - kExtraBase, std::string(optarg ? optarg : "")));
            continue;
        }
        // The argument getopt just looked at, to name a bad long option.
        const char* vArg = (optind > 0 && optind <= argc) ? argv[optind - 1] : "";
        const bool vLong = !strncmp(vArg, "--", 2);
        switch (option_index) {
        case 'h':
            aUsage(stdout);
            return 0;
        case 'c':
            channelsString = optarg;
            break;
        case 'P':
            patterns = optarg;
            break;
        case 't':
            timepointsString = optarg;
            break;
        case 'F':
            folderNames = optarg;
            break;
        case 'o':
            aArgs.outputDir = optarg;
            break;
        case 'r':
            reader = optarg;
            break;
        case 'v':
            vVoxelSizes = optarg;
            break;
        case 'n':
            aArgs.outName = optarg;
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
        case ':':
            if(optopt >= kMetadataBase && optopt < kMetadataBase + (int)vMetadataOptions.size()){
                return cliError(std::string("Option --") + vMetadataOptions[optopt - kMetadataBase].name + " needs a value. Run with -h for the options.");
            }
            if(optopt >= kExtraBase && optopt < kExtraBase + (int)aExtra.size()){
                return cliError(std::string("Option --") + aExtra[optopt - kExtraBase].name + " needs a value. Run with -h for the options.");
            }
            if(vLong && strchr(vArg, '=')) return cliError(std::string("Option ") + vArg + " takes no value. Run with -h for the options.");
            if(vLong) return cliError(std::string("Option ") + vArg + " needs a value. Run with -h for the options.");
            return cliError(std::string("Option -") + (char)optopt + " needs a value. Run with -h for the options.");
        default:
            if(vLong) return cliError(std::string("Unknown or malformed option \"") + vArg + "\". Run with -h for the options.");
            if(optopt > ' ' && optopt < 127) return cliError(std::string("Unknown option -") + (char)optopt + ". Run with -h for the options.");
            return cliError(std::string("Unknown option \"") + vArg + "\". Run with -h for the options.");
        }
    }
    if(optind < argc) return cliError(std::string("Unexpected argument \"") + argv[optind] + "\". Run with -h for the options.");

    // Tilde expansion item by item (a leading ~ only), then the folders without empty items
    std::vector<std::string>& folders = aArgs.folders;
    folders = listItems(folderNames ? folderNames : "");
    std::string& folderNamesString = aArgs.folderList;
    for(size_t i = 0; i < folders.size(); ++i){
        folders[i] = expandHome(folders[i]);
        folderNamesString += (i ? "," : "") + folders[i];
    }
    const std::vector<std::string>& patternList = aArgs.patterns = patterns ? listItems(patterns) : std::vector<std::string>();
    if(folders.empty()) return cliError("No input folder given. Use -F folder1,folder2,... to give the folders to search.");
    if(patternList.empty()) return cliError("No channel pattern given. Use -P pattern1,pattern2,... (one pattern per channel).");

    if(vVoxelSizes && !parsePositiveFloats(vVoxelSizes, 3, vOptions.voxelSize)) return badValue('v', vVoxelSizes, "three numbers greater than 0 (X,Y,Z)");
    if(blockSizes){
        if(!parseWholes(blockSizes, 3, 1, vOptions.blockSize)) return badValue('b', blockSizes, "three whole numbers of at least 1 (X,Y,Z)");
        vOptions.hasBlockSize = true;
    }
    if(boundingBoxString){
        if(!parseWholes(boundingBoxString, 6, 0, vOptions.boundingBox)) return badValue('B', boundingBoxString, "six whole numbers (X0,Y0,Z0,X1,Y1,Z1)");
        vOptions.hasBoundingBox = true;
    }
    if(blockProcessString){
        if(!parseWholes(blockProcessString, 3, 1, vOptions.processBlock)) return badValue('C', blockProcessString, "three whole numbers of at least 1 (X,Y,Z)");
        vOptions.hasProcessBlock = true;
    }
    uint64_t channels = 0, timepoints = 0;
    if(channelsString && (!parseWhole(channelsString, channels) || !channels)) return badValue('c', channelsString, "a whole number of at least 1");
    if(timepointsString && (!parseWhole(timepointsString, timepoints) || !timepoints)) return badValue('t', timepointsString, "a whole number of at least 1");

    if(!reader){
        printf("Reader type not specified. Defaulting to tiff.\n");
        reader = "tiff";
    }
    if(strcmp(reader,"tiff") && strcmp(reader,"zarr")){
        return cliError(std::string("Reader type \"") + reader + "\" is not a supported reader type (tiff or zarr).");
    }
    vOptions.reader = reader;

    // Files and patterns: files[channel][timepoint], sorted by name
    piw::Status vStatus = piw::findInputFiles(folders, patternList, reader, vOptions.files);
    if(!vStatus.ok()) return cliExitCode(vStatus);
    const uint64_t nChannels = vOptions.files.size();
    const uint64_t nTimepoints = vOptions.files[0].size();
    if(channelsString && channels != nChannels){
        return cliError("-c " + std::to_string((unsigned long long)channels) + " was given, but -P gives " +
                        std::to_string((unsigned long long)nChannels) + " pattern(s), one per channel.");
    }
    if(timepointsString && timepoints != nTimepoints){
        return cliError("-t " + std::to_string((unsigned long long)timepoints) + " was given, but each channel has " +
                        std::to_string((unsigned long long)nTimepoints) + " file(s).");
    }
    return -1;
}

// Creates the output folder if needed and checks that it can be written to; 0, or the exit code after an ERROR line.
int prepareOutputFolder(const std::string& outputDirString)
{
    const int vMkdirErrno = makeFolders(outputDirString);
    DIR* outputDir = opendir(outputDirString.c_str());
    if(!outputDir){
        return cliError("Given Output Directory \"" + outputDirString + "\" does not exist and was unable to be created" +
                        (vMkdirErrno ? std::string(": ") + strerror(vMkdirErrno) : std::string()) + ".");
    }
    closedir(outputDir);
    // Check if we can write to the output dir
    if(access(outputDirString.c_str(), W_OK)){
        return cliError("Given Output Directory \"" + outputDirString + "\" exists but cannot be written to.");
    }
    return 0;
}

int convertToImaris(int argc, char **argv)
{
    if(argc < 2){
        printUsage(stderr);
        return cliError("No arguments were given; the options are listed above.");
    }
    ConversionArgs vArgs;
    const ExtraOption vOwn[2] = {{"no-overwrite", false}, {"no-verify", false}};
    const int vRead = readConversion(argc, argv, vArgs, std::vector<ExtraOption>(vOwn, vOwn + 2), printUsage);
    if(vRead >= 0) return vRead;
    bool vOverwrite = true;
    for(size_t i = 0; i < vArgs.extra.size(); ++i){
        if(vArgs.extra[i].first == 0) vOverwrite = false;
        else vArgs.options.verify = false;
    }
    piw::ConvertOptions& vOptions = vArgs.options;
    const std::vector<std::string>& folders = vArgs.folders;
    const std::vector<std::string>& patternList = vArgs.patterns;
    const std::string& folderNamesString = vArgs.folderList;
    const char* outputDirArg = vArgs.outputDir;
    const char* outName = vArgs.outName;

    std::string outputDirString;
    if(!outputDirArg){
        outputDirString = folders[0];
        printf("Output Directory not provided. Using %s as the output directory.\n",outputDirString.c_str());
    }
    else outputDirString = expandHome(outputDirArg);
    const int vFolder = prepareOutputFolder(outputDirString);
    if(vFolder) return vFolder;
    if (!outName) outName = "output";
    #ifdef _WIN32
    const std::string outputFile = outputDirString + "\\" + outName + ".ims";
    const std::string timepointsFile = outputDirString + "\\timepoints.txt";
    #else
    const std::string outputFile = outputDirString + "/" + outName + ".ims";
    const std::string timepointsFile = outputDirString + "/timepoints.txt";
    #endif
    // A file of that name is complete (it gets the name only then): nothing to do.
    struct stat vExisting;
    if(!vOverwrite && stat(outputFile.c_str(), &vExisting) == 0 && (vExisting.st_mode & S_IFMT) == S_IFREG){
        printf("%s exists; not converting (--no-overwrite).\n", outputFile.c_str());
        return 0;
    }

    FILE* fp = fopen(timepointsFile.c_str(),"a");
    if(fp){
        fprintf(fp,"Folder Names: %s\n",folderNamesString.c_str());
        for(size_t c = 0; c < vOptions.files.size(); c++){
            fprintf(fp,"Channel Pattern: %s\n",patternList[c].c_str());
            for(size_t t = 0; t < vOptions.files[c].size(); t++) fprintf(fp,"%s\n",vOptions.files[c][t].c_str());
        }
        if(fclose(fp)) fprintf(stderr, "WARNING: Could not write %s: %s; continuing.\n", timepointsFile.c_str(), strerror(errno));
    }
    else fprintf(stderr, "WARNING: Could not open %s: %s; continuing without it.\n", timepointsFile.c_str(), strerror(errno));

    vOptions.outputFile = outputFile;
    if(vOptions.channelNames.empty()) vOptions.channelNames = patternList;   // the channels are named after their patterns

    bpCallbackData aCallbackUserData;
    aCallbackUserData.mImageIndex = 0;
    aCallbackUserData.mProgress = -5;
    piw::Callbacks vCallbacks;
    vCallbacks.message = MessageCallback;
    vCallbacks.progress = ProgressCallback;
    vCallbacks.user = &aCallbackUserData;
    const char* vTiming = getenv("PIW_TIMING");
    if(vTiming && !strcmp(vTiming, "1")) vCallbacks.timing = TimingCallback;

    return cliExitCode(piw::convert(vOptions, vCallbacks));
}

void printPlanUsage(FILE* aOut)
{
    fprintf(aOut,
        "usage: parallelimariswriter plan WORKDIR -F FOLDER[,FOLDER...] -P PATTERN[,PATTERN...] [options]\n"
        "Checks every input as the converter does and writes WORKDIR/manifest.json: stages of independent tasks, each\n"
        "run by 'task WORKDIR STAGE INDEX' (any process, any node, any order within a stage), then 'finish WORKDIR'\n"
        "('status WORKDIR' lists the tasks not done). The converter's options apply (-h without plan lists them); -o\n"
        "and -n name the finished file (default WORKDIR/output.ims, on WORKDIR's filesystem) and -C is each task's\n"
        "process block. Without --tiles: a time series, stage \"convert\" with one task per (timepoint, channel).\n"
        "  --tiles X,Y,Z       one enormous volume per (timepoint, channel), cut into tiles of X,Y,Z voxels: stages\n"
        "                      \"level0\", \"level1\", ... (each after the one before); Zarr input, uint16\n"
        "  --store mono|packs  tiles: straight into the single file (default; not on Windows) or one file per tile\n"
        "  --tiles-per-task N  tiles: consecutive tiles of a level per task (default 1)\n"
        "  --chunk-bytes N     tiles: the chunk budget (default: env PIW_CHUNK_BYTES, else 1048576, the converter's)\n");
}

// plan WORKDIR [the converter's options] [the plan's own]: piw::clusterPlan (src/parallel.h).
int planCommand(int argc, char** argv)
{
    if(argc >= 3 && (!strcmp(argv[2], "-h") || !strcmp(argv[2], "--help"))){ printPlanUsage(stdout); return 0; }
    if(argc < 3 || argv[2][0] == '-'){ printPlanUsage(stderr); return cliError("plan needs the workdir first; the options are listed above."); }
    const std::string vWorkdir = expandHome(argv[2]);
    std::vector<char*> vArgv(1, argv[0]);
    for(int i = 3; i < argc; ++i) vArgv.push_back(argv[i]);
    vArgv.push_back(NULL);
    std::vector<ExtraOption> vExtra;
    const ExtraOption vOwn[4] = {{"tiles", true}, {"store", true}, {"tiles-per-task", true}, {"chunk-bytes", true}};
    vExtra.assign(vOwn, vOwn + 4);
    ConversionArgs vArgs;
    const int vRead = readConversion((int)vArgv.size() - 1, vArgv.data(), vArgs, vExtra, printPlanUsage);
    if(vRead >= 0) return vRead;
    piw::ClusterPlanOptions vPlan;
    vPlan.workdir = vWorkdir;
    vPlan.folders = vArgs.folders;
    vPlan.patterns = vArgs.patterns;
    vPlan.convert = vArgs.options;
    for(size_t i = 0; i < vArgs.extra.size(); ++i){
        const std::string& vValue = vArgs.extra[i].second;
        uint64_t vWhole = 0;
        switch(vArgs.extra[i].first){
        case 0:
            if(!parseWholes(vValue.c_str(), 3, 1, vPlan.tileSize)) return cliError("Could not read --tiles \"" + vValue + "\": expected three whole numbers of at least 1 (X,Y,Z).");
            vPlan.kind = "tiles";
            break;
        case 1: vPlan.store = vValue; break;
        case 2:
            if(!parseWhole(vValue, vWhole) || !vWhole || vWhole > 1000000000ull) return cliError("Could not read --tiles-per-task \"" + vValue + "\": expected a whole number of at least 1.");
            vPlan.tilesPerTask = (long)vWhole;
            break;
        case 3:
            if(!parseWhole(vValue, vWhole) || vWhole < 4096) return cliError("Could not read --chunk-bytes \"" + vValue + "\": expected a whole number of at least 4096.");
            vPlan.chunkBytes = vWhole;
            break;
        }
    }
    if(vPlan.kind != "tiles" && (!vPlan.store.empty() || vPlan.tilesPerTask != 1 || vPlan.chunkBytes))
        return cliError("--store, --tiles-per-task and --chunk-bytes are for a tile plan (--tiles X,Y,Z).");
    if(vArgs.outputDir || vArgs.outName){
        const std::string vDir = vArgs.outputDir ? expandHome(vArgs.outputDir) : vWorkdir;
        if(vArgs.outputDir){ const int vFolder = prepareOutputFolder(vDir); if(vFolder) return vFolder; }
        #ifdef _WIN32
        vPlan.convert.outputFile = vDir + "\\" + (vArgs.outName ? vArgs.outName : "output") + ".ims";
        #else
        vPlan.convert.outputFile = vDir + "/" + (vArgs.outName ? vArgs.outName : "output") + ".ims";
        #endif
    }
    piw::ClusterPlan vResult;
    const piw::Status vStatus = piw::clusterPlan(vPlan, &vResult);
    if(!vStatus.ok()) return cliExitCode(vStatus);
    const char* vTypes[5] = {"", "uint8", "uint16", "float32", "uint32"};
    printf("plan: %s: %s%s, %d timepoints x %d channels, %llux%llux%llu %s, %d levels -> %s\n", vResult.workdir.c_str(), vResult.kind.c_str(),
           vResult.kind == "tiles" ? (" (" + vResult.store + " store)").c_str() : "", vResult.timepoints, vResult.channels,
           (unsigned long long)vResult.size[0], (unsigned long long)vResult.size[1], (unsigned long long)vResult.size[2],
           vTypes[(int)vResult.dataType], vResult.levels, vResult.outputFile.c_str());
    for(size_t i = 0; i < vResult.stages.size(); ++i){
        const piw::ClusterStage& vStage = vResult.stages[i];
        printf("  stage %s: %ld tasks%s%s: task %s %s <0..%ld>\n", vStage.name.c_str(), vStage.tasks, vStage.after.empty() ? "" : ", after ",
               vStage.after.c_str(), vResult.workdir.c_str(), vStage.name.c_str(), vStage.tasks - 1);
    }
    printf("  then: finish %s [--verify]\n", vResult.workdir.c_str());
    return 0;
}

void printVerifyUsage(FILE* aOut)
{
    fprintf(aOut,
        "usage: parallelimariswriter verify FILE.ims [FILE.ims...]\n"
        "Checks that each file is a complete .ims, from its metadata alone (no image data is read): the size, time\n"
        "points and channels that /DataSetInfo gives, every resolution level of the pyramid for that size, and every\n"
        "dataset holding all of its chunks. Prints one line per complete file; for any other, an ERROR line that names\n"
        "the first missing or wrong attribute or dataset. Exit code 0 when every file is complete, else 1. Works on\n"
        "the files of the converter, of finish/monofinish/monitor (the single file) and of link/linkparts (with\n"
        "their parts).\n");
}

// verify FILE.ims [FILE.ims...]: piw::verifyIms on each.
int verifyCommand(int argc, char** argv)
{
    if(argc >= 3 && (!strcmp(argv[2], "-h") || !strcmp(argv[2], "--help"))){ printVerifyUsage(stdout); return 0; }
    if(argc < 3){ printVerifyUsage(stderr); return cliError("verify needs at least one file; the usage is above."); }
    const char* vTypes[5] = {"", "uint8", "uint16", "float32", "uint32"};
    int vExitCode = 0;
    for(int i = 2; i < argc; ++i){
        piw::ImsVerifyResult vResult;
        const piw::Status vStatus = piw::verifyIms(expandHome(argv[i]), &vResult);
        if(!vStatus.ok()){ vExitCode = cliExitCode(vStatus); continue; }
        printf("%s: complete: %llux%llux%llu %s, %u time point(s) x %u channel(s) x %u level(s), %llu chunks\n", argv[i],
               (unsigned long long)vResult.size[0], (unsigned long long)vResult.size[1], (unsigned long long)vResult.size[2],
               vTypes[(int)vResult.dataType], vResult.timepoints, vResult.channels, vResult.levels, (unsigned long long)vResult.chunks);
    }
    return vExitCode;
}

}  // namespace

namespace piw {

int runCli(int argc, char** argv) noexcept
{
    int vExitCode = 1;
    try{
        // Cluster subcommands (setup/worker/monitor/..., monoinit/monoplace/...), verify; otherwise the single-node conversion.
        if(argc >= 2 && isParallelCommand(argv[1])) vExitCode = runParallel(argc, argv);
        else if(argc >= 2 && !strcmp(argv[1], "plan")) vExitCode = planCommand(argc, argv);
        else if(argc >= 2 && !strcmp(argv[1], "verify")) vExitCode = verifyCommand(argc, argv);
        else if(argc >= 2 && isClusterCommand(argv[1])) vExitCode = runCluster(argc, argv);
        else if(argc >= 2 && isMonoCommand(argv[1])) vExitCode = runMono(argc, argv);
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
