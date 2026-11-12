#ifndef PARALLEL_IMARIS_IMSMETA_H
#define PARALLEL_IMARIS_IMSMETA_H

// The /DataSetInfo metadata shared by the single-node converter (src/converter.cpp, through
// ImarisWriter) and the cluster path (src/imscore.h, writeImsMeta): the metadata options of the
// command line and of `setup`, the defaults, the time stamps, and the text forms ImarisWriter
// writes. Both paths resolve the options with resolveMetadata and write what it returns, so the
// same inputs and options give the same attributes (header-only, no HDF5).
//
// Time stamps are local wall-clock times kept as milliseconds since 1970-01-01 00:00 of that
// clock (no time zone); see ConvertOptions in parallelimariswriter.h for how they are chosen.
#include "parallelimariswriter.h"
#include <cctype>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <string>
#include <vector>
#include <sys/stat.h>

namespace imsmeta {

// What both paths write, one entry per channel or per time point.
struct Metadata {
    std::vector<std::string> names;
    std::vector<piw::Color> colors;
    std::vector<float> emission;          // nm; <= 0: no LSMEmissionWavelength
    std::vector<float> excitation;        // nm; <= 0: no LSMExcitationWavelength
    std::vector<int64_t> times;           // ms since 1970-01-01 00:00, local wall clock
};

// ImarisDataSet/Creator; Version is piw::version().
inline const char* creatorName(){ return "parallelimariswriter"; }

// Longer strings are written as datasets by ImarisWriter, so names stay below that.
static const size_t kMaxNameLength = 1024;

// ===================== calendar =====================
inline int64_t floorDiv(int64_t a, int64_t b){ int64_t q = a / b; return (a % b != 0 && ((a < 0) != (b < 0))) ? q - 1 : q; }
// Days since 1970-01-01 of a proleptic Gregorian date (H. Hinnant's days_from_civil).
inline int64_t daysFromCivil(int64_t y, int64_t m, int64_t d){
    y -= m <= 2;
    const int64_t era = floorDiv(y, 400);
    const int64_t yoe = y - era * 400;
    const int64_t doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
    const int64_t doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + doe - 719468;
}
inline bool leapYear(int64_t y){ return (y % 4 == 0 && y % 100 != 0) || y % 400 == 0; }
inline int daysInMonth(int64_t y, int m){
    static const int kDays[12] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    return m == 2 && leapYear(y) ? 29 : kDays[m - 1];
}
static const int64_t kMsPerDay = 86400000;
static const int64_t kJulianDayOf1970 = 2440588;    // Julian day number of 1970-01-01
// The time stamps both paths accept: years 1000 to 9999 (four-digit years in the text form).
inline int64_t minTime(){ return daysFromCivil(1000, 1, 1) * kMsPerDay; }
inline int64_t maxTime(){ return daysFromCivil(10000, 1, 1) * kMsPerDay - 1; }

// ImarisWriter's bpConverterTypesC_TimeInfo of a time stamp.
inline void julianTime(int64_t aMs, unsigned& aJulianDay, unsigned long long& aNanosecondsOfDay){
    const int64_t vDays = floorDiv(aMs, kMsPerDay);
    aJulianDay = (unsigned)(vDays + kJulianDayOf1970);
    aNanosecondsOfDay = (unsigned long long)(aMs - vDays * kMsPerDay) * 1000000ull;
}

// The text ImarisWriter writes for a time stamp (bpImsUtils::TimeInfoToString, ported):
// "YYYY-MM-DD HH:MM:SS", plus ".mmm" when the milliseconds are not 0.
inline std::string timeString(int64_t aMs){
    unsigned vJulianDay; unsigned long long vNs;
    julianTime(aMs, vJulianDay, vNs);
    long t1 = (long)vJulianDay + 68569L;             // bpImsUtils::FromJulianDay
    const long t2 = 4L * t1 / 146097L;
    t1 -= (146097L * t2 + 3L) / 4L;
    const long y = 4000L * (t1 + 1) / 1461001L;
    t1 = t1 - 1461L * y / 4L + 31;
    const long m = 80L * t1 / 2447L;
    const long vDay = t1 - 2447L * m / 80L;
    t1 = m / 11L;
    const long vMonth = m + 2L - 12L * t1;
    const long vYear = 100L * (t2 - 49L) + y + t1;
    const unsigned long long vSeconds = vNs / 1000000000ull;
    const unsigned vMs = (unsigned)((vNs / 1000000ull) % 1000);
    char vText[64];
    snprintf(vText, sizeof vText, "%02ld-%02ld-%02ld %02llu:%02llu:%02llu", vYear, vMonth, vDay,
             vSeconds / 3600, (vSeconds / 60) % 60, vSeconds % 60);
    std::string vResult = vText;
    if(vMs > 0){ snprintf(vText, sizeof vText, ".%03u", vMs); vResult += vText; }
    return vResult;
}

// "YYYY-MM-DD[ HH:MM[:SS[.fff]]]" ('T' may separate date and time; digits after the third of a
// fraction are ignored) -> a time stamp. timeString's output reads back to the same value.
inline bool parseTime(const std::string& aText, int64_t& aMs){
    const char* p = aText.c_str();
    auto number = [&](int aDigits, int64_t& aValue) -> bool {
        aValue = 0;
        for(int i = 0; i < aDigits; ++i){
            if(!isdigit((unsigned char)*p)) return false;
            aValue = aValue * 10 + (*p++ - '0');
        }
        return true;
    };
    int64_t vYear, vMonth, vDay, vHour = 0, vMinute = 0, vSecond = 0, vMs = 0;
    if(!number(4, vYear) || *p++ != '-' || !number(2, vMonth) || *p++ != '-' || !number(2, vDay)) return false;
    if(*p == ' ' || *p == 'T'){
        ++p;
        if(!number(2, vHour) || *p++ != ':' || !number(2, vMinute)) return false;
        if(*p == ':'){
            ++p;
            if(!number(2, vSecond)) return false;
            if(*p == '.'){
                ++p;
                if(!isdigit((unsigned char)*p)) return false;
                for(int i = 0; isdigit((unsigned char)*p); ++i, ++p) if(i < 3) vMs += (*p - '0') * (i == 0 ? 100 : i == 1 ? 10 : 1);
            }
        }
    }
    if(*p != '\0') return false;
    if(vYear < 1000 || vMonth < 1 || vMonth > 12 || vDay < 1 || vDay > daysInMonth(vYear, (int)vMonth) ||
       vHour > 23 || vMinute > 59 || vSecond > 59) return false;
    aMs = ((daysFromCivil(vYear, vMonth, vDay) * 24 + vHour) * 60 + vMinute) * 60000 + vSecond * 1000 + vMs;
    return true;
}

// A time_t as a time stamp: local wall clock, whole seconds.
inline bool localTime(time_t aTime, int64_t& aMs){
    struct tm vLocal;
#ifdef _WIN32
    const struct tm* vShared = localtime(&aTime);      // the Windows C runtime keeps one per thread
    if(!vShared) return false;
    vLocal = *vShared;
#else
    if(!localtime_r(&aTime, &vLocal)) return false;
#endif
    aMs = ((daysFromCivil(vLocal.tm_year + 1900, vLocal.tm_mon + 1, vLocal.tm_mday) * 24 + vLocal.tm_hour) * 60 +
           vLocal.tm_min) * 60000 + (int64_t)vLocal.tm_sec * 1000;
    return true;
}
// The modification time of a file or folder, local wall clock, whole seconds.
inline bool modificationTime(std::string aPath, int64_t& aMs){
    while(aPath.size() > 1 && (aPath[aPath.size() - 1] == '/' || aPath[aPath.size() - 1] == '\\')) aPath.erase(aPath.size() - 1);
    struct stat vInfo;
    if(stat(aPath.c_str(), &vInfo) != 0) return false;
    return localTime(vInfo.st_mtime, aMs);
}

// The "<n>msecAbs" stamp in a file name (PetaKit5D's names: ..._0000123msec_0012345678msecAbs.zarr),
// the last one in the file's own name (not its folders).
inline bool msecAbsStamp(const std::string& aPath, int64_t& aMs){
    size_t vEnd = aPath.size();
    while(vEnd > 0 && (aPath[vEnd - 1] == '/' || aPath[vEnd - 1] == '\\')) --vEnd;
    const size_t vSlash = aPath.find_last_of("/\\", vEnd ? vEnd - 1 : 0);
    const size_t vStart = (vSlash == std::string::npos || vSlash >= vEnd) ? 0 : vSlash + 1;
    const std::string vName = aPath.substr(vStart, vEnd - vStart);
    for(size_t vPos = vName.rfind("msecAbs"); vPos != std::string::npos && vPos > 0; vPos = vName.rfind("msecAbs", vPos - 1)){
        size_t vFirst = vPos;
        while(vFirst > 0 && isdigit((unsigned char)vName[vFirst - 1])) --vFirst;
        if(vFirst == vPos || vPos - vFirst > 15) continue;          // no digits, or more than 15 (> 30,000 years)
        aMs = strtoll(vName.substr(vFirst, vPos - vFirst).c_str(), NULL, 10);
        return true;
    }
    return false;
}

// ===================== text forms =====================
// LSMEmissionWavelength / LSMExcitationWavelength: "488", "488.5".
inline std::string wavelengthString(float aNm){ char b[32]; snprintf(b, sizeof b, "%g", (double)aNm); return b; }
inline piw::Color defaultColor(size_t aChannel){
    piw::Color c = {aChannel % 3 == 0 ? 1.f : 0.f, aChannel % 3 == 1 ? 1.f : 0.f, aChannel % 3 == 2 ? 1.f : 0.f};
    return c;
}
inline std::string defaultName(size_t aChannel){ return "Channel " + std::to_string(aChannel); }

// ===================== options =====================
// Resolves the metadata of nC channels x nT time points from the metadata fields of o (those of
// piw::ConvertOptions; piw::WriteOptions and piw::MonoInitOptions have the same ones): checks
// them, applies the defaults and computes the time stamps. aFiles = the inputs, [channel][time
// point], or NULL when there are none: then the start time defaults to the current time and the
// time points are 1 s (or timeInterval) apart. Returns "" or what is wrong.
template<class Options>
inline std::string resolveMetadataFields(const Options& o, size_t nC, size_t nT,
                                         const std::vector<std::vector<std::string> >* aFiles, Metadata& m){
    m = Metadata();
    if(!nC || !nT) return "No input files were given.";
    auto count = [&](size_t n, const char* what) -> std::string {
        if(n == 0 || n == nC) return std::string();
        return std::string("The ") + what + " list has " + std::to_string(n) + " entries for " + std::to_string(nC) + " channels.";
    };
    std::string vError = count(o.channelNames.size(), "channel name");
    if(vError.empty()) vError = count(o.channelColors.size(), "channel color");
    if(vError.empty()) vError = count(o.emissionWavelengths.size(), "emission wavelength");
    if(vError.empty()) vError = count(o.excitationWavelengths.size(), "excitation wavelength");
    if(!vError.empty()) return vError;
    for(size_t c = 0; c < nC; ++c){
        const std::string vName = o.channelNames.empty() ? defaultName(c) : o.channelNames[c];
        if(vName.empty() || vName.size() > kMaxNameLength)
            return "Channel " + std::to_string(c) + "'s name must have 1 to " + std::to_string(kMaxNameLength) + " characters.";
        m.names.push_back(vName);
        const piw::Color vColor = o.channelColors.empty() ? defaultColor(c) : o.channelColors[c];
        const float vRgb[3] = {vColor.red, vColor.green, vColor.blue};
        for(int i = 0; i < 3; ++i)
            if(!(vRgb[i] >= 0.f && vRgb[i] <= 1.f)) return "Channel " + std::to_string(c) + "'s color values must be between 0 and 1.";
        m.colors.push_back(vColor);
        const float vEm = o.emissionWavelengths.empty() ? 0.f : o.emissionWavelengths[c];
        const float vEx = o.excitationWavelengths.empty() ? 0.f : o.excitationWavelengths[c];
        if(!std::isfinite(vEm) || !std::isfinite(vEx)) return "Channel " + std::to_string(c) + "'s wavelength is not a number.";
        m.emission.push_back(vEm > 0.f ? vEm : 0.f);
        m.excitation.push_back(vEx > 0.f ? vEx : 0.f);
    }
    if(!std::isfinite(o.timeInterval) || o.timeInterval < 0 || o.timeInterval > 1e9)
        return "The time interval must be 0 (none) or a number of seconds up to 1e9.";

    int64_t vStart = 0;
    if(!o.startTime.empty()){
        if(!parseTime(o.startTime, vStart))
            return "Could not read the start time \"" + o.startTime + "\": expected YYYY-MM-DD HH:MM:SS[.mmm].";
    }
    else if(!(aFiles && modificationTime((*aFiles)[0][0], vStart)) && !localTime(time(NULL), vStart)){
        vStart = (int64_t)time(NULL) * 1000;          // the current time: no input, or one that does not exist
    }
    std::vector<int64_t> vOffsets(nT, 0);
    bool vStamped = false;
    if(o.timeInterval > 0){
        for(size_t t = 0; t < nT; ++t) vOffsets[t] = (int64_t)std::llround((double)t * o.timeInterval * 1000.0);
    }
    else{
        for(size_t c = 0; aFiles && c < nC && !vStamped; ++c){
            std::vector<int64_t> vStamps(nT);
            bool vAll = true;
            for(size_t t = 0; t < nT && vAll; ++t) vAll = msecAbsStamp((*aFiles)[c][t], vStamps[t]);
            if(!vAll) continue;
            for(size_t t = 0; t < nT; ++t) vOffsets[t] = vStamps[t] - vStamps[0];
            vStamped = true;
        }
        if(!vStamped) for(size_t t = 0; t < nT; ++t) vOffsets[t] = (int64_t)t * 1000;
    }
    for(size_t t = 0; t < nT; ++t){
        const int64_t vTime = vStart + vOffsets[t];
        if(vTime < minTime() || vTime > maxTime())
            return "Time point " + std::to_string(t) + " falls outside the years 1000 to 9999" +
                   (vStamped ? " (its msecAbs stamp is too far from the first)." : ".");
        m.times.push_back(vTime);
    }
    return std::string();
}
// Resolves the metadata of a conversion of options.files (which must not be empty).
inline std::string resolveMetadata(const piw::ConvertOptions& o, Metadata& m){
    const size_t nC = o.files.size(), nT = nC ? o.files[0].size() : 0;
    return resolveMetadataFields(o, nC, nT, &o.files, m);
}

// The metadata options of the command line and of `setup`, as --<name> <value> or --<name>=<value>.
struct OptionInfo { const char* name; const char* value; const char* help; };
inline const std::vector<OptionInfo>& metadataOptions(){
    static const std::vector<OptionInfo> kOptions = {
        {"channel-names", "A,B,...", "channel names (default: the -P patterns)"},
        {"colors", "C1,C2,...", "channel colors: red, green, blue, cyan, magenta, yellow, white, gray, RRGGBB (hex) or r:g:b (0-1); default red,green,blue,..."},
        {"emission", "NM1,NM2,...", "emission wavelengths in nm (0: none for that channel; default: none)"},
        {"excitation", "NM1,NM2,...", "excitation wavelengths in nm (0: none for that channel; default: none)"},
        {"time-interval", "SECONDS", "time between time points (default: the files' msecAbs stamps, else 1 s)"},
        {"start-time", "\"YYYY-MM-DD HH:MM:SS[.mmm]\"", "time of the first time point (default: the first input's modification time)"},
    };
    return kOptions;
}
inline bool isMetadataOption(const std::string& aName){
    for(const OptionInfo& i : metadataOptions()) if(aName == i.name) return true;
    return false;
}

// Comma-separated items, empty ones included ("a,,b" -> a, "", b).
inline std::vector<std::string> splitItems(const std::string& aText){
    std::vector<std::string> v;
    size_t vStart = 0;
    for(;;){
        const size_t vEnd = aText.find(',', vStart);
        v.push_back(aText.substr(vStart, vEnd == std::string::npos ? std::string::npos : vEnd - vStart));
        if(vEnd == std::string::npos) return v;
        vStart = vEnd + 1;
    }
}
inline bool parseNumber(const std::string& aText, double& aValue){
    if(aText.empty() || isspace((unsigned char)aText[0])) return false;
    char* vEnd = NULL;
    aValue = strtod(aText.c_str(), &vEnd);
    return *vEnd == '\0' && std::isfinite(aValue);
}
inline bool parseColor(std::string aText, piw::Color& aColor){
    static const struct { const char* name; float r, g, b; } kNamed[] = {
        {"red", 1, 0, 0}, {"green", 0, 1, 0}, {"blue", 0, 0, 1}, {"cyan", 0, 1, 1}, {"magenta", 1, 0, 1},
        {"yellow", 1, 1, 0}, {"white", 1, 1, 1}, {"gray", 0.5f, 0.5f, 0.5f}, {"grey", 0.5f, 0.5f, 0.5f}};
    for(const auto& n : kNamed) if(aText == n.name){ aColor.red = n.r; aColor.green = n.g; aColor.blue = n.b; return true; }
    if(aText.find(':') != std::string::npos){
        const size_t a = aText.find(':'), b = aText.find(':', a + 1);
        double v[3] = {0, 0, 0};
        if(b == std::string::npos || !parseNumber(aText.substr(0, a), v[0]) || !parseNumber(aText.substr(a + 1, b - a - 1), v[1]) ||
           !parseNumber(aText.substr(b + 1), v[2])) return false;
        aColor.red = (float)v[0]; aColor.green = (float)v[1]; aColor.blue = (float)v[2];
        return true;
    }
    if(!aText.empty() && aText[0] == '#') aText.erase(0, 1);
    if(aText.size() != 6) return false;
    for(char ch : aText) if(!isxdigit((unsigned char)ch)) return false;
    const unsigned long v = strtoul(aText.c_str(), NULL, 16);
    aColor.red = (float)((v >> 16) & 255) / 255.f; aColor.green = (float)((v >> 8) & 255) / 255.f; aColor.blue = (float)(v & 255) / 255.f;
    return true;
}

// Reads one metadata option (aName without the leading dashes) into o. Returns "" or what is wrong.
// Value ranges are checked later by resolveMetadata.
inline std::string parseMetadataOption(const std::string& aName, const std::string& aValue, piw::ConvertOptions& o){
    const std::string vBad = "Could not read --" + aName + " \"" + aValue + "\": ";
    const std::vector<std::string> vItems = splitItems(aValue);
    if(aName == "channel-names"){
        for(const std::string& s : vItems) if(s.empty()) return vBad + "a channel name is empty.";
        o.channelNames = vItems;
    }
    else if(aName == "colors"){
        o.channelColors.clear();
        for(const std::string& s : vItems){
            piw::Color c;
            if(!parseColor(s, c)) return vBad + "\"" + s + "\" is not a color (a name, RRGGBB or r:g:b).";
            o.channelColors.push_back(c);
        }
    }
    else if(aName == "emission" || aName == "excitation"){
        std::vector<float>& vList = aName == "emission" ? o.emissionWavelengths : o.excitationWavelengths;
        vList.clear();
        for(const std::string& s : vItems){
            double v;
            if(!parseNumber(s, v) || v < 0 || v > 1e6) return vBad + "expected comma-separated wavelengths in nm (0: none).";
            vList.push_back((float)v);
        }
    }
    else if(aName == "time-interval"){
        double v;
        if(!parseNumber(aValue, v) || v <= 0 || v > 1e9) return vBad + "expected a number of seconds greater than 0.";
        o.timeInterval = v;
    }
    else if(aName == "start-time"){
        int64_t v;
        if(!parseTime(aValue, v)) return vBad + "expected YYYY-MM-DD HH:MM:SS[.mmm].";
        o.startTime = aValue;
    }
    else return "Unknown option --" + aName + ".";
    return std::string();
}

}  // namespace imsmeta

#endif
