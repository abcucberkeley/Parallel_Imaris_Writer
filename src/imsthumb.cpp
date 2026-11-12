// /Thumbnail for the cluster outputs through ImarisWriter's own thumbnail builder (see imsthumb.h).
#include "imsthumb.h"
#if defined(__GNUC__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wreorder"   // bpThumbnailBuilder::cResampler's member order
#endif
#include "bpThumbnailBuilder.h"
#if defined(__GNUC__)
#pragma GCC diagnostic pop
#endif
#include <string>

namespace imsthumb {

namespace {
std::vector<bpVec3> sizesOf(const std::vector<imscore::Vec3>& levelSizes){
    std::vector<bpVec3> v;
    for(const imscore::Vec3& s : levelSizes){ bpVec3 b = {{(bpSize)s[0], (bpSize)s[1], (bpSize)s[2]}}; v.push_back(b); }
    return v;
}
// The settings bpImageConverterImpl::Finish passes on: base-colour mode, the adjusted ColorRange.
bpConverterTypes::cColorInfo colorInfo(const imscore::ChannelDisplay& d){
    bpConverterTypes::cColorInfo c;
    c.mIsBaseColorMode = true;
    c.mBaseColor = bpConverterTypes::cColor{ d.color[0], d.color[1], d.color[2], d.color[3] };
    c.mOpacity = d.opacity; c.mRangeMin = d.rangeMin; c.mRangeMax = d.rangeMax; c.mGammaCorrection = d.gamma;
    return c;
}
template<class T>
imscore::Status build(const std::vector<imscore::Vec3>& levelSizes, const std::vector<std::vector<T> >& volumes,
                      const std::vector<imscore::ChannelDisplay>& display, const float extentMax[3], Thumbnail& out){
    out = Thumbnail();
    if(levelSizes.empty() || volumes.empty()) return imscore::Status::fail("thumbnail: no levels or no channels");
    if(display.size() != volumes.size()) return imscore::Status::fail("thumbnail: " + std::to_string(volumes.size()) + " channels but "
                                                                     + std::to_string(display.size()) + " channel colours");
    const std::vector<bpVec3> sizes = sizesOf(levelSizes);
    const bpSize r = bpComputeThumbnailIndexR(THUMBNAIL_SIZE, sizes);
    const bpSize n = sizes[r][0] * sizes[r][1] * sizes[r][2];
    for(size_t c = 0; c < volumes.size(); c++)
        if(volumes[c].size() != n) return imscore::Status::fail("thumbnail: channel " + std::to_string(c) + " has " + std::to_string(volumes[c].size())
                                                               + " voxels, level " + std::to_string(r) + " has " + std::to_string(n));
    // one block per channel: the whole level (only level r's block size is used)
    bpThumbnailBuilder<T> builder(THUMBNAIL_SIZE, sizes, sizes, volumes.size());
    for(size_t c = 0; c < volumes.size(); c++)
        builder.StartCopyDataBlock(bpConstMemoryBlock<T>(volumes[c].data(), n, []{}), 0, 0, 0, 0, c, r);
    std::vector<bpConverterTypes::cColorInfo> colors;
    for(const imscore::ChannelDisplay& d : display) colors.push_back(colorInfo(d));
    const bpConverterTypes::cImageExtent extent = { 0, 0, 0, extentMax[0], extentMax[1], extentMax[2] };
    const bpThumbnail t = builder.CreateThumbnail(colors, extent);
    out.sizeX = t.GetSizeX(); out.sizeY = t.GetSizeY();
    out.rgba.assign(t.GetRGBAPointer(), t.GetRGBAPointer() + out.sizeX * out.sizeY * 4);
    return imscore::Status();
}
}  // namespace

size_t thumbnailLevel(const std::vector<imscore::Vec3>& levelSizes){
    return levelSizes.empty() ? 0 : (size_t)bpComputeThumbnailIndexR(THUMBNAIL_SIZE, sizesOf(levelSizes));
}

imscore::Status buildThumbnail(const std::vector<imscore::Vec3>& levelSizes, const std::vector<std::vector<uint16_t> >& volumes,
                               const std::vector<imscore::ChannelDisplay>& display, const float extentMax[3], Thumbnail& out){
    try{ return build(levelSizes, volumes, display, extentMax, out); }
    catch(const std::exception& e){ return imscore::Status::fail(std::string("thumbnail: ") + e.what()); }
    catch(const char* e){ return imscore::Status::fail(std::string("thumbnail: ") + e); }
    catch(...){ return imscore::Status::fail("thumbnail: unexpected error"); }
}

imscore::Status writeThumbnail(hid_t file, const Thumbnail& t){
    if(t.sizeX * t.sizeY == 0) return imscore::Status();
    if(t.rgba.size() != t.sizeX * t.sizeY * 4) return imscore::Status::fail("thumbnail: wrong data size");
    const imscore::Status bad = imscore::Status::fail("cannot write /Thumbnail (HDF5 error above)");
    hid_t g = H5Gcreate2(file, "Thumbnail", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    if(g < 0) return bad;
    hsize_t dims[2] = { (hsize_t)t.sizeY, (hsize_t)t.sizeX * 4 };
    hid_t sp = H5Screate_simple(2, dims, NULL);
    hid_t d = sp < 0 ? -1 : H5Dcreate2(g, "Data", H5T_NATIVE_UCHAR, sp, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    bool ok = d >= 0 && H5Dwrite(d, H5T_NATIVE_UCHAR, H5S_ALL, H5S_ALL, H5P_DEFAULT, t.rgba.data()) >= 0;
    if(d >= 0) ok = H5Dclose(d) >= 0 && ok;
    if(sp >= 0) H5Sclose(sp);
    ok = H5Gclose(g) >= 0 && ok;
    return ok ? imscore::Status() : bad;
}

imscore::Status writeThumbnail(hid_t file, const std::vector<imscore::Vec3>& levelSizes, const std::vector<std::vector<uint16_t> >& volumes,
                               const std::vector<imscore::ChannelDisplay>& display, const float extentMax[3]){
    Thumbnail t; imscore::Status s = buildThumbnail(levelSizes, volumes, display, extentMax, t);
    return s.ok() ? writeThumbnail(file, t) : s;
}

}  // namespace imsthumb
