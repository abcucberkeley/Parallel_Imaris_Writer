#ifndef PARALLEL_IMARIS_IMSTHUMB_H
#define PARALLEL_IMARIS_IMSTHUMB_H

// /Thumbnail of the cluster outputs, the same bytes as the single-node converter's. The thumbnail is made by
// ImarisWriter's own builder (bpThumbnailBuilder, compiled from the fetched ImarisWriter sources), fed with the
// voxels the cluster writers read back from their chunks: the level is the coarsest with X and Y >= 256 (level 0
// if none), timepoint 0, every channel; the maximum projection and the middle Z slice are resampled to the
// 256-pixel box with square pixels, coloured with each channel's display settings (ColorRange after the
// auto-adjust), and the one with the higher "quality" (luminance spread) is kept, as RGBA uint8 /Thumbnail/Data
// of shape (sizeY, sizeX * 4).
//
// ImarisWriter feeds the builder one memory block at a time; the result does not depend on the blocking (each
// thumbnail pixel takes one source voxel per slice, the maximum or the middle slice's value, from whichever
// block holds it), so here each channel's level is one dense block.
#include "imscore.h"
#include <cstdint>
#include <vector>

namespace imsthumb {

// The thumbnail box (ImarisWriter's mThumbnailSizeXY, which the converter sets to 256).
const imscore::bpSize THUMBNAIL_SIZE = 256;

// The level the thumbnail is built from (bpComputeThumbnailIndexR): the coarsest with X and Y >= 256, else 0.
size_t thumbnailLevel(const std::vector<imscore::Vec3>& levelSizes);

struct Thumbnail { imscore::bpSize sizeX = 0, sizeY = 0; std::vector<uint8_t> rgba; };   // rows of sizeX RGBA pixels

// The thumbnail of one timepoint. levelSizes: every level's X, Y, Z; volumes[c]: channel c's voxels of level
// thumbnailLevel(levelSizes) (X*Y*Z, x fastest); display[c]: its colour and ColorRange as written in /DataSetInfo;
// extentMax: the image extent (ExtMax0..2; the minimum is 0).
imscore::Status buildThumbnail(const std::vector<imscore::Vec3>& levelSizes, const std::vector<std::vector<uint16_t> >& volumes,
                               const std::vector<imscore::ChannelDisplay>& display, const float extentMax[3], Thumbnail& out);

// Writes /Thumbnail/Data as bpWriterHDF5::WriteThumbnail does (the group, then a contiguous uint8 dataset);
// nothing for an empty thumbnail.
imscore::Status writeThumbnail(hid_t file, const Thumbnail& thumbnail);

// buildThumbnail + writeThumbnail.
imscore::Status writeThumbnail(hid_t file, const std::vector<imscore::Vec3>& levelSizes, const std::vector<std::vector<uint16_t> >& volumes,
                               const std::vector<imscore::ChannelDisplay>& display, const float extentMax[3]);

}  // namespace imsthumb

#endif
