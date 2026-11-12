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
// Modified for Parallel_Imaris_Writer (see third_party/imariswriter/README.md): adds Close(), creates the
// HDF5 writer in the constructor (not in the writer thread, where a block's write could come first),
// creates the threads after the members they use (they are declared last), and gives the blocks between
// compressed and written an in-flight budget of at least 2 GB.
#include "bpWriterCompressor.h"
#include "bpWriterThreads.h"
#include "bpThreadPool.h"

#include <algorithm>


// The memory (MB) for the blocks that are complete but not written yet (raw and compressed). Upstream it
// is threads*3 + 32 (56 MB at 8 threads, about 27 blocks): every stall of the HDF5 writer thread, which
// shares the file with the kernel's writeback, then stopped the compression threads at once. The budget
// bounds the memory, it does not allocate it: memory is taken only while blocks wait for the writer (and
// kept for reuse until the converter is destroyed).
static const bpSize kMinInFlightBufferMB = 2048;


bpWriterCompressor::bpWriterCompressor(
  bpSharedPtr<bpWriterFactory> aWriterFactory,
  const bpString& aFilename,
  const bpImsLayout& aImageLayout,
  bpConverterTypes::tCompressionAlgorithmType aCompressionAlgorithmType,
  bpSize aNumberOfCompressionThreads,
  bpConverterTypes::tProgressCallback aProgressCallback)
: mProgressCallback(std::move(aProgressCallback)),
  mNumberOfBlocks(0),
  mNumberOfIncrements(0),
  mTotalBytesWritten(0),
  mCallbackThread(std::make_unique<bpThreadPool>(1)),
  mThreads(std::make_unique<bpWriterThreads>(std::max<bpSize>(aNumberOfCompressionThreads * 3 + 32, kMinInFlightBufferMB), aNumberOfCompressionThreads, std::make_shared<bpCompressionAlgorithmFactory>(), aCompressionAlgorithmType, aImageLayout.GetDataType()))
{
  if (mProgressCallback) {
    for (bpSize vIndex = 0; vIndex < aImageLayout.GetNumberOfResolutionLevels(); vIndex++) {
      const auto& vSize = aImageLayout.GetNBlocks(vIndex);
      mNumberOfBlocks += vSize[0] * vSize[1] * vSize[2];
    }
    mNumberOfBlocks *= aImageLayout.GetNumberOfChannels() * aImageLayout.GetNumberOfTimePoints();
  }

  // The HDF5 writer is created here, before any block can be written, and a file that cannot be created is
  // reported by the constructor. It was created by a task queued through the compression threads to the
  // writer thread: when the first block's compression finished before that task's, the block's write found
  // no writer (in this copy, the error "the output file was not created"; upstream, a null dereference).
  mWriter = aWriterFactory->CreateWriter(aFilename, aImageLayout, aCompressionAlgorithmType);
}


bpWriterCompressor::~bpWriterCompressor()
{
  try {
    FinishWriteDataBlocks();
  }
  catch (...) {
  }
}


static void CheckWriterCreated(const bpSharedPtr<bpWriter>& aWriter)
{
  // the constructor creates the HDF5 writer or throws: a guard only
  if (!aWriter) {
    throw bpError("bpWriterCompressor: the output file was not created");
  }
}


void bpWriterCompressor::WriteMetadata(
  const bpString& aApplicationName,
  const bpString& aApplicationVersion,
  const bpConverterTypes::cImageExtent& aImageExtent,
  const bpConverterTypes::tParameters& aParameters,
  const bpConverterTypes::tTimeInfoVector& aTimeInfoPerTimePoint,
  const bpConverterTypes::tColorInfoVector& aColorInfoPerChannel)
{
  CheckWriterCreated(mWriter);
  mWriter->WriteMetadata(aApplicationName, aApplicationVersion, aImageExtent, aParameters, aTimeInfoPerTimePoint, aColorInfoPerChannel);
}


void bpWriterCompressor::WriteHistogram(const bpHistogram& aHistogram, bpSize aIndexT, bpSize aIndexC, bpSize aIndexR)
{
  auto vWriteHistogram = [this, aHistogram, aIndexT, aIndexC, aIndexR] {
    CheckWriterCreated(mWriter);
    mWriter->WriteHistogram(aHistogram, aIndexT, aIndexC, aIndexR);
  };

  RunInWriteThread(std::move(vWriteHistogram));
}


void bpWriterCompressor::RunInWriteThread(std::function<void()> aFunction)
{
  bpWriterThreads::tWrite vPretendWrite = [vFunction = std::move(aFunction)](const void* aCompressedData, bpSize aDataSize) {
    vFunction();
  };

  mThreads->StartWrite(bpMemoryHandle(), 0, std::move(vPretendWrite), {});
}


void bpWriterCompressor::WriteThumbnail(const bpThumbnail& aThumbnail)
{
  CheckWriterCreated(mWriter);
  mWriter->WriteThumbnail(aThumbnail);
}


void bpWriterCompressor::Close()
{
  CheckWriterCreated(mWriter);
  mWriter->Close();
}


void bpWriterCompressor::WriteDataBlock(
  const void* aData, bpSize aDataSize,
  bpSize aBlockIndexX, bpSize aBlockIndexY, bpSize aBlockIndexZ,
  bpSize aIndexT, bpSize aIndexC, bpSize aIndexR)
{
  CheckWriterCreated(mWriter);
  mWriter->WriteDataBlock(aData, aDataSize, aBlockIndexX, aBlockIndexY, aBlockIndexZ, aIndexT, aIndexC, aIndexR);
}


void bpWriterCompressor::StartWriteDataBlock(
  bpMemoryHandle aData,
  bpSize aBlockIndexX, bpSize aBlockIndexY, bpSize aBlockIndexZ,
  bpSize aIndexT, bpSize aIndexC, bpSize aIndexR, tPreFunction aPreFunction)
{
  bpWriterThreads::tWrite vWriteBlock = [this, aBlockIndexX, aBlockIndexY, aBlockIndexZ, aIndexT, aIndexC, aIndexR](const void* aCompressedData, bpSize aDataSize) {
    WriteDataBlock(aCompressedData, aDataSize, aBlockIndexX, aBlockIndexY, aBlockIndexZ, aIndexT, aIndexC, aIndexR);
    if (mProgressCallback) {
      mCallbackThread->Run([this, aDataSize] { IncrementProgress(aDataSize); });
    }
  };

  mThreads->StartWrite(std::move(aData), 0, std::move(vWriteBlock), std::move(aPreFunction));
}


void bpWriterCompressor::FinishWriteDataBlocks()
{
  try {
    mThreads->FinishWrite();
  }
  catch (bpException&) {
    mCallbackThread->WaitAll();
    throw;
  }
  mCallbackThread->WaitAll();
}


void bpWriterCompressor::IncrementProgress(bpUInt64 aAdditionalBytesWritten)
{
  mNumberOfIncrements++;
  bpFloat vProgressPercent = static_cast<bpFloat>(mNumberOfIncrements) / mNumberOfBlocks;
  mTotalBytesWritten += aAdditionalBytesWritten;
  mProgressCallback(vProgressPercent, mTotalBytesWritten);
}
