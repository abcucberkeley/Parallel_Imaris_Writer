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
// Modified for Parallel_Imaris_Writer (see third_party/imariswriter/README.md): the first failed write or
// compression is kept and thrown by every later StartWrite and by FinishWrite, later writes are skipped,
// the memory of a block whose compression failed is returned, the free-memory count is atomic, and the
// threads are joined before the members their tasks use are destroyed.
#include "bpWriterThreads.h"
#include "bpMemoryManager.h"
#include "bpThreadPool.h"

#include <atomic>
#include <mutex>


// The message of the exception being handled (call inside a catch block); ImarisWriter also throws C strings.
static bpString GetCurrentExceptionMessage()
{
  try {
    throw;
  }
  catch (const bpException& aException) {
    return aException.what();
  }
  catch (const char* aMessage) {
    return aMessage ? aMessage : "Unknown error";
  }
  catch (...) {
    return "Unknown error";
  }
}


class bpWriterThreads::cImpl
{
public:
  cImpl(bpSize aMaxBufferSizeMB, bpSize aNumberOfThreads, bpCompressionAlgorithm::tPtr aCompressionAlgorithm)
    : mFreeMemory(aMaxBufferSizeMB * 1024 * 1024),
      mCompressionAlgorithm(std::move(aCompressionAlgorithm)),
      mWriterThread(1),
      mCompressionThreads(aNumberOfThreads)
  {
  }

  void StartWrite(bpMemoryHandle aData, tWrite aWrite, tPreFunction aPreFunction)
  {
    ThrowIfFailed();

    if (!mCompressionAlgorithm) {
      bpThreadPool::tCallback vReturnMemory = WaitReserveMemory(aData.GetSize());
      bpThreadPool::tFunction vWrite = [aData, vOriginalWrite = std::move(aWrite)]{
        vOriginalWrite(aData.GetData(), aData.GetSize());
      };
      RunInCompressionThread(std::move(aPreFunction), {}, std::move(vWrite), std::move(vReturnMemory));
      return;
    }

    bpSize vMaxCompressedDataSize = mCompressionAlgorithm->GetMaxCompressedSize(aData.GetSize());
    bpSize vAllocSize = aData.GetSize() + vMaxCompressedDataSize;

    bpThreadPool::tCallback vReturnMemory = WaitReserveMemory(vAllocSize);

    bpMemoryBlock<bpUInt8> vBuffer = mManager.GetMemory(vMaxCompressedDataSize);
    bpSharedPtr<bpSize> vCompressedDataSize = std::make_shared<bpSize>();

    auto vCompress = [this, aData, vBuffer, vCompressedDataSize] {
      bpSize vResultSize = vBuffer.GetSize();
      mCompressionAlgorithm->Compress(aData.GetData(), aData.GetSize(), vBuffer.GetData(), vResultSize);
      *vCompressedDataSize = vResultSize;
    };

    bpThreadPool::tFunction vWrite = [vBuffer, vOriginalWrite = std::move(aWrite), vCompressedDataSize]{
      vOriginalWrite(vBuffer.GetData(), *vCompressedDataSize);
    };

    RunInCompressionThread(std::move(aPreFunction), std::move(vCompress), std::move(vWrite), std::move(vReturnMemory));
  }

  void FinishWrite()
  {
    mCompressionThreads.WaitAll();
    mWriterThread.WaitAll();
    mWriterThread.CallFinishedCallbacks();
    ThrowIfFailed();
  }

private:
  // Runs aPreFunction and aCompress in a compression thread and then aWrite in the writer thread. The
  // reserved memory is returned (aReturnMemory, called back from the writer thread's finished callbacks)
  // after the write, or as soon as one of the steps has failed. Once anything has failed, the writes
  // still queued are skipped: the HDF5 writer does not even exist when creating the file failed.
  void RunInCompressionThread(tPreFunction aPreFunction, bpThreadPool::tFunction aCompress, bpThreadPool::tFunction aWrite, bpThreadPool::tCallback aReturnMemory)
  {
    bpThreadPool::tFunction vWrite = [this, vDoWrite = std::move(aWrite)] {
      if (mFailed) {
        return;
      }
      try {
        vDoWrite();
      }
      catch (...) {
        SetFailed(GetCurrentExceptionMessage());
      }
    };

    bpThreadPool::tFunction vFunction = [this, vDoWrite = std::move(vWrite), vDoCompress = std::move(aCompress), vDoReturnMemory = std::move(aReturnMemory), vPreFunction = std::move(aPreFunction)] () mutable {
      try {
        if (vPreFunction) {
          vPreFunction();
        }
        if (vDoCompress) {
          vDoCompress();
        }
        mWriterThread.Run(vDoWrite, vDoReturnMemory);
      }
      catch (...) {
        SetFailed(GetCurrentExceptionMessage());
        try {
          mWriterThread.Run({}, vDoReturnMemory);
        }
        catch (...) {
        }
      }
    };
    mCompressionThreads.Run(std::move(vFunction));
  }

  bpThreadPool::tCallback WaitReserveMemory(bpSize aSize)
  {
    mWriterThread.CallFinishedCallbacks();
    while (static_cast<bpInt64>(aSize) > mFreeMemory) {
      ThrowIfFailed();
      mWriterThread.WaitOne();
      mWriterThread.CallFinishedCallbacks();
    }

    mFreeMemory -= aSize;
    bpThreadPool::tCallback vReturnMemory = [this, aSize] {
      mFreeMemory += aSize;
    };
    return vReturnMemory;
  }

  // Keeps the first failure (of any thread); later ones are consequences of it.
  void SetFailed(bpString aMessage)
  {
    std::lock_guard<std::mutex> vLock(mFailureMutex);
    if (!mFailed) {
      mFailureMessage = std::move(aMessage);
      mFailed = true;
    }
  }

  void ThrowIfFailed() const
  {
    if (mFailed) {
      std::lock_guard<std::mutex> vLock(mFailureMutex);
      throw bpError(mFailureMessage);
    }
  }

  // declared before the threads, which may use them until they are joined
  std::atomic<bool> mFailed{ false };
  mutable std::mutex mFailureMutex;
  bpString mFailureMessage;

  // returned by the finished callbacks, which several threads call (the histogram threads write too)
  std::atomic<bpInt64> mFreeMemory;
  bpMemoryManager<bpUInt8> mManager;
  bpCompressionAlgorithm::tPtr mCompressionAlgorithm;

  // The threads last, so that they are joined (and the tasks still queued destroyed) before what their tasks
  // use is destroyed: the memory manager, whose blocks the tasks hold, and the compression algorithm (the
  // threads were declared first). The compression threads queue the writes: they are joined first.
  bpThreadPool mWriterThread;
  bpThreadPool mCompressionThreads;
};


bpWriterThreads::bpWriterThreads(bpSize aMaxBufferSizeMB, bpSize aNumberOfThreads, bpCompressionAlgorithmFactory::tPtr aCompressionAlgorithmFactory, bpConverterTypes::tCompressionAlgorithmType aCompressionAlgorithmType, bpConverterTypes::tDataType aDataType)
  : mImpl(std::make_shared<cImpl>(aMaxBufferSizeMB, aNumberOfThreads, std::move(aCompressionAlgorithmFactory->Create(aCompressionAlgorithmType, aDataType))))
{
}


void bpWriterThreads::StartWrite(bpMemoryHandle aData, bpSize aCompressionLevel, tWrite aWrite, tPreFunction aPreFunction)
{
  mImpl->StartWrite(std::move(aData), std::move(aWrite), std::move(aPreFunction));
}


void bpWriterThreads::FinishWrite()
{
  mImpl->FinishWrite();
}
