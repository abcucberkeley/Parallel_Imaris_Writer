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
// Modified for Parallel_Imaris_Writer (see third_party/imariswriter/README.md): a task's function, and
// everything it captured, is destroyed before the task counts as finished, and every waiter is woken when a
// task finishes.
#include "bpThreadPool.h"

#include <deque>

#include <thread>
#include <condition_variable>


class bpThreadPool::cImpl
{
public:
  explicit cImpl(bpSize aNumberOfThreads)
    : mNumberOfThreads(aNumberOfThreads)
  {
  }

  ~cImpl()
  {
    {
      std::lock_guard<std::mutex> vLock(mMutex);
      mTerminated = true;
      mTaskAddedCondition.notify_all();
    }

    for (std::thread& vThread : mThreads) {
      vThread.join();
    }
  }

  using tFunction = std::function<void()>;
  using tCallback = std::function<void()>;

  void Run(tFunction aFuntion, tCallback aCallback, bool aHighPriority)
  {
    std::lock_guard<std::mutex> vLock(mMutex);
    if (mNumberOfThreads == 0) {
      if (aFuntion) {
        aFuntion();
      }
      if (aCallback) {
        mFinishedCallbacks.emplace_back(std::move(aCallback), tError());
      }
      mTaskFinishedCondition.notify_all();
      return;
    }

    if (mNumberOfRunningTasks >= mThreads.size() && mThreads.size() < mNumberOfThreads) {
      mThreads.emplace_back([this] { TreadLoop(); });
    }

    if (aHighPriority) {
      mTasks.emplace_front(std::move(aFuntion), std::move(aCallback));
    }
    else {
      mTasks.emplace_back(std::move(aFuntion), std::move(aCallback));
    }
    mTaskAddedCondition.notify_one();
  }

  void WaitOne()
  {
    std::unique_lock<std::mutex> vLock(mMutex);
    if (mNumberOfRunningTasks > 0 || !mTasks.empty()) {
      mTaskFinishedCondition.wait(vLock);
    }
  }

  void WaitAll()
  {
    std::unique_lock<std::mutex> vLock(mMutex);
    mTaskFinishedCondition.wait(vLock, [this] { return mTasks.empty() && mNumberOfRunningTasks == 0; });
  }

  bpSize WaitSome(bpSize aMaxNumberOfWaitingFunctions)
  {
    std::unique_lock<std::mutex> vLock(mMutex);
    mTaskFinishedCondition.wait(vLock, [this, aMaxNumberOfWaitingFunctions] { return mTasks.size() <= aMaxNumberOfWaitingFunctions; });
    return mTasks.size();
  }

  void CallFinishedCallbacks()
  {
    std::deque<tFinishedCallback> vFinishedCallbacks;
    {
      std::lock_guard<std::mutex> vLock(mMutex);
      if (mFinishedCallbacks.empty()) return;
      std::swap(vFinishedCallbacks, mFinishedCallbacks);
    }
    tError vError;
    for (tFinishedCallback& vCallback : vFinishedCallbacks) {
      vCallback.first();
      if (!vError && vCallback.second) {
        vError = std::move(vCallback.second);
      }
    }
    if (vError) {
      throw *vError;
    }
  }

private:
  using tTask = std::pair<tFunction, tCallback>;
  using tError = bpUniquePtr<bpError>;
  using tFinishedCallback = std::pair<tCallback, tError>;

  void TreadLoop()
  {
    while (true) {
      tCallback vCallback;
      tError vError;
      {
        tTask vTask;
        if (!PopTask(vTask)) {
          return;
        }
        vCallback = std::move(vTask.second);

        if (vTask.first) {
          try {
            vTask.first();
          }
          catch (bpError& aError) {
            vError = std::make_unique<bpError>(std::move(aError));
          }
          catch (...) {
            vError = std::make_unique<bpError>("Unknown error");
          }
        }
        // The function, and what it captured, is destroyed here, before the task counts as finished: a thread
        // that waits for the task (WaitAll) may destroy what the captures refer to as soon as it returns. It
        // was destroyed after the count and the notification, so when a converter was destroyed right after
        // its writer threads had finished, the memory blocks the last write task held were returned to the
        // writer's memory manager after it had been freed (heap corruption, "free(): corrupted unsorted chunks").
      }

      std::lock_guard<std::mutex> vLock(mMutex);
      --mNumberOfRunningTasks;
      if (vCallback) {
        mFinishedCallbacks.emplace_back(std::move(vCallback), std::move(vError));
      }
      // all waiters: several threads may wait on one pool (WaitAll, WaitOne and WaitSome), and with a
      // single notification the thread woken may not be the one whose condition now holds
      mTaskFinishedCondition.notify_all();
    }
  }

  bool PopTask(tTask& aTask)
  {
    std::unique_lock<std::mutex> vLock(mMutex);
    mTaskAddedCondition.wait(vLock, [this] { return !mTasks.empty() || mTerminated; });
    if (!mTasks.empty()) {
      aTask = std::move(mTasks.front());
      mTasks.pop_front();
      ++mNumberOfRunningTasks;
    }
    return !mTerminated;
  }

  bpSize mNumberOfThreads;
  bool mTerminated = false;
  std::deque<std::thread> mThreads;
  std::deque<tTask> mTasks;
  std::deque<tFinishedCallback> mFinishedCallbacks;
  bpSize mNumberOfRunningTasks = 0;
  std::mutex mMutex;
  std::condition_variable mTaskAddedCondition;
  std::condition_variable mTaskFinishedCondition;
};


bpThreadPool::bpThreadPool(bpSize aNumberOfThreads)
  : mImpl(std::make_shared<cImpl>(aNumberOfThreads))
{
}


void bpThreadPool::Run(tFunction aFuntion, tCallback aCallback, bool aHighPriority)
{
  mImpl->Run(aFuntion, aCallback, aHighPriority);
}


void bpThreadPool::WaitOne()
{
  mImpl->WaitOne();
}


void bpThreadPool::WaitAll()
{
  mImpl->WaitAll();
}


bpSize bpThreadPool::WaitSome(bpSize aMaxNumberOfWaitingFunctions)
{
  return mImpl->WaitSome(aMaxNumberOfWaitingFunctions);
}


void bpThreadPool::CallFinishedCallbacks()
{
  mImpl->CallFinishedCallbacks();
}
