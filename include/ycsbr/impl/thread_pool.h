#pragma once

#include <condition_variable>
#include <functional>
#include <future>
#include <mutex>
#include <queue>
#include <thread>
#include <type_traits>
#include <vector>

namespace ycsbr {
namespace impl {

// A thread pool that supports thread-to-core pinning.
//
// Acknowledgements: This implementation is based on other existing thread pools
//   - https://github.com/fbastos1/thread_pool_cpp17
//   - https://github.com/progschj/ThreadPool
//   - https://github.com/vit-vit/CTPL
class ThreadPool {
 public:
  // Create a thread pool with `num_threads` threads.//!构造函数
  ThreadPool(size_t num_threads, std::function<void()> on_start,
             std::function<void()> on_shutdown);

  // Create a thread pool with `num_threads` threads and pin each thread to the
  // core id specified by `thread_to_core`.//!创建一个包含“num_threads”线程数的线程池，并将每个线程固定到“thread_to_core”指定的核心id。
  //
  // The `thread_to_core` vector must be of size `num_threads`. The value at
  // `thread_to_core[i]` represents the core id that thread `i` should be pinned
  // to, where `0 <= i < num_threads`.//!构造函数，“thread_to_core”矢量的大小必须为“num_threads”。“thread_to_core[i]”处的值表示线程“i”应固定到的核心id，其中“0<=i<num_threads”。
  ThreadPool(size_t num_threads, const std::vector<size_t>& thread_to_core,
             std::function<void()> on_start, std::function<void()> on_shutdown);

  // Waits for all submitted functions to execute before returning.//等待所有提交的函数执行后再返回。
  ~ThreadPool();

  // Schedule `f(...args)` to run on a thread in this thread pool.//计划“f（…args）”在此线程池中的线程上运行。
  //
  // This method returns a `std::future` that can be used to wait for `f` to
  // run and to retrieve its return value (if any).//!此方法返回一个“std:：future”，此方法用于等待“f”运行并返回其结果（如果有）。
  template <typename Function, typename... Args,
            std::enable_if_t<std::is_invocable<Function&&, Args&&...>::value,    //?std::is_invocable<Function&&, Args&&...>::value：这是一个条件，它使用std::is_invocable 类型特性检查函数模板是否可以被调用（是否可以通过给定的参数列表被调用）。
                             bool> = true>    //?std::enable_if_t：这是模板元编程工具，用于根据条件启用或禁用函数模板。
  auto Submit(Function&& f, Args&&... args);

  // Similar to `Submit()`, but instead does not provide a future that can be
  // used to wait on the function's result.
  template <typename Function, typename... Args,
            std::enable_if_t<std::is_invocable<Function&&, Args&&...>::value,
                             bool> = true>
  void SubmitNoWait(Function&& f, Args&&... args);  //!类似于“Submit（）”，但没有提供可用于等待函数结果的future。

 private:
  class Task {
   public:
    virtual ~Task() = default;
    virtual void operator()() = 0;
  };

  template <typename Function>
  class TaskContainer : public Task {
   public:
    TaskContainer(Function&& f) : f_(std::forward<Function>(f)) {} //!构造函数，初始化f_
    void operator()() override { f_(); }

   private:
    Function f_;
  };

  // Worker threads run this code.
  void ThreadMain();

  // Ensures the worker thread runs `ThreadMain()` on core `core_id`. //!确保工作线程在核心“core_id”上运行“ThreadMain（）”`
  void ThreadMainOnCore(size_t core_id);

  std::mutex mutex_;
  std::condition_variable cv_;
  bool shutdown_;
  std::queue<std::unique_ptr<Task>> work_queue_;
  std::vector<std::thread> threads_;

  // Called by each thread when it starts.
  std::function<void()> on_start_;
  // Called by each thread when it shuts down.
  std::function<void()> on_shutdown_;
};

template <   //Submit是用来添加task，并通知一个线程来取task的函数
    typename Function, typename... Args,         //模板的参数列表
    std::enable_if_t<std::is_invocable<Function&&, Args&&...>::value, bool>>  //?满足条件 std::is_invocable<Function&&, Args&&...>::value 为真，才允许使用这个函数模板
inline auto ThreadPool::Submit(Function&& f, Args&&... args) {
  std::packaged_task<std::invoke_result_t<Function, Args...>()> task(  //?std::invoke_result_t<Function, Args...>() 是函数调用的返回类型
      [runnable = std::move(f),
       task_args = std::make_tuple(std::forward<Args>(args)...)]() mutable {  //lambda 函数，用于执行任务
        return std::apply(std::move(runnable), std::move(task_args));  //?std::apply(Function, Tuple);
      });
  auto future = task.get_future();    //创建了一个 std::future 对象，用于获取任务的返回值
  {
    std::unique_lock<std::mutex> lock(mutex_);
    work_queue_.emplace(new TaskContainer(std::move(task)));  //将任务添加到线程池的工作队列中
  }
  cv_.notify_one();  //通知一个等待的线程来执行任务
  return future;
}

template <
    typename Function, typename... Args,
    std::enable_if_t<std::is_invocable<Function&&, Args&&...>::value, bool>>
inline void ThreadPool::SubmitNoWait(Function&& f, Args&&... args) {
  auto task = [runnable = std::move(f),
               task_args =
                   std::make_tuple(std::forward<Args>(args)...)]() mutable {
    std::apply(std::move(runnable), std::move(task_args));     
  };
  {
    std::unique_lock<std::mutex> lock(mutex_);
    work_queue_.emplace(new TaskContainer(std::move(task)));
  }
  cv_.notify_one();
}

}  // namespace impl
}  // namespace ycsbr

#include "thread_pool-inl.h"
