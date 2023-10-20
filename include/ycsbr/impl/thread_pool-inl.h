#include <cassert>

#include "affinity.h"

namespace ycsbr {
namespace impl {

inline ThreadPool::ThreadPool(size_t num_threads,
                              std::function<void()> on_start,
                              std::function<void()> on_shutdown)
    : shutdown_(false),
      on_start_(std::move(on_start)),
      on_shutdown_(std::move(on_shutdown)) {
  for (size_t i = 0; i < num_threads; ++i) {   //每次循环都创建一个新线程
    threads_.emplace_back(&ThreadPool::ThreadMain, this);   //&ThreadPool::ThreadMain作为线程的执行函数，this指针作为传递给线程的参数；这意味着每个线程都将执行ThreadPool类中的ThreadMain成员函数，并且可以访问该线程池的实例
  }
}

inline ThreadPool::ThreadPool(size_t num_threads,
                              const std::vector<size_t>& thread_to_core,
                              std::function<void()> on_start,
                              std::function<void()> on_shutdown)
    : shutdown_(false),
      on_start_(std::move(on_start)),
      on_shutdown_(std::move(on_shutdown)) {
  assert(num_threads == thread_to_core.size());
  for (size_t i = 0; i < num_threads; ++i) {
    threads_.emplace_back(&ThreadPool::ThreadMainOnCore, this,
                          thread_to_core[i]);
  }
}

inline ThreadPool::~ThreadPool() {   //超出作用域自动析构，销毁线程，使线程执行on_shundown_()函数
  {
    std::unique_lock<std::mutex> lock(mutex_);
    shutdown_ = true;
  }
  cv_.notify_all();
  for (auto& thread : threads_) {
    thread.join();
  }
}

inline void ThreadPool::ThreadMainOnCore(size_t core_id) {
  PinToCore(core_id);
  ThreadMain();
}

inline void ThreadPool::ThreadMain() {    //每个线程接收任何可以执行的任务
  on_start_();  //创建统计信息local并置0
  std::unique_ptr<Task> next_job = nullptr;
  while (true) {
    {
      std::unique_lock<std::mutex> lock(mutex_);
      // Need a loop here to handle spurious wakeup   
      while (!shutdown_ && work_queue_.empty()) {
        cv_.wait(lock);      //!如果满足while条件，线程就释放锁，并一直在这里等待直到被唤醒
      }
      if (shutdown_ && work_queue_.empty()) break;    
      next_job.reset(work_queue_.front().release());  //从任务队列里取出一个任务
      work_queue_.pop();  
    }
    (*next_job)();
    next_job.reset(nullptr);
  }
  on_shutdown_();   //将local汇总到全局
}

}  // namespace impl
}  // namespace ycsbr
