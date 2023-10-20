#pragma once

#include <future>

namespace ycsbr {
namespace impl {

// A thread synchronization object representing a "flag" that can be raised (but
// never lowered). Threads can wait for the flag to be raised, and one thread is
// allowed to "raise" the flag to notify the waiting threads.//++一个线程同步对象，表示可以raise（但永远不会降低）的“flag”。线程可以等待该标志被raise，并且允许一个线程“raise”该flag以通知等待的线程。
class Flag {
 public:
  Flag() : future_(flag_.get_future().share()) {}  //!构造函数，初始化Flag

  // "Raises" this flag, allowing any threads that have called `Wait()` or will
  // call it in the future to proceed.//++“raises”此flag，允许任何已调用“Wait（）”或将来将调用它的线程继续进行。
  //
  // NOTE: Caller must guarantee that this method is called **at most once**.//++注意：调用者必须保证此方法最多被调用一次**。
  void Raise() { flag_.set_value(); }

  // Wait for this flag to be raised. Threads will be blocked until the flag has
  // been raised. Threads that call this method after the flag has been raised
  // will proceed without blocking.//++等待标志raise。线程将被阻塞，直到该flag被raise。线程在raise后调用这个方法将在不阻塞的情况下继续进行。
  //
  // This method can be called concurrently by multiple threads without mutual
  // exclusion.//++此方法可以由多个线程同时调用，而不会相互排斥。
  void Wait() const { future_.get(); }

 private:
  std::promise<void> flag_;
  std::shared_future<void> future_;
};

}  // namespace impl
}  // namespace ycsbr
