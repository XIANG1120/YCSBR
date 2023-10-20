#pragma once

#include <memory>
#include <vector>

#include "benchmark_result.h"
#include "impl/thread_pool.h"
#include "run_options.h"
#include "trace.h"

namespace ycsbr {

template <class DatabaseInterface>
class Session {
 public:
  // Starts a benchmark session that will run workloads with `num_threads`
  // threads. If a core map is provided, the threads will be pinned to the cores
  // specified in `core_map`. All worker threads will call
  // `DatabaseInterface::InitializeWorker()` when they start up. After the
  // worker threads start up, `DatabaseInterface::InitializeDatabase()` will be
  // called once.//!启动一个基准测试会话，该会话将使用“num_threads”线程运行工作负载。 //如果提供了核心映射，线程将被固定到“core_map”中指定的核心。 //所有工作线程在启动时都会调用DatabaseInterface::InitializeWorker()。 //工作线程启动后，“DatabaseInterface::InitializeDatabase()”将被调用一次。
  Session(size_t num_threads,
          const std::vector<size_t>& core_map = std::vector<size_t>());

  // Calls `DatabaseInterface::InitializeDatabase()` on a single worker thread.
  // This must be called before any of the Replay/Run methods. This method
  // should also only be called at most once.//!在单个工作线程上调用“DatabaseInterface::InitializeDatabase()”。//这必须在任何 Replay/Run 方法之前调用。 此方法也最多只能调用一次。
  void Initialize();

  // If `Initialize()` was called, `DatabaseInterface::DeleteDatabase()` will be
  // called. Then, this method terminates the worker threads. All worker threads
  // will call `DatabaseInterface::ShutdownWorker()` before terminating. Once a
  // session has been terminated, it cannot be restarted.//!如果调用了“Initialize()”，则会调用“DatabaseInterface::DeleteDatabase()”，该方法终止工作线程。 //所有工作线程在终止之前都会调用“DatabaseInterface::ShutdownWorker()”。 会话一旦终止，就无法重新启动。
  void Terminate();

  ~Session();
  Session(Session&&) = default; //!Session(Session&&) = default; 是一个移动构造函数的声明，并且使用了 = default 来表示该构造函数是默认生成的。//移动构造函数是 C++11 中引入的特性，用于在对象之间进行高效的资源转移，避免不必要的拷贝开销。//移动构造函数使用右值引用（&&）来接收要移动的对象，通过将资源的所有权从源对象转移到目标对象，从而避免复制资源的操作。//一旦类中定义了移动构造函数，编译器将不再生成默认的拷贝构造函数。
  Session& operator=(Session&&) = default;//!Session& operator=(Session&&) = default; 是一个移动赋值运算符的声明，并且使用了 = default 来表示该赋值运算符是默认生成的。//这个特殊的语法用于告诉编译器生成一个默认的移动赋值运算符“=”。

  // Retrieve the underlying `DatabaseInterface` for use (e.g., calling custom
  // methods).//!检索底层“DatabaseInterface”以供使用（例如，调用自定义方法）。
  DatabaseInterface& db();
  const DatabaseInterface& db() const;

  // Replays the provided bulk load trace. Note that bulk loads always run on
  // one thread.//!重播提供的批量load跟踪。 请注意，批量加载始终在一个线程上运行。
  BenchmarkResult ReplayBulkLoadTrace(const BulkLoadTrace& load);

  // Replays the provided trace. The trace's requests will be split among all
  // the worker threads. //!重播提供的trace。 trace的requests将在所有工作线程之间分配。
  BenchmarkResult ReplayTrace(const Trace& trace,
                              const RunOptions& options = RunOptions());

  // Runs a custom workload against the database.//!针对数据库运行自定义工作负载
  template <class CustomWorkload>
  BenchmarkResult RunWorkload(const CustomWorkload& workload,
                              const RunOptions& options = RunOptions());

 private:
  DatabaseInterface db_;
  std::unique_ptr<impl::ThreadPool> threads_;   //num_threads个数的线程
  size_t num_threads_;
  bool initialized_;
};

}  // namespace ycsbr

#include "impl/session-inl.h"
