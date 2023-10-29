#include <cassert>
#include <chrono>
#include <memory>
#include <stdexcept>
#include <thread>

#include "../meter.h"
#include "../trace_workload.h"
#include "executor.h"

namespace ycsbr {

template <class DatabaseInterface> //C++模板类Session的构造函数的实现
inline Session<DatabaseInterface>::Session(size_t num_threads,
                                           const std::vector<size_t>& core_map)
    : threads_(core_map.size() == num_threads
                   ? (std::make_unique<impl::ThreadPool>(
                         num_threads, core_map,
                         [this]() {
                           db_.InitializeWorker(std::this_thread::get_id());
                         },
                         [this]() {
                           db_.ShutdownWorker(std::this_thread::get_id());
                         }))
                   : (std::make_unique<impl::ThreadPool>(
                         num_threads,   //第一个参数
                         [this]() {
                           db_.InitializeWorker(std::this_thread::get_id());
                         },       //第二个参数,每个线程最开始都要执行,将统计信息置0
                         [this]() {       //第三个参数，每个线程结束时都要执行，将统计信息汇总
                           db_.ShutdownWorker(std::this_thread::get_id());         
                         }))),//初始化session类中的threads_成员变量，选择性地创建了一个impl::ThreadPool的实例并将其分配给threads_，impl::ThreadPool是一个类
      num_threads_(num_threads),
      initialized_(false) {
  if (num_threads == 0) {
    throw std::invalid_argument("Must use at least 1 thread.");
  }
}

template <class DatabaseInterface>
inline Session<DatabaseInterface>::~Session() {
  Terminate();
}

template <class DatabaseInterface>
inline void Session<DatabaseInterface>::Initialize() {  //初始化数据库
  if (initialized_ || threads_ == nullptr) return;
  threads_->Submit([this]() { db_.InitializeDatabase(); }).get();  
  initialized_ = true;
}

template <class DatabaseInterface>
inline void Session<DatabaseInterface>::Terminate() {
  if (threads_ == nullptr) return;
  if (initialized_) {
    threads_->Submit([this]() { db_.ShutdownDatabase(); }).get();  //关闭数据库
  }
  threads_.reset(nullptr);  //将线程池指针重置为空，线程池自动析构
}

template <class DatabaseInterface>
inline DatabaseInterface& Session<DatabaseInterface>::db() { 
  return db_;
}

template <class DatabaseInterface>
inline const DatabaseInterface& Session<DatabaseInterface>::db() const {  
  return db_;
}

template <class DatabaseInterface>
inline BenchmarkResult Session<DatabaseInterface>::ReplayBulkLoadTrace(
    const BulkLoadTrace& load) {
  std::chrono::steady_clock::time_point start, end;
  threads_
      ->Submit([this, &load, &start, &end]() {
        start = std::chrono::steady_clock::now();
        db_.BulkLoad(load);
        end = std::chrono::steady_clock::now();
      })
      .get();

  const auto run_time = end - start;
  Meter load_meter;
  load_meter.RecordMultipleRecords(run_time, load.DatasetSizeBytes(), load.size());
  return BenchmarkResult(run_time, 0, FrozenMeter(),
                         std::move(load_meter).Freeze(), FrozenMeter(),
                         FrozenMeter(), 0,      ///////////////////////////////
                         0, 0,
                         0);
}

template <class DatabaseInterface>
inline BenchmarkResult Session<DatabaseInterface>::ReplayTrace(
    const Trace& trace, const RunOptions& options) {
  const TraceWorkload workload(&trace);
  return RunWorkload<TraceWorkload>(workload, options);
}

template <class DatabaseInterface>
template <class CustomWorkload>
inline BenchmarkResult Session<DatabaseInterface>::RunWorkload(
    const CustomWorkload& workload, const RunOptions& options) {
  using Runner =
      impl::Executor<DatabaseInterface, typename CustomWorkload::Producer>;

  auto producers = workload.GetProducers(num_threads_);   //*返回一个Producer容器，里面有num_threads_个producer
  assert(producers.size() == num_threads_);  

  impl::Flag can_start;
  std::vector<std::unique_ptr<Runner>> executors;
  executors.reserve(num_threads_);   //预留num_threads_个位置存放Runners指针

  size_t executor_id = 0;
  // std::cerr << "RunWorkload执行中..." <<std::endl;  /////////////////////////
  for (auto& producer : producers) {
    executors.push_back(std::make_unique<Runner>(
        &db_, std::move(producer), executor_id++, &can_start, options));  //*初始化Runner,并将其装入executors,producer和Runner一对一（id相同）
    threads_->SubmitNoWait([exec = executors.back().get()]() { (*exec)(); });   //向线程池提交任务
  }

  // Wait for the executors to finish performing their startup work.
  for (const auto& executor : executors) {    //等所有的线程完成准备工作
    executor->WaitForReady();
  }

  /////////////////////////
  //清空load_keys_
  executors[0]->GetProducer().GetLoadKeys()->clear();
 //将set中的元素移动到load_keys_
  std::move(executors[0]->GetProducer().GetLoadKeysSet()->begin(),executors[0]->GetProducer().GetLoadKeysSet()->end(),std::back_inserter(*(executors[0]->GetProducer().GetLoadKeys())));
  //设置num_load_keys
  executors[0]->GetProducer().SetNumLoadKeys(executors[0]->GetProducer().GetLoadKeys()->size());
  //将load_keys_中的元素排序
  std::sort(executors[0]->GetProducer().GetLoadKeys()->begin(),executors[0]->GetProducer().GetLoadKeys()->end());
  //为每第一个个phase设置itemcount
  size_t size = *(executors[0]->GetProducer().GetNumLoadKeys());
  for ( auto& executor : executors) {
    auto& phase = executor->GetProducer().GetPhases()[0];
      phase.SetItemCount(size + executor->GetProducer().GetNumDeleteKeys());
  }
  }
  ////////////////////////

  // Start the workload and the timer. 
  const auto start = std::chrono::steady_clock::now();
  can_start.Raise();   //告诉所有的线程可以开始运行工作负载了
  for (auto& executor : executors) {   //等待所有的线程都完成
    executor->WaitForCompletion();     
  }
  const auto end = std::chrono::steady_clock::now();

  // Retrieve the results.
  std::vector<impl::MetricsTracker> results;
  results.reserve(num_threads_);
  for (auto& executor : executors) {
    results.emplace_back(std::move(*executor).GetResults());
  }

  return impl::MetricsTracker::FinalizeGroup(end - start, std::move(results));
}

}  // namespace ycsbr
