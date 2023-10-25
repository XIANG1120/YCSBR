#pragma once

#include <atomic>
#include <chrono>
#include <fstream>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "../request.h"
#include "../run_options.h"
#include "flag.h"
#include "tracking.h"

namespace ycsbr {
namespace impl {

template <class DatabaseInterface, typename WorkloadProducer>
class Executor {
 public:
  Executor(DatabaseInterface* db, WorkloadProducer producer, size_t id,
           const Flag* can_start, const RunOptions& options);    //!构造函数

  Executor(const Executor&) = delete;  //拷贝构造函数被删除
  Executor& operator=(const Executor&) = delete;    //赋值运算符被删除

  // Runs the workload produced by the producer.   //++运行producer产生的工作负载
  void operator()();

  void WaitForReady() const;
  void WaitForCompletion() const;
  MetricsTracker&& GetResults() &&;

  // Meant for use by YCSBR's internal microbenchmarks.
  void BM_WorkloadLoop();

 private:
  void WorkloadLoop();
  void SetupOutputFileIfNeeded();

  Flag ready_;   
  const Flag* can_start_;
  Flag done_;

  DatabaseInterface* db_;
  WorkloadProducer producer_;
  MetricsTracker tracker_;
  size_t id_;

  const RunOptions options_;
  size_t latency_sampling_counter_;   //延迟样本计数
  size_t throughput_sampling_counter_;    //吞吐量样本计数

  // Used to print out throughput samples, if requested. //++如果需要的话，用来打印输出吞吐量样本
  std::ofstream throughput_output_file_;
};

// Implementation details follow.

template <class DatabaseInterface, typename WorkloadProducer>
inline Executor<DatabaseInterface, WorkloadProducer>::Executor(   //!构造函数的实现
    DatabaseInterface* db, WorkloadProducer producer, const size_t id,
    const Flag* can_start, const RunOptions& options)
    : ready_(),
      can_start_(can_start),
      done_(),
      db_(db),
      producer_(std::move(producer)),
      tracker_(),
      id_(id),
      options_(options),
      latency_sampling_counter_(0),
      throughput_sampling_counter_(0),
      throughput_output_file_() {}

template <class DatabaseInterface, typename WorkloadProducer>
inline void Executor<DatabaseInterface, WorkloadProducer>::WaitForReady()   //!等待，直到准备完成
    const {
  return ready_.Wait();   
}

template <class DatabaseInterface, typename WorkloadProducer>
inline void Executor<DatabaseInterface, WorkloadProducer>::WaitForCompletion()    //!等待，直到完成
    const {
  done_.Wait();    
}

template <class DatabaseInterface, typename WorkloadProducer>
inline MetricsTracker&&
Executor<DatabaseInterface, WorkloadProducer>::GetResults() && {    //!从MetricsTracker实例中获取结果
  WaitForCompletion();     //先等待完成
  return std::move(tracker_);     //再移动结果
}

template <typename Callable>
inline std::optional<std::chrono::nanoseconds> MeasurementHelper(     //!测量callable()函数运行时间的辅助函数
    Callable&& callable, bool measure_latency) {
  if (!measure_latency) {   //如果不测量则直接返回
    callable();
    return std::optional<std::chrono::nanoseconds>();
  }
  //选择测量
  const auto start = std::chrono::steady_clock::now();
  callable();
  const auto end = std::chrono::steady_clock::now();
  return end - start;
}

template <class DatabaseInterface, typename WorkloadProducer>
inline void Executor<DatabaseInterface, WorkloadProducer>::operator()() {      //!每个线程都运行
  // Run any needed preparation code.  //++运行任何需要的准备代码
  producer_.Prepare();  //*初始化phase_和insert_keys_

  // Sets up the throughput sample output file, if needed.  //++如果需要的话，设置吞吐量样本输出文件
  SetupOutputFileIfNeeded();

  // Now ready to proceed; wait until we're told to start.  //++现在准备继续；等待直到我们被告知开始
  ready_.Raise();    //告诉主线程：已经完成了ready工作
  can_start_->Wait();   //等待，直到主线程发送了可以开始执行任务的命令

  // Run the job.  //++运行作业
  WorkloadLoop();

  // Notify others that we are done.   //++通知主线程：我完成了
  done_.Raise();
}

template <class DatabaseInterface, typename WorkloadProducer>
inline void
Executor<DatabaseInterface, WorkloadProducer>::SetupOutputFileIfNeeded() {
  if (options_.throughput_sample_period == 0) return;
  const auto filename =
      options_.output_dir /
      (options_.throughput_output_file_prefix + std::to_string(id_) + ".csv");
  throughput_output_file_ = std::ofstream(filename);
  if (throughput_output_file_.fail()) {
    throw std::invalid_argument("Failed to create output file: " +
                                filename.string());
  }
  throughput_output_file_ << "mrecords_per_s,elapsed_ns" << std::endl;
}

template <class DatabaseInterface, typename WorkloadProducer>
inline void Executor<DatabaseInterface, WorkloadProducer>::WorkloadLoop() {   //!很重要的执行工作负载函数----------------------------------------------------------
  // Initialize state needed for the replay.   //++初始化重播所需的状态
  uint32_t read_xor = 0;
  std::string value_out;
  std::vector<std::pair<Request::Key, std::string>> scan_out;

  tracker_.ResetSample();  //吞吐量采样开始
   std::cerr <<"WorkloadLoop执行中..." <<std::endl;   ///////////////////////////

  // Run our trace slice.
  while (producer_.HasNext()) {
    const auto& req = producer_.Next();
    // std::cerr << "拿到request了" << std::endl;      /////////////////////////////
    bool measure_latency = false;
    if (++latency_sampling_counter_ >= options_.latency_sample_period) {  //每十个request测量一次request的run_time
      measure_latency = true;
      latency_sampling_counter_ = 0;
    }

    switch (req.op) {
      case Request::Operation::kRead:
      case Request::Operation::kNegativeRead: {    //!request为读操作
        bool succeeded = false;
        value_out.clear();
        const auto run_time = MeasurementHelper(
            [this, &req, &value_out, &read_xor, &succeeded]() {
              succeeded = db_->Read(req.key, &value_out);    //参数为key和&value
              if (succeeded) {
                // Force a read of the extracted value. We want to count this
                // time against the read latency too.//++强制读取提取的值。 我们也想将这个时间计入读取延迟。
                read_xor ^=
                    *reinterpret_cast<const uint32_t*>(value_out.c_str());
              }
            },
            measure_latency);
        tracker_.RecordRead(run_time, value_out.size(), succeeded);   //如果measure_latency为false的话，run_time是空的
        if (!succeeded && options_.expect_request_success) {   //如果不成功但是参数中设置了request必须成功，则抛出错误
          throw std::runtime_error(
              "Failed to read a key that was expected to be found.");
        }
        break;
      }

      ///////////////////////////
      case Request::Operation::kDelete: {     //!request为删除操作
        bool succeeded = false;
        const auto run_time = MeasurementHelper(
            [this, &req, &succeeded]() {
              succeeded = db_->Delete(req.key);  
            },
            measure_latency);
        tracker_.RecordDelete(run_time, succeeded);
        if (!succeeded && options_.expect_request_success) {
          throw std::runtime_error(
              "Failed to delete a record (expected to succeed).");
        }
        break; 
      }
      ///////////////////////////

      case Request::Operation::kInsert: {    //!request为插入操作
        // Inserts count the whole record size, since this should be the first
        // time the entire record is written to the DB.
        bool succeeded = false;
        const auto run_time = MeasurementHelper(
            [this, &req, &succeeded]() {
              succeeded = db_->Insert(req.key, req.value, req.value_size);  //参数为key,value,value.size
            },
            measure_latency);
        tracker_.RecordWrite(run_time, req.value_size + sizeof(req.key),    //记录key和value的大小
                             succeeded);
        if (!succeeded && options_.expect_request_success) {
          throw std::runtime_error(
              "Failed to insert a record (expected to succeed).");
        }
        break;
      }

      case Request::Operation::kUpdate: {     //!request为更新操作
        // Updates only record the value size, since the key should already
        // exist in the DB.
        bool succeeded = false;
        const auto run_time = MeasurementHelper(
            [this, &req, &succeeded]() {
              succeeded = db_->Update(req.key, req.value, req.value_size);  //参数为key,value,value.size
            },
            measure_latency);
        tracker_.RecordWrite(run_time, req.value_size, succeeded);   //记录value大小
        if (!succeeded && options_.expect_request_success) {
          throw std::runtime_error(
              "Failed to update a record (expected to succeed).");
        }
        break;
      }

      case Request::Operation::kScan: {     //!request为扫描操作
        bool succeeded = false;
        scan_out.clear();
        scan_out.reserve(req.scan_amount);
        const auto run_time = MeasurementHelper(
            [this, &req, &scan_out, &read_xor, &succeeded]() {
              succeeded = db_->Scan(req.key, req.scan_amount, &scan_out);
              if (succeeded && scan_out.size() > 0) {
                // Force a read of the first extracted value. We want to count
                // this time against the read latency too.//++强制读取第一个提取的值。 我们也想将这个时间计入读取延迟
                read_xor ^= *reinterpret_cast<const uint32_t*>(
                    scan_out.front().second.c_str());
              }
            },
            measure_latency);
        size_t scanned_bytes = 0;
        for (const auto& entry : scan_out) {
          scanned_bytes += sizeof(entry.first) + entry.second.size();  //记录所有的key的大小+value的大小
        }
        tracker_.RecordScan(run_time, scanned_bytes, scan_out.size(),
                            succeeded);
        if (!succeeded && options_.expect_request_success) {
          throw std::runtime_error(
              "Failed to run a range scan (expected to succeed).");
        }
        if (options_.expect_scan_amount_found &&    //参数设置成期待scan全都能找到，但是并没有，则抛出错误
            scan_out.size() < req.scan_amount) {
          throw std::runtime_error(
              "A range scan returned too few (or too many) records.");
        }
        break;
      }

      case Request::Operation::kReadModifyWrite: {      //!request为read_modify_write操作
        bool succeeded = false;

        // First, do the read.
        const auto read_run_time = MeasurementHelper(
            [this, &req, &value_out, &read_xor, &succeeded]() {
              // Do the read.
              succeeded = db_->Read(req.key, &value_out);   //先读
              if (!succeeded) return;
              // Force a read of the extracted value. We want to count this
              // time against the read latency too.
              read_xor ^= *reinterpret_cast<const uint32_t*>(value_out.c_str());
            },
            measure_latency);
        tracker_.RecordRead(read_run_time, value_out.size(), succeeded);
        if (!succeeded && options_.expect_request_success) {
          throw std::runtime_error(
              "Failed to read a record during a read-modify-write (expected to "
              "succeed).");
        }
        // Skip the write if the read failed.
        if (!succeeded) break;

        // Now do the write.
        const auto write_run_time = MeasurementHelper(
            [this, &req, &succeeded]() {
              succeeded = db_->Update(req.key, req.value, req.value_size);   //再更新
            },
            measure_latency);
        tracker_.RecordWrite(write_run_time, req.value_size, succeeded);
        if (!succeeded && options_.expect_request_success) {
          throw std::runtime_error(
              "Failed to update a record during a read-modify-write (expected "
              "to succeed).");
        }
        break;
      }

      default:
        throw std::runtime_error("Unrecognized request operation!");   //无法识别的请求
    }

    if (options_.throughput_sample_period > 0 &&   //如果参数中设置了吞吐量采样时间间隔且大于0，则每隔options_.throughput_sample_period个request，采样一次吞吐量到throughput_output_file_文件中
        ++throughput_sampling_counter_ >= options_.throughput_sample_period) {
      auto sample = tracker_.GetSample();
      throughput_output_file_ << sample.MRecordsPerSecond() << ","
                              << sample.ElapsedTimeNanos().count() << std::endl;
      throughput_sampling_counter_ = 0;
    }
  }
  // Used to prevent optimizing away reads.//++用于防止优化流失读取？
  tracker_.SetReadXOR(read_xor);
}

template <class DatabaseInterface, typename WorkloadProducer>
inline void Executor<DatabaseInterface, WorkloadProducer>::BM_WorkloadLoop() {
  WorkloadLoop();
}

}  // namespace impl
}  // namespace ycsbr
