#pragma once

#include <chrono>
#include <cstdlib>
#include <optional>
#include <vector>

#include "../benchmark_result.h"
#include "../meter.h"

namespace ycsbr {
namespace impl {

class ThroughputSample {
 public:
  ThroughputSample(size_t records_processed, std::chrono::nanoseconds elapsed);

  // Amount of time "captured" by this throughput sample.//++此吞吐量样本“捕获”的时间量
  template <typename Units>
  Units ElapsedTime() const;
  std::chrono::nanoseconds ElapsedTimeNanos() const;

  // Throughput in millions of records processed per second.//++每秒处理数百万条记录的吞吐量。
  double MRecordsPerSecond() const;

  // The number of records processed.   //++处理的记录的数量
  size_t NumRecordsProcessed() const;

 private:
  size_t records_processed_;
  std::chrono::nanoseconds elapsed_;
};

class MetricsTracker {
 public:
  MetricsTracker(size_t num_reads_hint = 100000,
                 size_t num_writes_hint = 100000, size_t num_scans_hint = 1000
                 ,size_t num_deletes_hint = 10000    /////////////////
                 )
      : reads_(num_reads_hint),
        writes_(num_writes_hint),
        scans_(num_scans_hint),
        deletes_(num_delete_hint),   ////////////////////
        failed_reads_(0),
        failed_writes_(0),
        failed_scans_(0),
        failed_deletes_(0),    //////////////////
        read_xor_(0) {}

  void RecordRead(std::optional<std::chrono::nanoseconds> run_time,
                  size_t read_bytes, bool succeeded) {
    if (succeeded) {
      reads_.Record(run_time, read_bytes);   //!调用Meter类型记录一个read request的运行时间和字节数，记录数=1;如果插入失败则不记录
    } else {
      ++failed_reads_;
    }
  }

  void RecordWrite(std::optional<std::chrono::nanoseconds> run_time,
                   size_t write_bytes, bool succeeded) {
    if (succeeded) {
      writes_.Record(run_time, write_bytes);   //!调用Meter类型记录一个write request的运行时间和字节数，记录数=1；如果插入失败则不记录
    } else {
      ++failed_writes_;
    }
  }

  void RecordScan(std::optional<std::chrono::nanoseconds> run_time,
                  size_t scanned_bytes, size_t scanned_amount, bool succeeded) {
    if (succeeded) {
      scans_.RecordMultipleRecords(run_time, scanned_bytes, scanned_amount);     //!调用Meter类型记录一个scan request的运行时间和字节数,记录数；如果插入失败则不记录
    } else {
      ++failed_scans_;
    }
  }

////////////////////
  void RecordDelete(std::optional<std::chrono::nanoseconds> run_time,
                  bool succeeded) {
    if (succeeded) {
      deletes_.Record(run_time, 0);    //没有写，也没有读任何字节
    }else{
      ++failed_deletes_;
    }
  }
////////////////////
  void SetReadXOR(uint32_t value) { read_xor_ = value; }

  ThroughputSample GetSample() {   //!返回完成的一个吞吐量样本（从上次取样开始，到现在）
    const auto now = std::chrono::steady_clock::now();
    const size_t count = TotalRequestCount();
    const ThroughputSample result(count - last_count_, now - last_sample_time_);
    last_count_ = count;
    last_sample_time_ = now;
    return result;
  }

  void ResetSample() {   //!将此时刻设为取样开始点
    last_count_ = TotalRequestCount();
    last_sample_time_ = std::chrono::steady_clock::now();
  }

  BenchmarkResult Finalize(std::chrono::nanoseconds total_run_time) {   //!构造一个benchmarkresult
    return BenchmarkResult(
        total_run_time, read_xor_, std::move(reads_).Freeze(),
        std::move(writes_).Freeze(), std::move(scans_).Freeze(), 
        std::move(deletes_).Freeze(),failed_deletes_,    ///////////////////////
        failed_reads_,
        failed_writes_, failed_scans_);
  }

  static BenchmarkResult FinalizeGroup(std::chrono::nanoseconds total_run_time,
                                       std::vector<MetricsTracker> trackers) {    //!构造一个benchmarkresult，将多个MetricsTracker合成一个
    std::vector<Meter> reads, writes, scans;
    /////////////////
    std::vector<Meter> deletes;
    size_t failed_deletes_ = 0;
    /////////////////
    size_t failed_reads = 0, failed_writes = 0, failed_scans = 0;
    uint32_t read_xor = 0;
    reads.reserve(trackers.size());
    writes.reserve(trackers.size());
    scans.reserve(trackers.size());
    deletes.reserve(trackers.size());     ////////////////////

    for (auto& tracker : trackers) {
      reads.emplace_back(std::move(tracker.reads_));
      writes.emplace_back(std::move(tracker.writes_));
      scans.emplace_back(std::move(tracker.scans_));
      deletes.emplce_back(std::move(tracker.deletes_));   /////////////////////
      read_xor ^= tracker.read_xor_;
      failed_reads += tracker.failed_reads_;
      failed_writes += tracker.failed_writes_;
      failed_scans += tracker.failed_scans_;
      failed_deletes_ += tracker.failed_deletes_;  ////////////////////////////
    }

    return BenchmarkResult(total_run_time, read_xor,
                           Meter::FreezeGroup(std::move(reads)),
                           Meter::FreezeGroup(std::move(writes)),
                           Meter::FreezeGroup(std::move(scans)),
                           Meter::FreezeGroup(std::move(deletes)),failed_deletes_,   ////////////////////////
                           failed_reads,
                           failed_writes, failed_scans);
  }

 private:
  size_t TotalRequestCount() const {   //!返回之前的所有处理过的request数（不管失败还是成功）
    return reads_.RequestCount() + writes_.RequestCount() +
           scans_.RequestCount() + 
           deletes_.RequestCount() + failed_deletes_ +   //////////////////
           failed_reads_ + failed_writes_ +
           failed_scans_;
  }

  Meter reads_, writes_, scans_;
  Meter deletes_;   ///////////////
  size_t failed_reads_, failed_writes_, failed_scans_;
  size_t failed_deletes_;   ////////////////
  uint32_t read_xor_;

  size_t last_count_;
  std::chrono::steady_clock::time_point last_sample_time_;   //?std::chrono::steady_clock::time_point 是C++标准库中用于表示时间点（时间戳）的类型
};

// Additional implementation details follow.

inline ThroughputSample::ThroughputSample(size_t records_processed,
                                          std::chrono::nanoseconds elapsed)
    : records_processed_(records_processed), elapsed_(elapsed) {}

inline double ThroughputSample::MRecordsPerSecond() const {
  return records_processed_ /
         // Converts the elapsed time into microseconds, represented using a
         // double (to account for fractional microseconds).//++将经过的时间转换为微秒，使用双精度数表示（以考虑小数微秒）
         std::chrono::duration_cast<std::chrono::duration<double, std::micro>>(
             elapsed_)
             .count();
}

template <typename Units>
inline Units ThroughputSample::ElapsedTime() const {   //将elapsed_转换为Units类型
  return std::chrono::duration_cast<Units>(elapsed_);
}

inline std::chrono::nanoseconds ThroughputSample::ElapsedTimeNanos() const {   //将elapsed_转换为std::chrono::nanoseconds类型
  return ElapsedTime<std::chrono::nanoseconds>();
}

inline size_t ThroughputSample::NumRecordsProcessed() const {    
  return records_processed_;
}

}  // namespace impl
}  // namespace ycsbr
