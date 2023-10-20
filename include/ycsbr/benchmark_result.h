#pragma once

#include <chrono>
#include <iostream>

#include "meter.h"

namespace ycsbr {

class BenchmarkResult {
 public:
  BenchmarkResult(std::chrono::nanoseconds total_run_time);  //!std::chrono::nanoseconds 是 C++ 标准库中的一个时间单位，用于表示纳秒（nanoseconds）级别的时间间隔
  BenchmarkResult(std::chrono::nanoseconds total_run_time, uint32_t read_xor,
                  FrozenMeter reads, FrozenMeter writes, FrozenMeter scans,
                  FrozenMeter deletes, size_t failed_deletes,  ///////////////////////
                  size_t failed_reads, size_t failed_writes,
                  size_t failed_scans);

  template <typename Units>
  Units RunTime() const;

  double ThroughputThousandRequestsPerSecond() const;  //req/s
  double ThroughputThousandRecordsPerSecond() const;   //record/s
  double ThroughputReadMiBPerSecond() const;
  double ThroughputWriteMiBPerSecond() const;

  const FrozenMeter& Reads() const { return reads_; }
  const FrozenMeter& Writes() const { return writes_; }
  const FrozenMeter& Scans() const { return scans_; }
  const FrozenMeter& Deletes() const {return deletes_; }   /////////////////////

  size_t NumFailedReads() const { return failed_reads_; }
  size_t NumFailedWrites() const { return failed_writes_; }
  size_t NumFailedScans() const { return failed_scans_; }
  size_t NumFailedDeletes() const { return failed_deletes_; }    ////////////////////////

  static void PrintCSVHeader(std::ostream& out);
  void PrintAsCSV(std::ostream& out, bool print_header = true) const;

 private:
  friend std::ostream& operator<<(std::ostream& out,
                                  const BenchmarkResult& res);   //友元函数
  const std::chrono::nanoseconds run_time_;
  const FrozenMeter reads_, writes_, scans_;
  const FrozenMeter deletes_; const size_t failed_deletes_; ////////////////////
  const size_t failed_reads_, failed_writes_, failed_scans_;
  const uint32_t read_xor_;
};

std::ostream& operator<<(std::ostream& out, const BenchmarkResult& res);

}  // namespace ycsbr

#include "impl/benchmark_result-inl.h"
