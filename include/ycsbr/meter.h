#pragma once

#include <algorithm>
#include <chrono>
#include <numeric>
#include <optional>
#include <vector>

namespace ycsbr {

class FrozenMeter;

class Meter {
 public:
  Meter(size_t num_entries_hint = 100000)   //!构造函数，默认有100000条记录
      : bytes_(0), request_count_(0), record_count_(0) {
    latencies_.reserve(num_entries_hint);
  }

  void Record(std::optional<std::chrono::nanoseconds> run_time, size_t bytes) {   //!记录一个request的运行时间和字节数(默认只有一条record)
    RecordMultipleRecords(run_time, bytes, /*record_count=*/1);
  }

  void RecordMultipleRecords(std::optional<std::chrono::nanoseconds> run_time,    //!记录一个request的运行时间和字节数(默认有多条record)
                             size_t bytes, size_t record_count) {
    if (run_time.has_value()) {
      latencies_.push_back(*run_time);   //!把run_time对象中包含的值加到latencies中
    }
    ++request_count_;     //request_count加1
    bytes_ += bytes;
    record_count_ += record_count;
  }

  size_t RecordCount() const { return record_count_; }
  size_t RequestCount() const { return request_count_; }

  FrozenMeter Freeze() &&;
  static FrozenMeter FreezeGroup(std::vector<Meter> meters);

 private:
  friend class FrozenMeter;
  size_t bytes_;
  // Number of requests processed.
  size_t request_count_;
  // Number of records processed. This differs from `request_count_` when
  // counting scans and bulk loads (there are usually multiple records processed
  // per scan and bulk load request).
  size_t record_count_;  //++在对scan和bulk loads进行计数时，这与“request_count_”不同（每次scan和bulk loads请求通常会处理多个记录）
  std::vector<std::chrono::nanoseconds> latencies_;     //每个request的运行时间
};

class FrozenMeter {
 public:
  FrozenMeter() : bytes_(0), request_count_(0), record_count_(0) {}  //!默认构造函数
  size_t TotalBytes() const { return bytes_; }
  size_t NumRequests() const { return request_count_; }
  size_t NumRecords() const { return record_count_; }

  template <typename Units>
  Units LatencyMin() const {   //!以Units类型返回最小延迟
    return latencies_.empty()
               ? Units(0)
               : std::chrono::duration_cast<Units>(latencies_.front());
  }

  template <typename Units>
  Units LatencyMean() const {  //!以Units类型返回平均延迟
    if (latencies_.empty()) {
      return Units(0);
    }
    auto total = std::accumulate(latencies_.begin(), latencies_.end(),
                                 std::chrono::nanoseconds(0));
    return std::chrono::duration_cast<Units>(total / latencies_.size());
  }

  template <typename Units>
  Units LatencyMax() const {   //!以Units类型返回最大延迟
    return latencies_.empty()
               ? Units(0)
               : std::chrono::duration_cast<Units>(latencies_.back());
  }

  // Returns percentile latency, where `percentile` is a value between 0.0 and
  // 1.0 inclusive (i.e., `percentile = 0.99` represents the 99th percentile).
  template <typename Units>  //!返回百分位数延迟，其中“percentile”是 0.0 到 1.0（含 0.0 和 1.0）之间的值（即“percentile = 0.99”表示第 99 个百分位数）
  Units LatencyPercentile(double percentile) const {
    if (percentile > 1.0 || percentile < 0.0) {
      throw std::invalid_argument(
          "Percentile out of range (must be between 0.0 and 1.0 inclusive).");
    }
    if (latencies_.empty()) {
      return Units(0);
    }
    size_t index = percentile * latencies_.size();
    if (index == latencies_.size()) {
      --index;
    }
    return std::chrono::duration_cast<Units>(latencies_.at(index));
  }

 private:
  friend class Meter;  //友元类

  // REQUIRES: `meter.latencies_` is in ascending order.
  FrozenMeter(Meter meter)   //!要求：`meter.latency_` 按升序排列，构造函数（形参是Meter类型）
      : FrozenMeter(meter.bytes_, meter.request_count_, meter.record_count_,
                    std::move(meter.latencies_)) {}

  // REQUIRES: `latencies` is in ascending order.
  FrozenMeter(size_t bytes, size_t request_count, size_t record_count,  //!要求：`latencies` 按升序排列，构造函数(形参是成员变量)
              std::vector<std::chrono::nanoseconds> latencies)
      : bytes_(bytes),
        request_count_(request_count),
        record_count_(record_count),
        latencies_(std::move(latencies)) {}

  const size_t bytes_;
  const size_t request_count_;
  const size_t record_count_;
  const std::vector<std::chrono::nanoseconds> latencies_;
};

inline FrozenMeter Meter::Freeze() && {   //! FrozenMeter的构造函数， && 表示这是一个右值引用版本的函数，它表示它只能被右值对象调用
  std::sort(latencies_.begin(), latencies_.end());
  return FrozenMeter(std::move(*this));   
}

inline FrozenMeter Meter::FreezeGroup(std::vector<Meter> meters) {  //!FrozenMeter的构造函数,FreezeGroup
  std::vector<std::chrono::nanoseconds> all_latencies;
  size_t request_count = 0;
  size_t record_count = 0;
  size_t bytes = 0;

  size_t total_size = 0;
  for (const auto& meter : meters) {
    total_size += meter.latencies_.size();
    request_count += meter.request_count_;
    record_count += meter.record_count_;
    bytes += meter.bytes_;
  }
  all_latencies.reserve(total_size);

  for (const auto& meter : meters) {
    all_latencies.insert(all_latencies.end(), meter.latencies_.begin(),
                         meter.latencies_.end());    
  }
  std::sort(all_latencies.begin(), all_latencies.end());

  return FrozenMeter(bytes, request_count, record_count,   
                     std::move(all_latencies));
}

}  // namespace ycsbr
