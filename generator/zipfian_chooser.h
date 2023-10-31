#pragma once

#include <cassert>
#include <cmath>
#include <cstdint>
#include <random>

#include "hash.h"
#include "ycsbr/gen/chooser.h"

namespace ycsbr {
namespace gen {

// Returns Zipfian-distributed values in the range [0, item_count). This
// implementation is based on the YCSB driver's Zipfian implementation, which in
// turn uses the algorithm presented in
//   J. Gray et al. Quickly generating billion-record synthetic databases. In
//   SIGMOD'94.  //++返回 [0, item_count) 范围内的 Zipfian 分布values。 此实现基于 YCSB 驱动程序的 Zipfian 实现，后者又使用 J. Gray 等人中提出的算法。 快速生成数十亿条记录的综合数据库。 在 SIGMOD'94 中。
class ZipfianChooser : public Chooser {
 public:
  // The value of `theta` must be in the exclusive range (0, 1). //++theta的值必须在(0,1)之间
  ZipfianChooser(size_t item_count, double theta);  

  // Get a sample from the distribution. The returned value will be in the range
  // [0, item_count). Note that index 0 will be the most popular, followed by
  // index 1, and so on. //!从分布中获取一个样本。 返回的值将在 [0, item_count) 范围内。 请注意，索引 0 最受欢迎，其次是索引 1，依此类推。
  size_t Next(PRNG& prng) override;

  // This requires some computation and can be slow if `delta` is large.//++这需要一些计算，并且如果“delta”很大的话可能会很慢。
  void IncreaseItemCountBy(size_t delta) override;      

  // Will recompute constants for `new_item_count`. 
  void SetItemCount(size_t new_item_count) override;  //!将重新设置item_count_

 protected:
  size_t item_count() const;

 private:
  static double ComputeZetaN(size_t item_count, double theta,
                             size_t prev_item_count = 0,
                             double prev_zeta_n = 0.0);
  // Computes `zeta(n)`, using previously cached values if possible.//++计算 `zeta(n)`，如果可能的话使用之前缓存的值。

  ////////////////////////////
  static double ComputeZetaNForDecrease(const size_t item_count,    
                                           const double theta,
                                           const size_t prev_item_count,
                                           const double prev_zeta_n);
  ///////////////////////////
  void UpdateZetaNWithCaching();
  void UpdateETA();

  size_t item_count_;
  double theta_;
  double alpha_;
  double thres_;
  double zeta2theta_;
  double zeta_n_;
  double eta_;

  std::uniform_real_distribution<double> dist_;
};

// Returns Zipfian-distributed values in the range [0, item_count), but ensuring
// that the popular values are scattered throughout the range.  //++返回范围[0，item_count）中的Zipfian分布值，但要确保流行值分散在整个范围内
class ScatteredZipfianChooser : public ZipfianChooser {    //继承自ZipfianChooser
 public:
  // Chooser instances with the same `scatter_salt` will choose the same hot
  // keys. Set `scatter_salt` to change the "hot" keys.//++具有相同“scatter_salt”的选择器实例将选择相同的热键。 设置“scatter_salt”来更改“热”键。
  ScatteredZipfianChooser(size_t item_count, double theta,
                          uint64_t scatter_salt = 0);
  size_t Next(PRNG& prng) override;

 private:
  uint64_t scatter_salt_;
};

// Implementation details follow.

inline ZipfianChooser::ZipfianChooser(const size_t item_count,  //!ZipfianChooser构造函数的实现
                                      const double theta)
    : item_count_(item_count),
      theta_(theta),
      alpha_(1.0 / (1.0 - theta)),
      thres_(1.0 + std::pow(0.5, theta)),
      zeta2theta_(ComputeZetaN(2, theta)),
      zeta_n_(0.0),
      eta_(0.0),
      dist_(0.0, 1.0) {
  UpdateZetaNWithCaching();
  UpdateETA();
}

inline ScatteredZipfianChooser::ScatteredZipfianChooser(    //!ScatteredZipfianChooser构造函数的实现
    const size_t item_count, const double theta, const uint64_t scatter_salt)
    : ZipfianChooser(item_count, theta), scatter_salt_(scatter_salt) {}

inline size_t ZipfianChooser::Next(PRNG& prng) {
  const double u = dist_(prng);
  const double uz = u * zeta_n_;
  if (uz < 1.0) return 0;
  if (uz < thres_) return 1;
  return static_cast<size_t>(item_count_ *
                             std::pow(eta_ * u - eta_ + 1, alpha_));
}

inline size_t ScatteredZipfianChooser::Next(PRNG& prng) {
  // Most of the generator code assumes that we're running on a 64-bit system.
  static_assert(sizeof(uint64_t) == sizeof(size_t));
  const uint64_t hashed_choice =
      FNVHash64(ZipfianChooser::Next(prng) ^ scatter_salt_);
#ifdef __SIZEOF_INT128__
  // Fast modulo for 64-bit integers. See
  // https://lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/
  return static_cast<uint64_t>((static_cast<__uint128_t>(hashed_choice) *
                                static_cast<__uint128_t>(item_count())) >>
                               64);
#else
  return hashed_choice % item_count();
#endif
}

inline void ZipfianChooser::IncreaseItemCountBy(const size_t delta) {   //!item_count增加delta，并重新计算zeta_n_和eta   
  const size_t prev_item_count = item_count_;
  const double prev_zeta_n = zeta_n_;
  //////////////////////////
    size_t c = 1 ;
    if (delta != c ) {
      item_count_ -= 1;
      zeta_n_ = ComputeZetaNForDecrease(item_count_, theta_, prev_item_count, prev_zeta_n);
      UpdateETA();
    }else{
    //////////////////////////
    item_count_ += delta;
    zeta_n_ = ComputeZetaN(item_count_, theta_, prev_item_count, prev_zeta_n);
    UpdateETA();
    }  ////////////////////////
}

inline void ZipfianChooser::SetItemCount(const size_t new_item_count) {   //!重新设置item_count为new_item_count，并用缓存查找/更新zeta_n_，更新eta_
  assert(new_item_count > 0);
  item_count_ = new_item_count;
  UpdateZetaNWithCaching();
  UpdateETA();
}

inline size_t ZipfianChooser::item_count() const { return item_count_; }

inline double ZipfianChooser::ComputeZetaN(const size_t item_count,    //!计算zeta_n_
                                           const double theta,
                                           const size_t prev_item_count,
                                           const double prev_zeta_n) {
  assert(item_count > prev_item_count);
  size_t item_count_so_far = prev_item_count;
  double zeta_so_far = prev_zeta_n;
  for (; item_count_so_far < item_count; ++item_count_so_far) {
    zeta_so_far +=
        1.0 / std::pow(static_cast<double>(item_count_so_far + 1), theta);
  }
  return zeta_so_far;
}

inline void ZipfianChooser::UpdateETA() {    //!计算eta_
  eta_ = (1 - std::pow(2.0 / item_count_, 1.0 - theta_)) /
         (1.0 - zeta2theta_ / zeta_n_);
}

////////////////////////////////////
inline double ZipfianChooser::ComputeZetaNForDecrease(const size_t item_count,    
                                           const double theta,
                                           const size_t prev_item_count,
                                           const double prev_zeta_n) {
  assert(item_count < prev_item_count);
  size_t item_count_so_far = item_count;
  double zeta_so_far = prev_zeta_n;
  for (; item_count_so_far < prev_item_count; ++item_count_so_far) {
    zeta_so_far -=
        1.0 / std::pow(static_cast<double>(item_count_so_far + 1), theta);
  }
  return zeta_so_far;
}
/////////////////////////////////////

}  // namespace gen
}  // namespace ycsbr
