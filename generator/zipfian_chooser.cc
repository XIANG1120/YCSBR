#include "zipfian_chooser.h"

#include <iterator>
#include <map>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <utility>

namespace {

// A thread-safe `zeta(n)` cache (to reduce recomputation latency for large item
// counts). //++线程安全的“zeta(n)”缓存（以减少大量item_count_的重新计算延迟）。
class ZetaCache {
 public:
  static ZetaCache& Instance() {
    static ZetaCache instance;
    return instance;
  }

  using Theta = double;
  using ItemCount = size_t;
  using ZetaN = double;

  // Finds a `zeta(n)` value for a given `item_count` (or for a smaller
  // `item_count` if the exact `item_count` is not in the cache). //++查找给定“item_count”的“zeta(n)”值（如果缓存中没有确切的“item_count”，则查找更小的“item_count”）。
  std::optional<std::pair<ItemCount, ZetaN>> FindStartingPoint(    //!在缓存中查找theta和item_count有没有对应的zeta(n)
      const size_t item_count, const double theta) const {
    std::unique_lock<std::mutex> lock(mutex_);
    auto theta_map_it = cache_.find(theta);
    if (theta_map_it == cache_.end() || theta_map_it->second.empty()) {   //如果没有找到缓存(第一层map)或缓存为空，则返回空值
      return std::optional<std::pair<ItemCount, ZetaN>>();
    }
    //开始第二层查找
    const auto& theta_map = theta_map_it->second;
    const auto it = theta_map.lower_bound(item_count);  //?lower_bound函数查找不小于给定键的第一个元素的迭代器，没有找到就返回.end
    if (it == theta_map.end()) {   //没找到(第二层map)
      return *std::prev(theta_map.end());   //?将theta_map.end()迭代器逆向移动1位，并取里面的值（返回最后一个元素的值）
    } else {   //找到了(第二层map)
      if (it->first == item_count) {
        // Exact match. //++完全符合
        return *it;
      } else if (it == theta_map.begin()) {
        // No previous values. //++没有以前的值
        return std::optional<std::pair<size_t, double>>();
      } else {
        // Not an exact match, so the starting point should be the first zeta
        // computed with a smaller item count.//++不完全匹配
        return *std::prev(theta_map.end());
      }
    }
  }

  void Add(const size_t item_count, const double theta, const double zeta) {     //!向缓存中加入新项
    std::unique_lock<std::mutex> lock(mutex_);
    // Will create a map for the `theta` value if one does not already exist.
    auto& theta_map = cache_[theta];
    // N.B. If an entry for `item_count` already exists, this insert will be an
    // effective no-op.
    theta_map.insert(std::make_pair(item_count, zeta));
  }

  ZetaCache(ZetaCache&) = delete;    //删除拷贝构造函数
  ZetaCache& operator=(ZetaCache&) = delete;    //删除赋值运算符

 private:
  // Singleton class - use `ZetaCache::Instance()` instead. //++Singleton 类 - 使用 `ZetaCache::Instance()` 代替。
  ZetaCache() = default;

  mutable std::mutex mutex_;

  // Caches (item_count, zeta) pairs for a given `theta`. It is okay to key the
  // map by a `double` here because the `theta` values are parsed from a
  // configuration file (i.e., they do not come from calculations). 
  std::unordered_map<Theta, std::map<ItemCount, ZetaN>> cache_;   //++双层map，缓存给定“theta”的（item_count，zeta）对
};

}  // namespace

namespace ycsbr {
namespace gen {

void ZipfianChooser::UpdateZetaNWithCaching() {    //!在缓存中查找theta和item_count有没有对应的zeta(n)，没有的话就计算zeta(n)并加到缓存中
  ZetaCache& cache = ZetaCache::Instance();   //引用ZetaCache的静态对象
  auto result = cache.FindStartingPoint(item_count_, theta_);  //从cache.cache_中查找zeta(n)
  if (result.has_value() && result->first == item_count_) {  //找到了
    // We computed zeta(n) for this `item_count` and `theta` before.
    zeta_n_ = result->second;
    return;
  }  //没找到的话执行以下代码
  size_t prev_item_count = 0;
  double prev_zeta_n = 0.0;
  if (result.has_value()) {
    prev_item_count = result->first;
    prev_zeta_n = result->second;   
   // assert(prev_item_count < item_count_);    /////////////////////////
  }
  //////////////////////////
  if (prev_item_count < item_count_){  
  zeta_n_ = ComputeZetaN(item_count_, theta_, prev_item_count, prev_zeta_n);}
    else{
    zeta_n_ = ComputeZetaNForDecrease(item_count_, theta_, prev_item_count, prev_zeta_n);
  }
  /////////////////////////
  // N.B. Multiple threads may end up computing zeta(n) for the same
  // `item_count`, but we consider this case acceptable because it cannot lead
  // to incorrect zeta(n) values.//++注意： 多个线程可能最终会计算相同的“item_count”的 zeta(n)，但我们认为这种情况是可以接受的，因为它不会导致不正确的 zeta(n) 值
  cache.Add(item_count_, theta_, zeta_n_);
}

}  // namespace gen
}  // namespace ycsbr
