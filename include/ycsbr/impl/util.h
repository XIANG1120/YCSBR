#pragma once

#include <cassert>
#include <cstdint>
#include <memory>

namespace ycsbr {
namespace impl {

template <class RNG>
inline std::unique_ptr<char[]> GetRandomBytes(const size_t size,
                                              RNG& prng) {
  assert(size >= sizeof(uint32_t));   //size必须大于四
  std::unique_ptr<char[]> values = std::make_unique<char[]>(size);   //生成size字节数的values
  const size_t num_u32 = size / sizeof(uint32_t);   //每四个字节四个字节地生成
  uint32_t* start = reinterpret_cast<uint32_t*>(values.get());
  for (size_t i = 0; i < num_u32; ++i, ++start) {
    *start = prng();
  }
  return values;
}

}  // namespace impl
}  // namespace ycsbr
