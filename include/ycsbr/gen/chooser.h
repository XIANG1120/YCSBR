#pragma once

#include <cstdint>
#include <cstring>

#include "ycsbr/gen/types.h"
#include "ycsbr/request.h"

namespace ycsbr {
namespace gen {

// Chooses values from a 0-based dense range.  //++从基于0的密集范围中选择value
// Used to select existing keys for read/update/scan operations.  //++用于选择实现read/update/scan操作的现有key
class Chooser {
 public:
  virtual ~Chooser() = default;
  virtual size_t Next(PRNG& prng) = 0;
  virtual void SetItemCount(size_t item_count) = 0;
  virtual void IncreaseItemCountBy(int delta) = 0;   ////////////////////////////////
};

}  // namespace gen
}  // namespace ycsbr
