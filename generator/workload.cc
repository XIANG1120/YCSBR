#include "ycsbr/gen/workload.h"
#include <iostream>    //////////////////////////
#include <thread>   ////////////////////////

#include <cassert>

#include "ycsbr/buffered_workload.h"
#include "ycsbr/gen/types.h"

namespace {

using namespace ycsbr;
using namespace ycsbr::gen;

// Producers will cycle through this many unique values (when inserting or
// making updates).
constexpr size_t kNumUniqueValues = 100;

void ApplyPhaseAndProducerIDs(std::vector<Request::Key>::iterator begin,
                              std::vector<Request::Key>::iterator end,
                              const PhaseID phase_id,
                              const ProducerID producer_id) {
  for (auto it = begin; it != end; ++it) {
    *it = (*it << 16) | ((phase_id & 0xFF) << 8) | (producer_id & 0xFF); //key = key值左移16位，后面紧跟的8位放phase_id，最后放producer_id，一共key在后面多添了16位
  }
}

}  // namespace

namespace ycsbr {

// For convenience, instantiate a `BufferedWorkload` for `PhasedWorkload`.
template class BufferedWorkload<gen::PhasedWorkload>;

namespace gen {

std::mutex mute;    /////////////////////////////

using Producer = PhasedWorkload::Producer;

std::unique_ptr<PhasedWorkload> PhasedWorkload::LoadFrom( //LoadFrom函数实例化一个PhasedWorkload对象，并返回指向这个对象的唯一指针
    const std::filesystem::path& config_file, const uint32_t prng_seed,
    const size_t set_record_size_bytes) {
  return std::make_unique<PhasedWorkload>( //?std::make_unique 是 C++11 标准引入的一个函数模板，用于创建一个动态分配的对象，并返回一个指向该对象的 std::unique_ptr 智能指针 //!返回一个指向PhasedWorkload对象的唯一指针（创建实例）
      WorkloadConfig::LoadFrom(config_file, set_record_size_bytes), prng_seed);  //调用构造函数，WorkloadConfig::LoadFrom(config_file, set_record_size_bytes)是workloadconfig的构造函数；此时prng_seed_已被初始化
}

std::unique_ptr<PhasedWorkload> PhasedWorkload::LoadFromString(
    const std::string& raw_config, const uint32_t prng_seed,
    const size_t set_record_size_bytes) {
  return std::make_unique<PhasedWorkload>(
      WorkloadConfig::LoadFromString(raw_config, set_record_size_bytes),
      prng_seed);
}

PhasedWorkload::PhasedWorkload(std::shared_ptr<WorkloadConfig> config,
                               const uint32_t prng_seed)
    : prng_(prng_seed),
      prng_seed_(prng_seed),
      config_(std::move(config)),
      load_keys_(nullptr) {
  // If we're using a custom dataset, the user will call SetCustomLoadDataset()
  // to configure `load_keys_`.
  if (config_->UsingCustomDataset()) return;

  load_keys_ = std::make_shared<std::vector<Request::Key>>(
      config_->GetNumLoadRecords(), 0);
  auto load_gen = config_->GetLoadGenerator();
  load_gen->Generate(prng_, load_keys_.get(), 0);
  ApplyPhaseAndProducerIDs(load_keys_->begin(), load_keys_->end(),
                           /*phase_id=*/0,
                           /*producer_id=*/0);

  // Keep the initial load keys sorted to allow for efficiently generating
  // clustered hot sets.
  std::sort(load_keys_->begin(), load_keys_->end());
}

void PhasedWorkload::SetCustomLoadDataset(std::vector<Request::Key> dataset) {
  assert(dataset.size() > 0);//?assert 宏，用于在程序中插入断言检查，以确保程序在运行时满足特定的条件。 //如果断言条件不满足，即为假（false），则会触发断言失败，程序可能会终止并输出错误信息。
  if (*std::max_element(dataset.begin(), dataset.end()) > kMaxKey) {//?std::max_element 是一个函数，该函数返回vector中的最大元素的迭代器。dataset.begin() 表示容器的起始迭代器，dataset.end() 表示容器的结束迭代器。
    throw std::invalid_argument("The maximum supported key is 2^48 - 1.");
  }
  load_keys_ = std::make_shared<std::vector<Request::Key>>(std::move(dataset));//!创建一个std::shared_ptr，指向一个std::vector，其中存储着从 dataset 移动而来的数据。//这样，load_keys_ 成员变量指向的地址将包含自定义数据集中的数据
  ApplyPhaseAndProducerIDs(load_keys_->begin(), load_keys_->end(),
                           /*phase_id=*/0, /*producer_id=*/0);//!在原key后面加入8位phase_id和8位producer_id

  // Keep the initial load keys sorted to allow for efficiently generating
  // clustered hot sets.//++保持初始加载密钥的排序，以便有效地生成集群热集。
  std::sort(load_keys_->begin(), load_keys_->end());   
}

void PhasedWorkload::AddCustomInsertList(const std::string& name,
                                         std::vector<Request::Key> to_insert) {
  assert(to_insert.size() > 0);
  if (*std::max_element(to_insert.begin(), to_insert.end()) > kMaxKey) {
    throw std::invalid_argument("The maximum supported key is 2^48 - 1.");
  }
  if (custom_inserts_ == nullptr) {
    custom_inserts_ = std::make_shared<
        std::unordered_map<std::string, std::vector<Request::Key>>>();
  }
  custom_inserts_->emplace(name, std::move(to_insert));
}

size_t PhasedWorkload::GetRecordSizeBytes() const {
  return config_->GetRecordSizeBytes();
}

BulkLoadTrace PhasedWorkload::GetLoadTrace(const bool sort_requests) const {
  Trace::Options options;
  options.value_size = config_->GetRecordSizeBytes() - sizeof(Request::Key);
  options.sort_requests = sort_requests;
  return BulkLoadTrace::LoadFromKeys(*load_keys_, options);
}

std::vector<Producer> PhasedWorkload::GetProducers(
    const size_t num_producers) const {
  std::vector<Producer> producers;
  producers.reserve(num_producers);
  /////////////////////////////
  size_t num_load = load_keys_->size();
  std::shared_ptr<size_t> num_load_keys_ = std::make_shared<size_t>(num_load);
  std::shared_ptr<std::map<size_t,size_t>> map_= std::make_shared<std::map<size_t,size_t>>();
  std::shared_ptr<std::unordered_set<Request::Key>> keys;
  std::shared_ptr<std::size_t> map_size = std::make_shared<std::size_t>(0);
  //////////////////////////////
  for (ProducerID id = 0; id < num_producers; ++id) {
    producers.push_back(
        // Each Producer's workload should be deterministic, but we want each
        // Producer to produce different requests from each other. So we include
        // the producer ID in its seed.//++每个Producer的工作负载应该是确定性的，但我们希望每个Producer彼此产生不同的requests。因此，我们在其种子中包含producer ID。
        //Producer(config_, load_keys_,  custom_inserts_, id, num_producers, 
        Producer(config_, load_keys_, num_load_keys_, mute,  map_,  keys, map_size, custom_inserts_, id, num_producers,  ///////////////////////////
                 prng_seed_ ^ id));
  }
  return producers;
}

Producer::Producer(
    std::shared_ptr<const WorkloadConfig> config,
    //std::shared_ptr<const std::vector<Request::Key>> load_keys,  
    std::shared_ptr< std::vector<Request::Key>> load_keys,   /////////////////////////////
    std::shared_ptr<size_t> num_load_keys,   /////////////////////////////
    std::mutex & mute,   ///////////////////////
    std::shared_ptr<std::map<size_t,size_t>> map,   /////////////////////////
    std::shared_ptr<std::unordered_set<Request::Key>> keys,   //////////////////////////
    std::shared_ptr<std::size_t> map_size,    //////////////////////////
    std::shared_ptr<
        const std::unordered_map<std::string, std::vector<Request::Key>>>
        custom_inserts,
    const ProducerID id, const size_t num_producers, const uint32_t prng_seed)
    : id_(id),
      num_producers_(num_producers),
      config_(std::move(config)),
      prng_(prng_seed),
      current_phase_(0),
      load_keys_(std::move(load_keys)),
      //num_load_keys_(load_keys_->size()),
      num_load_keys_(num_load_keys),    /////////////////////////
      custom_inserts_(std::move(custom_inserts)),
      next_insert_key_index_(0),
      num_load_previous(*num_load_keys),          //////////////////////////////////
      mtx(mute),     ///////////////////////////
      delete_map_(map),   /////////////////////////
      map_size_(map_size),    ////////////////////////
      map_size_insert(0),
      valuegen_(config_->GetRecordSizeBytes() - sizeof(Request::Key),
                kNumUniqueValues, prng_),
      op_dist_(0, 99) {}

void Producer::Prepare() {   //!配置各个phase,生成每个phase的各种chooser，并载入/生成insert keys到producer.insert_keys_
  // Set up the workload phases.  //++设置每个phase
  const size_t num_phases = config_->GetNumPhases();  //返回phase数量
  phases_.reserve(num_phases);
  for (PhaseID phase_id = 0; phase_id < num_phases; ++phase_id) {
    phases_.push_back(config_->GetPhase(phase_id, id_, num_producers_));  //以(阶段id,producer id，producer数量)初始化Phase并放入phases_中
  }

  // Generate the inserts.  //++为每个phase生成inserts
  size_t insert_index = 0;
  for (auto& phase : phases_) {    //*遍历每个phase,如果phase.num_inserts=0则continue
    if (phase.num_inserts == 0) continue;      //////////////////////
    const auto custom_insert_info = config_->GetCustomInsertsForPhase(phase);   //返回std::optional<WorkloadConfig::CustomInserts>类型,包含自定义插入的name和offset
    if (custom_insert_info.has_value()) {
      // This phase uses a custom insert list.   //++这个阶段用了一个自定义插入列表
      if (custom_inserts_ == nullptr) {
        throw std::runtime_error("Did not find inserts for '" +
                                 custom_insert_info->name + "'.");
      }
      const auto it = custom_inserts_->find(custom_insert_info->name);    //从custom_inserts_找key为name的元素
      if (it == custom_inserts_->end()) {
        throw std::runtime_error("Did not find inserts for '" +
                                 custom_insert_info->name + "'.");
      }
      if (it->second.size() < custom_insert_info->offset ||
          it->second.size() - custom_insert_info->offset < phase.num_inserts) {    //数量对不上
        throw std::runtime_error("Not enough keys in '" +
                                 custom_insert_info->name +
                                 "' to make all requested inserts.");
      }
      insert_keys_.resize(insert_keys_.size() + phase.num_inserts);
      std::copy(   //?std::copy(InputIt first, InputIt last, OutputIt d_first);
          it->second.begin() + custom_insert_info->offset,
          it->second.begin() + custom_insert_info->offset + phase.num_inserts,      //将从offset位置开始的phase.num_inserts个key放入insert_keys_
          insert_keys_.begin() + insert_index);

    } else {
      // This phase's inserts are randomly generated.   //++这个阶段的插入被随机生成
      auto generator = config_->GetGeneratorForPhase(phase);
      assert(generator != nullptr);
      insert_keys_.resize(insert_keys_.size() + phase.num_inserts);
      generator->Generate(prng_, &insert_keys_, insert_index);
    }
    ApplyPhaseAndProducerIDs(   //*为每个insert key添加phase id和proceducer id
        insert_keys_.begin() + insert_index,
        insert_keys_.begin() + insert_index + phase.num_inserts,
        // We add 1 because ID 0 is reserved for the initial load.  //++我们添加 1，因为 ID 0 是为初始加载保留的。
        phase.phase_id + 1, id_ + 1);
    insert_index = insert_keys_.size();
  }  //遍历phase结束

  // Set the phase chooser item counts based on the number of inserts the
  // producer will make in each phase.  //++根据producer在每个phase中进行的插入数量设置phase chooser item counts。
  size_t count = load_keys_->size();  //每个producer都相同
  for (auto& phase : phases_) {
    phase.SetItemCount(count);  //为每个phase设置itemcount
    count += phase.num_inserts;
  }
}

Request::Key Producer::ChooseKey(const std::unique_ptr<Chooser>& chooser) {       
  ///////////////////////  
  //  std::cerr<< "成功进入choosekey"<<std::endl;
  Phase & this_phase = phases_[current_phase_];
  Request::Key key;
  mtx.lock();
  size_t num_load = *num_load_keys_-*map_size_;   //读
  if (num_load_previous - num_load > 0) {
    this_phase.IncreaseItemCountBy(num_load - num_load_previous); }
  num_load_previous = num_load;  //读
  size_t index = chooser->Next(prng_);
  if( *num_load_keys_-*map_size_ > index){    //一开始在load_keys_里面查找
    size_t target = IntoLoadKeys(index);
    while (target!=0)
    {
      index++;
      auto result = delete_map_->find(index);
      if(result==delete_map_->end()) target--;
    }
    key = (*load_keys_)[index];  
    mtx.unlock();
    return key;
  }else{   //一开始在delete_map_insert里面查找
    mtx.unlock();
    index = index - *num_load_keys_+ *map_size_;
    size_t target = IntoInsertKeys(index+*num_load_keys_);
    while (target!=0)
    {
      index++;
      auto result = delete_map_insert_.find(index+*num_load_keys_);
      if(result==delete_map_insert_.end()) target--;
    }
    return insert_keys_[index];
  }
}

size_t Producer::IntoLoadKeys(size_t index){
if(*map_size_>0){
        auto result = delete_map_->lower_bound(index);
        if (result == delete_map_->end() ) {  //map里面的元素全都小于目标值
          return *map_size_;
        }
        else if(result->first == index){   //找到了与目标值相等的元素
          return result->second;
        }
        else if(result->first > index){   //找到了第一个大于目标值的元素
          if(*map_size_!=1)
          {
          return result->second-1;
          }
        }
    }
    return 0;
}

size_t Producer::IntoInsertKeys(size_t  index) {
  if(map_size_insert>0){
        auto result = delete_map_insert_.lower_bound(index);
        if (result == delete_map_insert_.end() ) {
          return  map_size_insert;
        }
        else if(result->first == index){
          return result->second;
        }
        else if(result->first > index){
          if(map_size_insert!=1)
          {
            return result->second-1;
          }
        }
    }
    return 0;
}

////////////////////////////////////
Request::Key Producer::deleteChooseKey(const std::unique_ptr<Chooser>& chooser) {
  // std::cerr<< "成功进入deletechoosekey"<<std::endl;
  Phase & this_phase = phases_[current_phase_];
  Request::Key key;
  mtx.lock();
  size_t num_load = *num_load_keys_-*map_size_;   //读
  if (num_load_previous - num_load > 0) {
    this_phase.IncreaseItemCountBy(num_load - num_load_previous); }
  num_load_previous = num_load;  //读
  size_t index = chooser->Next(prng_);
  if( *num_load_keys_-*map_size_ > index){    //一开始在load_keys_里面查找
    size_t target = IntoLoadKeys(index);
    while (target!=0)
    {
      index++;
      auto result = delete_map_->find(index);
      if(result==delete_map_->end()) target--;
    }
    key = (*load_keys_)[index];  
    delete_map_->emplace(index,*map_size_+1);
    (*map_size_)++;
    mtx.unlock();
    return key;
  }else{   //一开始在delete_map_insert里面查找
    mtx.unlock();
    index = index - (*num_load_keys_- *map_size_);
    size_t target = IntoInsertKeys(index+*num_load_keys_);
    while (target!=0)
    {
      index++;
      auto result = delete_map_insert_.find(index+*num_load_keys_);
      if(result==delete_map_insert_.end()) target--;
    }
    delete_map_insert_.emplace(index+*num_load_keys_,map_size_insert+1);
    map_size_insert++;
    this_phase.IncreaseItemCountBy(-1);
    return insert_keys_[index];
  }
}
////////////////////////////////////

Request Producer::Next() {
  assert(HasNext());
  Phase& this_phase = phases_[current_phase_];

  Request::Operation next_op = Request::Operation::kInsert;

  // If there are more requests left than inserts, we can randomly decide what
  // request to do next. Otherwise we must do an insert. Note that we adjust
  // `op_dist_` as needed to ensure that we do not generate an insert once
  // `this_phase.num_inserts_left == 0`.//++如果剩下的请求比插入的多，我们可以随机决定下一步要做什么请求。否则我们必须插入。请注意，我们根据需要调整`op_dist_`，以确保不会生成一次插入`this_phase.num_inserts_left=0`。
  if (this_phase.num_inserts_left < this_phase.num_requests_left) {
    // Decide what operation to do.
    const uint32_t choice = op_dist_(prng_);
    if (choice < this_phase.read_thres) {
      next_op = Request::Operation::kRead;
    } else if (choice < this_phase.rmw_thres) {
      next_op = Request::Operation::kReadModifyWrite;
    } else if (choice < this_phase.negativeread_thres) {
      next_op = Request::Operation::kNegativeRead;
    } else if (choice < this_phase.scan_thres) {
      next_op = Request::Operation::kScan;
    } else if (choice < this_phase.update_thres) {
      next_op = Request::Operation::kUpdate;
    //////////////////
    } else if (choice < this_phase.delete_thres) {
      next_op = Request::Operation::kDelete;
    ///////////////////
    } else {
      next_op = Request::Operation::kInsert;
      assert(this_phase.num_inserts_left > 0);
    }
  }

  Request to_return;
  switch (next_op) {
    case Request::Operation::kRead: {
      to_return = Request(Request::Operation::kRead,
                          ChooseKey(this_phase.read_chooser), 0, nullptr, 0); 
      break;
    }

    case Request::Operation::kReadModifyWrite: {
      to_return = Request(Request::Operation::kReadModifyWrite,
                          ChooseKey(this_phase.rmw_chooser), 0,   
                          valuegen_.NextValue(), valuegen_.value_size());
      break;
    }

    case Request::Operation::kNegativeRead: {
      Request::Key to_read = ChooseKey(this_phase.negativeread_chooser);  
      to_read |= (0xFF << 8);
      to_return =
          Request(Request::Operation::kNegativeRead, to_read, 0, nullptr, 0);
      break;
    }

    case Request::Operation::kScan: {
      // We add 1 to the chosen scan length because `Chooser` instances always
      // return values in a 0-based range.
      to_return =
          Request(Request::Operation::kScan, ChooseKey(this_phase.scan_chooser),   
                  this_phase.scan_length_chooser->Next(prng_) + 1, nullptr, 0);
      break;
    }

    case Request::Operation::kUpdate: {
      to_return = Request(Request::Operation::kUpdate,
                          ChooseKey(this_phase.update_chooser), 0,  
                          valuegen_.NextValue(), valuegen_.value_size());
      break;
    }

    //////////////////////////////
    case Request::Operation::kDelete: {
      to_return =
          Request(Request::Operation::kDelete, deleteChooseKey(this_phase.delete_chooser), 0, nullptr, 0);
      break;
    }
    //////////////////////////////

    case Request::Operation::kInsert: {
      to_return = Request(Request::Operation::kInsert,
                          insert_keys_[next_insert_key_index_], 0,
                          valuegen_.NextValue(), valuegen_.value_size());
      ++next_insert_key_index_;
      --this_phase.num_inserts_left;
      this_phase.IncreaseItemCountBy(1);
      if (this_phase.num_inserts_left == 0) {
        // No more inserts left. We adjust the operation selection distribution
        // to make sure we no longer select inserts during this phase. Note that
        // the bounds used below are inclusive.
        if (this_phase.delete_thres > 0) {       //////////////////////
          op_dist_ = std::uniform_int_distribution<uint32_t>(
              0, this_phase.delete_thres - 1);       //////////////////////
        } else {
          // This case should only occur if the workload is insert-only. However
          // this means that this was the last request (we decrement the
          // requests counter below).
          assert(this_phase.num_requests_left == 1);    //剩下的这一个request就是我们当前的这个insert request
        }
      }
      break;
    }
  }

  // Advance to the next request.
  --this_phase.num_requests_left;
  if(this_phase.num_requests_left==523400) {std::cerr<<std::this_thread::get_id() << ":" <<"已完成三分之二"<<std::endl;}
  if(this_phase.num_requests_left==1046800) {std::cerr<<std::this_thread::get_id() << ":" <<"已完成三分之一"<<std::endl;}
  // std::cerr<< std::this_thread::get_id() << ":" << this_phase.num_requests_left<<std::endl;
  if (this_phase.num_requests_left == 0) {
    ++current_phase_;
    /////////////////////////
    // std::cerr << "我要进入下一个阶段了" <<std::endl;
    if(current_phase_<phases_.size()){
      std::lock_guard<std::mutex> lock(mtx);
      phases_[current_phase_].SetItemCount(*num_load_keys_ + insert_keys_.size()-*map_size_-map_size_insert);
      // std::cerr << "我进入了下一个阶段" <<std::endl;
    }
    /////////////////////////
    // Reset the operation selection distribution.
    op_dist_ = std::uniform_int_distribution<uint32_t>(0, 99);
  }
  return to_return;
}

}  // namespace gen
}  // namespace ycsbr
