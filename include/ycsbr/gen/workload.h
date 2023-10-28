#pragma once

#include <cstdint>
#include <filesystem>
#include <memory>
#include <random>
#include <unordered_map>
#include <vector>
#include <mutex>
#include <map>
#include <set>   //////////////////////////
#include <unordered_set> ///////////////////////

#include "ycsbr/gen/config.h"
#include "ycsbr/gen/phase.h"
#include "ycsbr/gen/types.h"
#include "ycsbr/gen/valuegen.h"
#include "ycsbr/request.h"
#include "ycsbr/trace_workload.h"

namespace ycsbr {
namespace gen {

// Represents a customizable workload with "phases". The workload configuration
// must be specified in a YAML file. See `tests/workloads/custom.yml` for an
// example.//++表示具有“阶段”的自定义工作负载。 工作负载配置必须在 YAML 文件中指定。 有关示例，请参阅“tests/workloads/custom.yml”。
class PhasedWorkload {
 public:
  // Creates a `PhasedWorkload` from the configuration in the provided file.  //++根据提供的文件中的配置创建“PhasedWorkload”。 
  // Set the `prng_seed` to ensure reproducibility. Setting  //++设置“prng_seed”以确保再现性。
  // `set_record_size_bytes` to a positive value will override the record size  //++将“set_record_size_bytes”设置为正值将覆盖工作负载文件中指定的记录大小（如果有）。
  // specified in the workload file, if any.
  static std::unique_ptr<PhasedWorkload> LoadFrom(
      const std::filesystem::path& config_file, uint32_t prng_seed = 42,
      const size_t set_record_size_bytes = 0);

  // Creates a `PhasedWorkload` from a configuration stored in a string. This
  // method is mainly useful for testing purposes. Setting
  // `set_record_size_bytes` to a positive value will override the record size
  // specified in the workload file, if any.
  static std::unique_ptr<PhasedWorkload> LoadFromString(   //!根据存储在字符串中的配置创建“PhasedWorkload”。 此方法主要用于测试目的。
      const std::string& raw_config, uint32_t prng_seed = 42,
      const size_t set_record_size_bytes = 0);

  // Sets the "load dataset" that should be used. This method should be used
  // when you want to use a custom dataset. Note that the workload config file's
  // "load" section must specify that the distribution is "custom".
  void SetCustomLoadDataset(std::vector<Request::Key> dataset);  //!设置应该使用的“load dataset”。 当想要使用自定义数据集时，应使用此方法。 请注意，工作负载配置文件的“load”部分必须指定分配是“custom”。

  // Used to specify a custom list of keys to insert. The keys will be inserted
  // in the given order. The specified `name` should match a name used in the
  // workload configuration file.
  void AddCustomInsertList(const std::string& name,   //!指定一个自定义键列表用于插入。 keys将按照给定的顺序插入。 指定的“name”应与工作负载配置文件中使用的name匹配。
                           std::vector<Request::Key> to_insert);

  // Retrieve the size of the records in the workload, in bytes.
  size_t GetRecordSizeBytes() const;    //!检索工作负载中record的大小（以字节为单位）

  // Get a load trace that can be used to load a database with the records used
  // in this workload.    //++获取可用于load包含此工作负载中使用的记录的数据库的加载跟踪
  //
  // If `sort_requests` is set to true, the records in the trace will be sorted
  // in ascending order by key. If `sort_requests` is false, there are no
  // guarantees on the order of the records in the trace.    //++如果“sort_requests”设置为 true，则跟踪中的记录将按键升序排序。 如果“sort_requests”为 false，则无法保证跟踪中记录的顺序。
  //
  // NOTE: If a custom dataset is used, `SetCustomLoadDataset()` must be called
  // first before this method.      //++注意：如果使用自定义数据集，则必须在此方法之前先调用“SetCustomLoadDataset()”。
  BulkLoadTrace GetLoadTrace(bool sort_requests = false) const;

  class Producer;
  // Used by the workload runner to prepare the workload for execution. You
  // generally do not need to call this method.//++由workload runner程序用于为执行准备工作负载。您通常不需要调用此方法。
  std::vector<Producer> GetProducers(size_t num_producers) const;

  // Not intended to be used directly. Use `LoadFrom()` instead.   //!构造函数，不应被直接使用，使用LoadFrom()替代
  PhasedWorkload(std::shared_ptr<WorkloadConfig> config, uint32_t prng_seed);

 private:
  PRNG prng_;
  uint32_t prng_seed_;
  std::shared_ptr<WorkloadConfig> config_;
  std::shared_ptr<std::vector<Request::Key>> load_keys_;
  std::shared_ptr<std::set<Request::Key>> load_keys_set;   //////////////////////////
  std::shared_ptr<std::unordered_map<std::string, std::vector<Request::Key>>>
      custom_inserts_;
};

// Used by the workload runner to actually execute the workload. This class
// generally does not need to be used directly.
class PhasedWorkload::Producer {
 public:
  void Prepare();

  bool HasNext() const {
    return current_phase_ < phases_.size() && phases_[current_phase_].HasNext();
  }
  Request Next();
  
  ///////////////////////////
  std::shared_ptr< std::vector<Request::Key>> GetLoadKeys(){   
    return load_keys_;
  }

  std::shared_ptr<size_t> GetNumLoadKeys(){
    return num_load_keys_;
  }

  void SetNumLoadKeys(size_t size){
    *num_load_keys_=size;
  }

  std::shared_ptr<std::set<Request::Key>> GetLoadKeysSet(){
    return load_keys_set;
  }

  std::vector<Phase>& GetPhases(){
    return phases_;
  }
  
  ///////////////////////////

 private:
  friend class PhasedWorkload;   //友元类
  Producer(std::shared_ptr<const WorkloadConfig> config,  //工作负载配置
          // std::shared_ptr<const std::vector<Request::Key>> load_keys,  //被加载的key
           std::shared_ptr< std::vector<Request::Key>> load_keys,  //被加载的key     ///////////////////////////
           std::shared_ptr<size_t> num_load_keys_,    ///////////////////////////////
           std::mutex & mute,   //////////////////////////
           std::shared_ptr<std::unordered_set<Request::Key>> keys,   //////////////////////////
           std::shared_ptr<std::set<Request::Key>> set_,  ////////////////////////
           std::shared_ptr<
               const std::unordered_map<std::string, std::vector<Request::Key>>>
               custom_inserts,   //自定义插入键
           ProducerID id, size_t num_producers, uint32_t prng_seed);  //producer ID,生产者数量，prng_seed

  Request::Key ChooseKey(const std::unique_ptr<Chooser>& chooser);    

  ProducerID id_;
  size_t num_producers_;
  std::shared_ptr<const WorkloadConfig> config_;
  PRNG prng_;

  std::vector<Phase> phases_;
  PhaseID current_phase_;

  // The keys that were loaded.    //++被加载的key
  //std::shared_ptr<const std::vector<Request::Key>> load_keys_;
  std::shared_ptr< std::vector<Request::Key>> load_keys_;    /////////////////////
  //size_t num_load_keys_;
  std::shared_ptr<size_t> num_load_keys_;   /////////////////////////
  // Custom keys to insert.   //++自定义插入key
  std::shared_ptr<
      const std::unordered_map<std::string, std::vector<Request::Key>>>
      custom_inserts_;

  // Stores all the keys this producer will eventually insert.//++存储producer的每个阶段最终将插入的所有key
  std::vector<Request::Key> insert_keys_;
  std::vector<Request::Key> delete_keys_;    ///////////////////////
  size_t next_insert_key_index_;
  size_t next_delete_key_index_;   ////////////////////////////
  
  std::shared_ptr<std::unordered_set<Request::Key>> keys_;
  std::shared_ptr<std::set<Request::Key>> load_keys_set;

  std::mutex & mtx;   ///////////////////////////

  ValueGenerator valuegen_;

  std::uniform_int_distribution<uint32_t> op_dist_;
};

}  // namespace gen
}  // namespace ycsbr
