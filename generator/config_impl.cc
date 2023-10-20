#include "config_impl.h"

#include <cassert>
#include <iostream>
#include <stdexcept>

#include "hotspot_keygen.h"
#include "latest_chooser.h"
#include "linspace_keygen.h"
#include "uniform_chooser.h"
#include "uniform_keygen.h"
#include "yaml-cpp/yaml.h"
#include "ycsbr/gen/keyrange.h"
#include "ycsbr/gen/types.h"
#include "zipfian_chooser.h"

namespace {

using namespace ycsbr;

// Top-level keys.
const std::string kLoadConfigKey = "load";
const std::string kRunConfigKey = "run";
const std::string kRecordSizeBytesKey = "record_size_bytes";

// Operation keys.
const std::string kReadOpKey = "read";
const std::string kScanOpKey = "scan";
const std::string kUpdateOpKey = "update";
const std::string kInsertOpKey = "insert";
const std::string kRMWOpKey = "readmodifywrite";
const std::string kNegativeReadKey = "negativeread";
const std::string kDeleteOpKey = "delete";                ///////////////////////////

// Assorted keys.
const std::string kNumRecordsKey = "num_records";
const std::string kNumRequestsKey = "num_requests";
const std::string kDistributionKey = "distribution";
const std::string kDistributionTypeKey = "type";
const std::string kProportionKey = "proportion_pct";
const std::string kScanMaxLengthKey = "max_length";

// Distribution names and keys.
// Access operations are read, scan, update, readmodifywrite, and negativeread
// (i.e., everything except insert).
const std::string kUniformDist = "uniform";    // Insert and access ops
const std::string kZipfianDist = "zipfian";    // Access ops only
const std::string kHotspotDist = "hotspot";    // Insert ops only
const std::string kLinspaceDist = "linspace";  // Insert ops only
const std::string kCustomDist = "custom";      // Insert ops only
const std::string kLatestDist = "latest";      // Access ops only
// This does not scatter the zipfian-generated requests.
const std::string kZipfianClusteredDist =
    "zipfian_clustered";  // Access ops only

const std::string kRangeMinKey = "range_min";
const std::string kRangeMaxKey = "range_max";
const std::string kZipfianThetaKey = "theta";
const std::string kHotspotProportionKey = "hot_proportion_pct";
const std::string kHotRangeMinKey = "hot_" + kRangeMinKey;
const std::string kHotRangeMaxKey = "hot_" + kRangeMaxKey;
const std::string kLinspaceStartKey = "start_key";
const std::string kLinspaceStepSize = "step_size";
const std::string kSaltKey = "salt";
const std::string kCustomNameKey = "name";
const std::string kCustomOffsetKey = "offset";

// Only does a quick high-level structural validation. The semantic validation
// is done when phases are retrieved.
bool ValidateConfig(const YAML::Node& raw_config) {
  if (!raw_config.IsMap()) {
    std::cerr << "ERROR: Workload config needs to be a YAML map." << std::endl;
    return false;
  }
  if (!raw_config[kLoadConfigKey]) {
    std::cerr << "ERROR: Missing workload config '" << kLoadConfigKey
              << "' section." << std::endl;
    return false;
  }
  if (!raw_config[kRunConfigKey]) {
    std::cerr << "ERROR: Missing workload config '" << kRunConfigKey
              << "' section." << std::endl;
    return false;
  }
  if (!raw_config[kRunConfigKey].IsSequence()) {
    std::cerr << "ERROR: The workload config's '" << kRunConfigKey
              << "' section should be a list of phases." << std::endl;
    return false;
  }
  for (const auto& raw_phase : raw_config[kRunConfigKey]) {
    if (!raw_phase.IsMap()) {
      std::cerr
          << "ERROR: Each phase in the workload config should be a YAML map."
          << std::endl;
      return false;
    }
  }

  return true;
}

// NOTE: This method will release the lock while the chooser is being
// constructed. It will the reacquire the lock before returning. This is done to
// avoid holding the lock while creating the generator, which may take a lot of
// time.  //++注意：此方法将在构造chooser时释放锁。它将在返回之前重新获得锁。这样做是为了避免在创建chooser时保持锁定，这可能需要花费大量时间。
std::unique_ptr<gen::Chooser> CreateChooser(     //!创建不同类型的chooser并返回它们的唯一指针
    std::unique_lock<std::mutex>& lock, const YAML::Node& distribution_config,
    const std::string& operation_name, const size_t item_count) {
  assert(lock.owns_lock());

  const std::string& dist_type =
      distribution_config[kDistributionTypeKey].as<std::string>();   //获取分布的type，如zipfian、uniform等

  if (dist_type == kUniformDist) {   //!如果是uniform分布，则创建gen::UniformChooser并返回其唯一指针
    lock.unlock();
    auto chooser = std::make_unique<gen::UniformChooser>(item_count);
    lock.lock();
    return chooser;

  } else if (dist_type == kZipfianDist || dist_type == kZipfianClusteredDist) {     //!如果是zipfian或zipfian_clustered分布，则创建gen::ScatteredZipfianChooser或gen::ZipfianChooser并返回其唯一指针
    const double theta = distribution_config[kZipfianThetaKey].as<double>();  //获取theta值，且theta值有范围要求
    if (theta <= 0.0 || theta >= 1.0) {
      throw std::invalid_argument("Zipfian theta must be in the range (0, 1).");
    }
    // Salts are optional and are used to create different "scatterings" (i.e.,
    // to have two zipfian distributions choose different hot keys).//++salt是可选的，用于创建不同的“scatterings”（即，要有两个zipfian分布，请选择不同的热键）。
    uint64_t salt = 0;
    if (distribution_config[kSaltKey]) {   
      salt = distribution_config[kSaltKey].as<uint64_t>();
    }
    lock.unlock();
    if (dist_type == kZipfianDist) {
      auto chooser = std::make_unique<gen::ScatteredZipfianChooser>(
          item_count, theta, salt);
      lock.lock();
      return chooser;
    } else {
      assert(dist_type == kZipfianClusteredDist);
      auto chooser = std::make_unique<gen::ZipfianChooser>(item_count, theta);
      lock.lock();
      return chooser;
    }

  } else if (dist_type == kLatestDist) {    //!如果是latest分布，则创建gen::LatestChooser并返回其唯一指针
    const double theta = distribution_config[kZipfianThetaKey].as<double>();     //获取theta值，且theta值有范围要求
    if (theta <= 0.0 || theta >= 1.0) {
      throw std::invalid_argument("Theta must be in the range (0, 1).");
    }
    lock.unlock();
    auto chooser = std::make_unique<gen::LatestChooser>(item_count, theta);
    lock.lock();
    return chooser;

  } else {
    throw std::invalid_argument("Unsupported " + operation_name +
                                " distribution: " + dist_type);
  }
}

gen::KeyRange ParseKeyRange(const YAML::Node& config,    //!返回Range类
                            const std::string& min_key_name,
                            const std::string& max_key_name) {
  const Request::Key range_min = config[min_key_name].as<Request::Key>();
  const Request::Key range_max = config[max_key_name].as<Request::Key>();
  if (range_min > range_max) {
    throw std::invalid_argument(
        min_key_name + " and " + max_key_name +
        " specify an invalid range (min is greater than max).");
  }
  if (range_min > gen::kMaxKey || range_max > gen::kMaxKey) {
    throw std::invalid_argument("Key values cannot exceed 2^48 - 1.");
  }
  return gen::KeyRange(range_min, range_max);
}

// NOTE: This method will release the passed in mutex. This is to avoid holding
// the lock while creating the generator, which may take a lot of time.//++注意：此方法将释放传入的互斥锁。 这是为了避免在创建生成器时持有锁，这可能会花费大量时间。
std::unique_ptr<gen::Generator> CreateGenerator(     //!创造随机生成插入key的生成器，并返回其指针
    std::unique_lock<std::mutex>& lock, const YAML::Node& distribution_config,   
    const size_t num_keys) {
  assert(lock.owns_lock());

  const std::string dist_type =
      distribution_config[kDistributionTypeKey].as<std::string>();
  if (dist_type == kUniformDist) {  //uniform
    gen::KeyRange range =
        ParseKeyRange(distribution_config, kRangeMinKey, kRangeMaxKey);   //从.yaml文件中解析key范围

    lock.unlock();
    return std::make_unique<gen::UniformGenerator>(num_keys, std::move(range));    //将(要生成的键的数量,要生成的键的KeyRange)传入，生成UniformGenerator

  } else if (dist_type == kHotspotDist) {   //hotspot
    gen::KeyRange overall =
        ParseKeyRange(distribution_config, kRangeMinKey, kRangeMaxKey);
    gen::KeyRange hot =
        ParseKeyRange(distribution_config, kHotRangeMinKey, kHotRangeMaxKey);
    const uint32_t hot_proportion_pct =
        distribution_config[kHotspotProportionKey].as<uint32_t>();

    lock.unlock();
    return std::make_unique<gen::HotspotGenerator>(
        num_keys, hot_proportion_pct, std::move(overall), std::move(hot));

  } else if (dist_type == kLinspaceDist) {    //linespace
    const Request::Key start_key =
        distribution_config[kLinspaceStartKey].as<Request::Key>();
    const Request::Key step_size =
        distribution_config[kLinspaceStepSize].as<Request::Key>();

    lock.unlock();
    return std::make_unique<gen::LinspaceGenerator>(num_keys, start_key,
                                                    step_size);

  } else {
    lock.unlock();
    throw std::invalid_argument("Unsupported load/insert distribution: " +
                                dist_type);
  }
}

}  // namespace

namespace ycsbr {
namespace gen {

std::shared_ptr<WorkloadConfig> WorkloadConfig::LoadFrom(
    const std::filesystem::path& config_file,
    const size_t set_record_size_bytes) {
  try {
    YAML::Node node = YAML::LoadFile(config_file);  //解析yaml文件
    if (!ValidateConfig(node)) {
      throw std::invalid_argument("Invalid workload configuration file.");
    }
    return std::make_shared<WorkloadConfigImpl>(std::move(node),    //!返回一个shared指针，指向WorkloadConfigImpl的一个对象（创建实例）  [WorkloadConfigImpl继承WorkloadConfig类]
                                                set_record_size_bytes);
  } catch (const YAML::BadFile&) {
    throw std::invalid_argument(
        "Could not parse the workload configuration file.");
  }
}

std::shared_ptr<WorkloadConfig> WorkloadConfig::LoadFromString(
    const std::string& raw_config, const size_t set_record_size_bytes) {
  YAML::Node node = YAML::Load(raw_config);
  if (!ValidateConfig(node)) {
    throw std::invalid_argument("Invalid workload configuration string.");
  }
  return std::make_shared<WorkloadConfigImpl>(std::move(node),
                                              set_record_size_bytes);
}

WorkloadConfigImpl::WorkloadConfigImpl(YAML::Node raw_config,     
                                       const size_t set_record_size_bytes)
    : raw_config_(std::move(raw_config)),        //初始化raw_config
      set_record_size_bytes_(set_record_size_bytes) {}       //初始化set_record_size_bytes_

bool WorkloadConfigImpl::UsingCustomDataset() const {
  std::unique_lock<std::mutex> lock(mutex_);
  return UsingCustomDatasetImpl();
}

bool WorkloadConfigImpl::UsingCustomDatasetImpl() const {
  return raw_config_[kLoadConfigKey][kDistributionKey][kDistributionTypeKey]
             .as<std::string>() == kCustomDist;
}

size_t WorkloadConfigImpl::GetNumLoadRecords() const {
  std::unique_lock<std::mutex> lock(mutex_);
  return GetNumLoadRecordsImpl();
}

size_t WorkloadConfigImpl::GetNumLoadRecordsImpl() const {
  if (UsingCustomDatasetImpl()) return 0;
  return raw_config_[kLoadConfigKey][kNumRecordsKey].as<size_t>();
}

size_t WorkloadConfigImpl::GetRecordSizeBytes() const {
  std::unique_lock<std::mutex> lock(mutex_);
  size_t record_size_bytes;

  if (raw_config_[kRecordSizeBytesKey]) {
    record_size_bytes = raw_config_[kRecordSizeBytesKey].as<size_t>();
  } else if (set_record_size_bytes_ != 0) {
    record_size_bytes = set_record_size_bytes_;
  } else {
    throw std::invalid_argument("No record size was specified.");
  }
  if (record_size_bytes < 9) {
    throw std::invalid_argument("Record sizes must be at least 9 bytes.");
  }
  return record_size_bytes;
}

std::unique_ptr<Generator> WorkloadConfigImpl::GetLoadGenerator() const {
  std::unique_lock<std::mutex> lock(mutex_);
  if (UsingCustomDatasetImpl()) {
    throw std::invalid_argument(
        "Cannot create a generator when a custom dataset is being used.");
  }

  const YAML::Node& load_dist = raw_config_[kLoadConfigKey][kDistributionKey];
  if (!load_dist) {
    throw std::invalid_argument("Missing load distribution configuration.");
  }
  return CreateGenerator(lock, load_dist, GetNumLoadRecordsImpl());
}

size_t WorkloadConfigImpl::GetNumPhases() const {
  std::unique_lock<std::mutex> lock(mutex_);
  const size_t num_phases = raw_config_[kRunConfigKey].size();
  if (num_phases > kMaxNumPhases) {
    throw std::invalid_argument(
        "Too many workload phases (only 254 are supported).");
  }
  return num_phases;
}

Phase WorkloadConfigImpl::GetPhase(const PhaseID phase_id,
                                   const ProducerID producer_id,
                                   const size_t num_producers) const {
  std::unique_lock<std::mutex> lock(mutex_);

  // We set the item counts of all choosers to this dummy initial value because
  // they will be properly set in Producer::Prepare().//++我们将所有choosers的item计数设置为这个伪初始值，因为它们将在Producer：：Prepare（）中正确设置
  const size_t initial_chooser_size = 1;
  const YAML::Node& phase_config = raw_config_[kRunConfigKey][phase_id];  //将phase_id的任务加载到phase_config中
  Phase phase(phase_id);

  // Compute the number of requests for this producer.    //
  const size_t total_requests = phase_config[kNumRequestsKey].as<size_t>();
  phase.num_requests = total_requests / num_producers;  //给各producer平分request数量
  const size_t remainder = total_requests % num_producers;   //没有整除，有剩余的
  if (producer_id < remainder) {  //如果producer_id小于剩余量，此阶段多加一个request
    ++phase.num_requests;  
  }
  phase.num_requests_left = phase.num_requests;

  // Load the request proportions and validate them.  //++加载查询比例并验证它们
  uint32_t insert_pct = 0;
  if (phase_config[kReadOpKey]) {   //!read  
    phase.read_thres = phase_config[kReadOpKey][kProportionKey].as<uint32_t>();  //read请求占总请求的比例

    // Create the read key chooser.
    phase.read_chooser =
        CreateChooser(lock, phase_config[kReadOpKey][kDistributionKey], "read",    //使用 (分布类型，"read"，initial_chooser_size) 初始化phase.read_chooser
                      initial_chooser_size);
  }
  if (phase_config[kRMWOpKey]) {  //!Read-modify-write
    // Read-modify-write.
    phase.rmw_thres = phase_config[kRMWOpKey][kProportionKey].as<uint32_t>();
    phase.rmw_chooser =
        CreateChooser(lock, phase_config[kRMWOpKey][kDistributionKey],
                      "readmodifywrite", initial_chooser_size);
  }
  if (phase_config[kNegativeReadKey]) {   //!negativeread
    phase.negativeread_thres =
        phase_config[kNegativeReadKey][kProportionKey].as<uint32_t>();
    phase.negativeread_chooser =
        CreateChooser(lock, phase_config[kNegativeReadKey][kDistributionKey],
                      "negativeread", initial_chooser_size);
  }
  if (phase_config[kScanOpKey]) {    //!scan
    phase.scan_thres = phase_config[kScanOpKey][kProportionKey].as<uint32_t>();
    phase.max_scan_length =                                         //
        phase_config[kScanOpKey][kScanMaxLengthKey].as<size_t>();   //
    if (phase.max_scan_length == 0) {                               // 
      throw std::invalid_argument(                                  //  scan.max_length至少为1
          "The maximum scan length must be at least 1.");           //
    }                                                               //

    // Create the scan key chooser.
    phase.scan_chooser =   //scan_chooser 
        CreateChooser(lock, phase_config[kScanOpKey][kDistributionKey], "scan",
                      initial_chooser_size);

    // We need to add 1 because the UniformChooser returns values in a 0-based
    // exclusive upper range. //++我们需要加1，因为UniformChooser返回的值在基于0的除了上限的范围内。
    phase.scan_length_chooser =    //scan_length_chooser 
        std::make_unique<UniformChooser>(phase.max_scan_length + 1);
  }
  if (phase_config[kUpdateOpKey]) {    //!update
    phase.update_thres =
        phase_config[kUpdateOpKey][kProportionKey].as<uint32_t>();

    // Create the update key chooser.
    phase.update_chooser =
        CreateChooser(lock, phase_config[kUpdateOpKey][kDistributionKey],
                      "update", initial_chooser_size);
  }
  ///////////////////////
  if (phase_config[kDeleteOpKey]) {   //!delete      
    phase.delete_thres = 
        phase_config[kDeleteOpKey][kProportionKey].as<uint32_t>();
    //创建chooser
    phase.delete_chooser =
        CreateChooser(lock, phase_config[kDeleteOpKey][kDistributionKey],
                      "delete", initial_chooser_size);
  }
  ///////////////////////
  if (phase_config[kInsertOpKey]) {     //!insert，只收集插入比例
    insert_pct = phase_config[kInsertOpKey][kProportionKey].as<uint32_t>();
  }
  if (insert_pct + phase.read_thres + phase.rmw_thres +     //验证这几个操作加起来的比例是否为100
          phase.negativeread_thres + phase.scan_thres + phase.update_thres 
          + phase.delete_thres            ///////////////////////
           !=
      100) {
    throw std::invalid_argument(
        "Request proportions must sum to exactly 100%.");
  }  

  // Compute the number of inserts we should expect to do.  //++为producer计算应该插入的数量
  phase.num_inserts =
      static_cast<size_t>(phase.num_requests * (insert_pct / 100.0));
  phase.num_inserts_left = phase.num_inserts;

  // Set the thresholds appropriately to allow for comparsion against a random
  // integer generated in the range [0, 100).  //++适当设置阈值，以允许与在[0，100）范围内生成的随机整数进行比较
  phase.delete_thres += phase.read_thres;        ///////////////////
  phase.rmw_thres += phase.read_thres;
  phase.negativeread_thres += phase.rmw_thres;
  phase.scan_thres += phase.negativeread_thres;
  phase.update_thres += phase.scan_thres;

  return phase;
}

std::unique_ptr<Generator> WorkloadConfigImpl::GetGeneratorForPhase(     //!创建生成器
    const Phase& phase) const {
  std::unique_lock<std::mutex> lock(mutex_);

  const YAML::Node& phase_config = raw_config_[kRunConfigKey][phase.phase_id];
  if (!phase_config) {
    throw std::invalid_argument("Nonexistent phase id: " + phase.phase_id);
  }
  if (!phase_config[kInsertOpKey] || phase.num_inserts == 0) {
    // There are no inserts.
    return nullptr;
  }

  const YAML::Node& dist = phase_config[kInsertOpKey][kDistributionKey];
  return CreateGenerator(lock, dist, phase.num_inserts);
}

std::optional<WorkloadConfig::CustomInserts>
WorkloadConfigImpl::GetCustomInsertsForPhase(const Phase& phase) const {   //!获取自定义插入的name和offest，可以返回空值
  std::unique_lock<std::mutex> lock(mutex_);

  const YAML::Node& phase_config = raw_config_[kRunConfigKey][phase.phase_id];
  if (!phase_config) {  //该阶段的工作负载配置不存在
    throw std::invalid_argument("Nonexistent phase id: " + phase.phase_id);
  }
  if (!phase_config[kInsertOpKey] || phase.num_inserts == 0) {  
    // There are no inserts.没有插入
    return std::optional<WorkloadConfig::CustomInserts>();
  }

  const YAML::Node& dist = phase_config[kInsertOpKey][kDistributionKey];  //抓取distribution关键字
  const std::string dist_type = dist[kDistributionTypeKey].as<std::string>();   //获取分布类型
  if (dist_type != kCustomDist) {
    // Phase does not have custom inserts.  此阶段没有自定义插入，则返回空白值
    return std::optional<WorkloadConfig::CustomInserts>();
  }
  if (!dist[kCustomNameKey]) {    //name关键字不存在，抛出错误
    throw std::invalid_argument("Missing custom insert name.");
  }

  WorkloadConfig::CustomInserts res;
  res.name = dist[kCustomNameKey].as<std::string>();
  if (dist[kCustomOffsetKey]) {   //如果有offset关键字
    res.offset = dist[kCustomOffsetKey].as<uint64_t>();
  } else {
    res.offset = 0;
  }
  return res;
}

}  // namespace gen
}  // namespace ycsbr
