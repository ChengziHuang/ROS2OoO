#include <algorithm>
#include <array>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <limits>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <rclcpp/rclcpp.hpp>
#include <rclcpp/serialization.hpp>
#include <rclcpp/serialized_message.hpp>
#include <rclcpp_components/register_node_macro.hpp>
#include <rcutils/error_handling.h>
#include <sensor_msgs/msg/camera_info.hpp>
#include <sensor_msgs/msg/image.hpp>
#include <sensor_msgs/msg/point_cloud2.hpp>
#include <sensor_msgs/msg/point_field.hpp>
#include <std_msgs/msg/u_int64_multi_array.hpp>
#include <stereo_msgs/msg/disparity_image.hpp>

namespace let_ros_perception
{
namespace
{
using Image = sensor_msgs::msg::Image;
using CameraInfo = sensor_msgs::msg::CameraInfo;
using Cloud = sensor_msgs::msg::PointCloud2;
using Disparity = stereo_msgs::msg::DisparityImage;
using Summary = std_msgs::msg::UInt64MultiArray;

constexpr uint64_t kFnvOffset = UINT64_C(14695981039346656037);
constexpr uint64_t kFnvPrime = UINT64_C(1099511628211);

void hash_bytes(uint64_t & h, const uint8_t * p, size_t n)
{
  for (size_t i = 0; i < n; ++i) {
    h = (h ^ p[i]) * kFnvPrime;
  }
}

void hash_word(uint64_t & h, uint64_t word)
{
  for (int b = 0; b < 8; ++b) {
    h = (h ^ static_cast<uint8_t>(word >> (8 * b))) * kFnvPrime;
  }
}

uint64_t double_bits(double value)
{
  static_assert(sizeof(double) == sizeof(uint64_t), "summary requires IEEE-sized doubles");
  uint64_t out = 0;
  std::memcpy(&out, &value, sizeof(out));
  return out;
}

template<typename MessageT>
std::shared_ptr<const MessageT> read_cdr(const std::filesystem::path & path, uint64_t & total)
{
  std::ifstream in(path, std::ios::binary | std::ios::ate);
  if (!in) {
    throw std::runtime_error("cannot open CDR input " + path.string());
  }
  const auto end = in.tellg();
  if (end <= 0 || static_cast<uint64_t>(end) > std::numeric_limits<size_t>::max()) {
    throw std::runtime_error("empty or oversized CDR input " + path.string());
  }
  const size_t bytes = static_cast<size_t>(end);
  rclcpp::SerializedMessage serialized(bytes);
  auto & raw = serialized.get_rcl_serialized_message();
  in.seekg(0);
  if (!in.read(reinterpret_cast<char *>(raw.buffer), static_cast<std::streamsize>(bytes))) {
    throw std::runtime_error("cannot read CDR input " + path.string());
  }
  raw.buffer_length = bytes;
  auto message = std::make_shared<MessageT>();
  try {
    rclcpp::Serialization<MessageT> codec;
    codec.deserialize_message(&serialized, message.get());
  } catch (const std::exception & e) {
    throw std::runtime_error("cannot deserialize " + path.string() + ": " + e.what());
  }
  rcutils_reset_error();
  total += bytes;
  return message;
}

std::array<std::string, 4> manifest_row(const std::string & line, size_t line_number)
{
  std::array<std::string, 4> fields;
  size_t start = 0;
  for (size_t i = 0; i < fields.size(); ++i) {
    const size_t tab = line.find('\t', start);
    if ((i < 3 && tab == std::string::npos) || (i == 3 && tab != std::string::npos)) {
      throw std::runtime_error("manifest line " + std::to_string(line_number) +
              " must have exactly four TAB-separated paths");
    }
    fields[i] = line.substr(start, tab == std::string::npos ? tab : tab - start);
    if (fields[i].empty()) {
      throw std::runtime_error("empty path on manifest line " + std::to_string(line_number));
    }
    start = tab == std::string::npos ? line.size() : tab + 1;
  }
  return fields;
}

void validate_image(const Image & image, const std::string & context)
{
  const uint64_t needed = static_cast<uint64_t>(image.step) * image.height;
  if (image.width == 0 || image.height == 0 || image.step == 0 || needed > image.data.size()) {
    throw std::runtime_error(context + ": empty or inconsistent image layout");
  }
}

bool host_big_endian()
{
  const uint16_t value = 1;
  return *reinterpret_cast<const uint8_t *>(&value) == 0;
}

template<typename NumberT>
NumberT read_number(const uint8_t * bytes, bool big_endian)
{
  std::array<uint8_t, sizeof(NumberT)> copy;
  std::copy(bytes, bytes + copy.size(), copy.begin());
  if (host_big_endian() != big_endian) {
    std::reverse(copy.begin(), copy.end());
  }
  NumberT out;
  std::memcpy(&out, copy.data(), sizeof(out));
  return out;
}

class SummaryState
{
public:
  explicit SummaryState(uint64_t kind) : kind_(kind) {}

  void header(const std_msgs::msg::Header & header)
  {
    ++frames_;
    hash_word(content_hash_, static_cast<uint64_t>(header.stamp.sec));
    hash_word(content_hash_, header.stamp.nanosec);
    hash_bytes(content_hash_, reinterpret_cast<const uint8_t *>(header.frame_id.data()),
      header.frame_id.size());
  }

  void bytes(const std::vector<uint8_t> & data)
  {
    hash_word(content_hash_, data.size());
    hash_bytes(content_hash_, data.data(), data.size());
  }

  void word(uint64_t value) {hash_word(content_hash_, value);}

  void sample(double a, double b = 0.0, double c = 0.0)
  {
    ++samples_;
    if (!std::isfinite(a) || !std::isfinite(b) || !std::isfinite(c)) {
      ++invalid_;
      hash_word(numeric_hash_, UINT64_C(0x7ff8000000000000));
      return;
    }
    ++finite_;
    sums_[0] += a;
    sums_[1] += b;
    sums_[2] += c;
    minimum_ = std::min(minimum_, a);
    maximum_ = std::max(maximum_, a);
    hash_word(numeric_hash_, double_bits(a));
    hash_word(numeric_hash_, double_bits(b));
    hash_word(numeric_hash_, double_bits(c));
  }

  Summary message() const
  {
    Summary output;
    output.layout.dim.resize(1);
    auto & dim = output.layout.dim[0];
    dim.label = "v1,kind,frames,samples,finite,invalid,content_fnv1a,numeric_fnv1a,"
      "sum0_f64bits,sum1_f64bits,sum2_f64bits,min0_f64bits,max0_f64bits";
    output.data = {1, kind_, frames_, samples_, finite_, invalid_, content_hash_, numeric_hash_,
      double_bits(sums_[0]), double_bits(sums_[1]), double_bits(sums_[2]),
      double_bits(finite_ ? minimum_ : 0.0), double_bits(finite_ ? maximum_ : 0.0)};
    dim.size = static_cast<uint32_t>(output.data.size());
    dim.stride = dim.size;
    return output;
  }

  uint64_t frames() const {return frames_;}
  uint64_t finite() const {return finite_;}

private:
  uint64_t kind_;
  uint64_t frames_ = 0, samples_ = 0, finite_ = 0, invalid_ = 0;
  uint64_t content_hash_ = kFnvOffset, numeric_hash_ = kFnvOffset;
  std::array<double, 3> sums_{};
  double minimum_ = std::numeric_limits<double>::infinity();
  double maximum_ = -std::numeric_limits<double>::infinity();
};
}

class StereoReplaySource : public rclcpp::Node
{
public:
  explicit StereoReplaySource(const rclcpp::NodeOptions & options)
  : Node("stereo_replay", options)
  {
    const std::string manifest = declare_parameter<std::string>("manifest", "");
    const int64_t period = declare_parameter<int64_t>("period_ns", 33333333);
    const int64_t maximum = declare_parameter<int64_t>("max_frames", 0);
    if (manifest.empty() || period <= 0 || maximum < 0) {
      throw std::invalid_argument("manifest is required, period_ns must be positive, "
              "and max_frames must be nonnegative");
    }
    const auto qos = rclcpp::SensorDataQoS().keep_last(10);
    left_image_ = create_publisher<Image>("left/image_raw", qos);
    left_info_ = create_publisher<CameraInfo>("left/camera_info", qos);
    right_image_ = create_publisher<Image>("right/image_raw", qos);
    right_info_ = create_publisher<CameraInfo>("right/camera_info", qos);
    const auto begin = std::chrono::steady_clock::now();
    const auto path = std::filesystem::absolute(manifest);
    std::ifstream in(path);
    if (!in) {
      throw std::runtime_error("cannot open stereo manifest " + path.string());
    }
    std::string line;
    uint64_t bytes = 0;
    size_t line_number = 0;
    while (std::getline(in, line)) {
      ++line_number;
      if (!line.empty() && line.back() == '\r') line.pop_back();
      if (line_number == 1 && line.compare(0, 3, "\xef\xbb\xbf") == 0) line.erase(0, 3);
      const auto first = line.find_first_not_of(" \t");
      if (first == std::string::npos || line[first] == '#') continue;
      if (maximum > 0 && frames_.size() >= static_cast<uint64_t>(maximum)) break;
      const auto names = manifest_row(line, line_number);
      std::array<std::filesystem::path, 4> files;
      for (size_t i = 0; i < files.size(); ++i) {
        const std::filesystem::path named(names[i]);
        files[i] = named.is_absolute() ? named : path.parent_path() / named;
      }
      Frame frame{read_cdr<Image>(files[0], bytes), read_cdr<CameraInfo>(files[1], bytes),
        read_cdr<Image>(files[2], bytes), read_cdr<CameraInfo>(files[3], bytes)};
      validate_image(*frame.left_image, "left image at line " + std::to_string(line_number));
      validate_image(*frame.right_image, "right image at line " + std::to_string(line_number));
      frames_.push_back(std::move(frame));
    }
    if (in.bad()) throw std::runtime_error("error reading stereo manifest " + path.string());
    if (frames_.empty()) throw std::runtime_error("stereo manifest contains no frames");
    const double seconds = std::chrono::duration<double>(
      std::chrono::steady_clock::now() - begin).count();
    RCLCPP_INFO(get_logger(),
      "LET_PERCEPTION_PRELOAD frames=%zu cdr_bytes=%llu preload_s=%.9f period_ns=%lld manifest=%s",
      frames_.size(), static_cast<unsigned long long>(bytes), seconds,
      static_cast<long long>(period), path.string().c_str());
    timer_ = create_wall_timer(std::chrono::nanoseconds(period),
      std::bind(&StereoReplaySource::tick, this));
  }

private:
  struct Frame
  {
    std::shared_ptr<const Image> left_image;
    std::shared_ptr<const CameraInfo> left_info;
    std::shared_ptr<const Image> right_image;
    std::shared_ptr<const CameraInfo> right_info;
  };

  void tick()
  {
    if (index_ == frames_.size()) return;
    const auto & frame = frames_[index_++];
    left_image_->publish(*frame.left_image);
    left_info_->publish(*frame.left_info);
    right_image_->publish(*frame.right_image);
    right_info_->publish(*frame.right_info);
  }

  std::vector<Frame> frames_;
  size_t index_ = 0;
  rclcpp::Publisher<Image>::SharedPtr left_image_, right_image_;
  rclcpp::Publisher<CameraInfo>::SharedPtr left_info_, right_info_;
  rclcpp::TimerBase::SharedPtr timer_;
};

class ImageSink : public rclcpp::Node
{
public:
  explicit ImageSink(const rclcpp::NodeOptions & options) : Node("image_sink", options), state_(1)
  {
    summary_ = create_publisher<Summary>("~/summary", 10);
    input_ = create_subscription<Image>("input", rclcpp::SensorDataQoS().keep_last(10),
      [this](Image::ConstSharedPtr image) {
        validate_image(*image, "ImageSink");
        state_.header(image->header);
        state_.word(image->width);
        state_.word(image->height);
        state_.word(image->step);
        state_.bytes(image->data);
        for (uint8_t byte : image->data) state_.sample(byte);
        summary_->publish(state_.message());
      });
  }
private:
  SummaryState state_;
  rclcpp::Publisher<Summary>::SharedPtr summary_;
  rclcpp::Subscription<Image>::SharedPtr input_;
};

class CameraInfoSink : public rclcpp::Node
{
public:
  explicit CameraInfoSink(const rclcpp::NodeOptions & options)
  : Node("camera_info_sink", options), state_(2)
  {
    summary_ = create_publisher<Summary>("~/summary", 10);
    input_ = create_subscription<CameraInfo>("input", rclcpp::SensorDataQoS().keep_last(10),
      [this](CameraInfo::ConstSharedPtr info) {
        state_.header(info->header);
        state_.word(info->width);
        state_.word(info->height);
        for (double value : info->d) state_.sample(value);
        for (double value : info->k) state_.sample(value);
        for (double value : info->r) state_.sample(value);
        for (double value : info->p) state_.sample(value);
        summary_->publish(state_.message());
      });
  }
private:
  SummaryState state_;
  rclcpp::Publisher<Summary>::SharedPtr summary_;
  rclcpp::Subscription<CameraInfo>::SharedPtr input_;
};

class DisparitySink : public rclcpp::Node
{
public:
  explicit DisparitySink(const rclcpp::NodeOptions & options)
  : Node("disparity_sink", options), state_(3)
  {
    summary_ = create_publisher<Summary>("~/summary", 10);
    input_ = create_subscription<Disparity>("input", rclcpp::SensorDataQoS().keep_last(10),
      [this](Disparity::ConstSharedPtr disparity) {
        const auto & image = disparity->image;
        validate_image(image, "DisparitySink");
        if (image.encoding != "32FC1" || image.step < static_cast<uint64_t>(image.width) * 4) {
          throw std::runtime_error("DisparitySink requires a valid 32FC1 image");
        }
        state_.header(disparity->header);
        state_.bytes(image.data);
        state_.word(double_bits(disparity->f));
        state_.word(double_bits(disparity->t));
        for (uint32_t row = 0; row < image.height; ++row) {
          for (uint32_t col = 0; col < image.width; ++col) {
            const size_t offset = static_cast<size_t>(row) * image.step + 4 * col;
            state_.sample(read_number<float>(&image.data[offset], image.is_bigendian != 0));
          }
        }
        summary_->publish(state_.message());
      });
  }
private:
  SummaryState state_;
  rclcpp::Publisher<Summary>::SharedPtr summary_;
  rclcpp::Subscription<Disparity>::SharedPtr input_;
};

class PointCloudSink : public rclcpp::Node
{
public:
  explicit PointCloudSink(const rclcpp::NodeOptions & options)
  : Node("pointcloud_sink", options), state_(4)
  {
    const int64_t minimum = declare_parameter<int64_t>("min_finite_points", 1);
    if (minimum < 0) throw std::invalid_argument("min_finite_points must be nonnegative");
    min_finite_ = static_cast<uint64_t>(minimum);
    summary_ = create_publisher<Summary>("~/summary", 10);
    input_ = create_subscription<Cloud>("input", rclcpp::SensorDataQoS().keep_last(10),
      std::bind(&PointCloudSink::observe, this, std::placeholders::_1));
  }

  ~PointCloudSink() override
  {
    RCLCPP_INFO(get_logger(), "LET_PERCEPTION_SINK node=%s frames=%llu finite_xyz=%llu",
      get_fully_qualified_name(),
      static_cast<unsigned long long>(state_.frames()),
      static_cast<unsigned long long>(state_.finite()));
  }

private:
  void observe(Cloud::ConstSharedPtr cloud)
  {
    const uint64_t row_bytes = static_cast<uint64_t>(cloud->width) * cloud->point_step;
    const uint64_t needed = static_cast<uint64_t>(cloud->row_step) * cloud->height;
    if (!cloud->width || !cloud->height || !cloud->point_step ||
      row_bytes > cloud->row_step || needed > cloud->data.size())
    {
      throw std::runtime_error("PointCloudSink received an empty or inconsistent point cloud");
    }
    std::array<const sensor_msgs::msg::PointField *, 3> xyz{};
    for (const auto & field : cloud->fields) {
      const size_t coordinate = field.name == "x" ? 0 : field.name == "y" ? 1 :
        field.name == "z" ? 2 : 3;
      if (coordinate < 3) xyz[coordinate] = &field;
    }
    for (const auto * field : xyz) {
      if (!field || field->count != 1 ||
        (field->datatype != sensor_msgs::msg::PointField::FLOAT32 &&
        field->datatype != sensor_msgs::msg::PointField::FLOAT64))
      {
        throw std::runtime_error("PointCloudSink requires scalar FLOAT32/FLOAT64 x, y and z");
      }
      const uint64_t width = field->datatype == sensor_msgs::msg::PointField::FLOAT32 ? 4 : 8;
      if (static_cast<uint64_t>(field->offset) + width > cloud->point_step) {
        throw std::runtime_error("PointCloudSink coordinate extends outside point_step");
      }
    }
    state_.header(cloud->header);
    state_.word(cloud->width);
    state_.word(cloud->height);
    state_.bytes(cloud->data);
    const uint64_t previous_finite = state_.finite();
    for (uint32_t row = 0; row < cloud->height; ++row) {
      for (uint32_t col = 0; col < cloud->width; ++col) {
        const size_t offset = static_cast<size_t>(row) * cloud->row_step +
          static_cast<size_t>(col) * cloud->point_step;
        std::array<double, 3> point;
        for (size_t coordinate = 0; coordinate < point.size(); ++coordinate) {
          const auto & field = *xyz[coordinate];
          const auto * bytes = &cloud->data[offset + field.offset];
          point[coordinate] = field.datatype == sensor_msgs::msg::PointField::FLOAT32 ?
            static_cast<double>(read_number<float>(bytes, cloud->is_bigendian)) :
            read_number<double>(bytes, cloud->is_bigendian);
        }
        state_.sample(point[0], point[1], point[2]);
      }
    }
    if (state_.finite() - previous_finite < min_finite_) {
      throw std::runtime_error("PointCloudSink received fewer than min_finite_points finite XYZ "
              "points; verify real stereo calibration and input synchronization");
    }
    summary_->publish(state_.message());
  }

  uint64_t min_finite_;
  SummaryState state_;
  rclcpp::Publisher<Summary>::SharedPtr summary_;
  rclcpp::Subscription<Cloud>::SharedPtr input_;
};
}

RCLCPP_COMPONENTS_REGISTER_NODE(let_ros_perception::StereoReplaySource)
RCLCPP_COMPONENTS_REGISTER_NODE(let_ros_perception::ImageSink)
RCLCPP_COMPONENTS_REGISTER_NODE(let_ros_perception::CameraInfoSink)
RCLCPP_COMPONENTS_REGISTER_NODE(let_ros_perception::DisparitySink)
RCLCPP_COMPONENTS_REGISTER_NODE(let_ros_perception::PointCloudSink)
