#include "store/data_output.hpp"
#include "utils/string.hpp"

namespace irs {

class BytesOutput : public DataOutput {
 public:
  explicit BytesOutput(bstring& buf) noexcept : buf_{&buf} {}

  void WriteByte(byte_type b) final { buf_->push_back(b); }

  void WriteBytes(const byte_type* b, size_t size) final {
    buf_->append(b, size);
  }

  IRS_DATA_OUTPUT_MEMBERS

 private:
  bstring* buf_;
};

}  // namespace irs
