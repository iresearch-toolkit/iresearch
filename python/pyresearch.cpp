#include "pyresearch.h"
#include "store/mmap_directory.hpp"
#include "index/directory_reader.hpp"

/*static*/ index_reader index_reader::open(const char* path) {
  auto dir = std::make_shared<irs::mmap_directory>(path);
  auto index = irs::directory_reader::open(*dir, irs::formats::get("1_0"));

  return std::shared_ptr<irs::index_reader>(
    index.get(),
    [dir, index](irs::index_reader*) { }
  );
}

segment_reader index_reader::segment(size_t i) const {
  auto begin = reader_->begin();
  std::advance(begin, i);

  return std::shared_ptr<irs::sub_reader>(
    &*begin,
    [](irs::sub_reader*){}
  );
}
